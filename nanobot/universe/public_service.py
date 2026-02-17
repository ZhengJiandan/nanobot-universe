from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import httpx
import websockets

from loguru import logger

from nanobot.config.schema import Config
from nanobot.universe.node_server import NodeServer, NodeServerConfig
from nanobot.universe.protocol import Envelope, make_envelope


@dataclass
class PublicServiceHandle:
    server: NodeServer
    register_task: asyncio.Task[None]


def build_public_capabilities(cfg: Config) -> dict[str, Any]:
    caps = dict(cfg.universe.public_capabilities or {"llm.chat": True})
    if cfg.universe.public_allow_agent_tasks:
        caps["nanobot.agent"] = True
    else:
        caps.pop("nanobot.agent", None)
    return caps


async def _detect_public_ip(url: str) -> str | None:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(url)
            r.raise_for_status()
            ip = (r.text or "").strip()
            return ip or None
    except Exception:
        return None


async def resolve_advertise_url(cfg: Config, server_port: int) -> str:
    uc = cfg.universe
    if uc.public_advertise_url:
        return uc.public_advertise_url

    host = uc.public_advertise_host
    port = uc.public_advertise_port or server_port
    if host:
        return f"ws://{host}:{port}"

    if uc.public_detect_public_ip:
        ip = await _detect_public_ip(uc.public_detect_ip_service)
        if ip:
            return f"ws://{ip}:{port}"

    if uc.public_service_host and uc.public_service_host != "0.0.0.0":
        return f"ws://{uc.public_service_host}:{port}"

    # Fallback: localhost (only useful for same-machine testing).
    return f"ws://127.0.0.1:{port}"


async def _register_loop(
    *,
    registry_url: str,
    registry_token: str,
    node_id: str,
    node_name: str,
    endpoint_url: str,
    capabilities: dict[str, Any],
    price_points: int,
    log_prefix: str = "universe",
) -> None:
    backoff = 1.0
    while True:
        try:
            async with websockets.connect(registry_url) as ws:
                env = make_envelope(
                    "register",
                    from_node=node_id,
                    payload={
                        "nodeId": node_id,
                        "nodeName": node_name,
                        "endpointUrl": endpoint_url,
                        "capabilities": capabilities,
                        "pricePoints": price_points,
                        "registryToken": registry_token,
                    },
                )
                await ws.send(env.to_json())
                resp = Envelope.from_json(await ws.recv())
                if resp.type != "register_ok":
                    raise RuntimeError((resp.payload or {}).get("message", "register failed"))

                logger.info(f"{log_prefix}: registered in registry {registry_url} as {node_id}")
                backoff = 1.0
                while True:
                    await asyncio.sleep(30)
                    upd = make_envelope(
                        "update",
                        from_node=node_id,
                        payload={
                            "nodeId": node_id,
                            "nodeName": node_name,
                            "endpointUrl": endpoint_url,
                            "capabilities": capabilities,
                            "pricePoints": price_points,
                            "registryToken": registry_token,
                        },
                    )
                    await ws.send(upd.to_json())
                    _ = await ws.recv()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"{log_prefix}: registry connection failed ({e}); retrying in {backoff:.0f}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


async def start_public_service(cfg: Config, *, log_prefix: str = "universe") -> PublicServiceHandle:
    uc = cfg.universe
    server = NodeServer(
        NodeServerConfig(
            host=uc.public_service_host,
            port=uc.public_service_port,
            service_token=uc.public_service_token,
        )
    )
    await server.start()

    endpoint_url = await resolve_advertise_url(cfg, server.bound_port)
    if endpoint_url.startswith("ws://127.0.0.1"):
        logger.warning(f"{log_prefix}: advertise_url is localhost; other machines cannot reach this node")

    caps = build_public_capabilities(cfg)
    node_id = uc.node_id
    node_name = uc.node_name or ""
    reg_task = asyncio.create_task(
        _register_loop(
            registry_url=uc.public_registry_url,
            registry_token=uc.public_registry_token,
            node_id=node_id,
            node_name=node_name,
            endpoint_url=endpoint_url,
            capabilities=caps,
            price_points=int(uc.public_price_points or 1),
            log_prefix=log_prefix,
        )
    )

    return PublicServiceHandle(server=server, register_task=reg_task)


async def stop_public_service(handle: PublicServiceHandle | None) -> None:
    if not handle:
        return
    handle.register_task.cancel()
    try:
        await handle.register_task
    except Exception:
        pass
    await handle.server.stop()


async def maybe_start_public_service(cfg: Config, *, log_prefix: str = "universe") -> PublicServiceHandle | None:
    uc = cfg.universe
    if not uc.public_enabled:
        return None
    if not uc.public_provide_service:
        return None
    if not uc.public_auto_register:
        return None
    return await start_public_service(cfg, log_prefix=log_prefix)
