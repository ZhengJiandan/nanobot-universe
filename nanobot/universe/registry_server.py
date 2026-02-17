"""Public Universe Registry (central directory).

MVP: a single, centralized WebSocket registry where service nodes can register
their endpoint + capabilities, and clients can query available nodes.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from loguru import logger

import websockets
from websockets.server import WebSocketServerProtocol

from nanobot.universe.protocol import Envelope, make_envelope
from nanobot.universe.registry_state import RegistryEntry, RegistryState


@dataclass
class RegistryServerConfig:
    host: str = "0.0.0.0"
    port: int = 18999
    hello_timeout_s: float = 10.0
    registry_token: str = ""  # if set, required for register/update


class RegistryServer:
    def __init__(self, state: RegistryState | None = None, cfg: RegistryServerConfig | None = None) -> None:
        self.state = state or RegistryState()
        self.cfg = cfg or RegistryServerConfig()
        self.bound_port: int = self.cfg.port
        self._server: websockets.server.Serve | None = None
        self._ws_to_node: dict[WebSocketServerProtocol, str] = {}

    async def start(self) -> None:
        self._server = await websockets.serve(self._handler, self.cfg.host, self.cfg.port, ping_interval=20, ping_timeout=20)
        try:
            if self._server.sockets:
                self.bound_port = int(self._server.sockets[0].getsockname()[1])
        except Exception:
            self.bound_port = self.cfg.port
        logger.info(f"Universe registry listening on ws://{self.cfg.host}:{self.bound_port}")

    async def stop(self) -> None:
        if not self._server:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None

    def _check_token(self, provided: str) -> bool:
        if not self.cfg.registry_token:
            return True
        return provided == self.cfg.registry_token

    async def _handler(self, ws: WebSocketServerProtocol) -> None:
        node_id: str | None = None
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=self.cfg.hello_timeout_s)
            env = Envelope.from_json(raw)

            if env.type not in {"register", "list"}:
                await ws.send(make_envelope("error", id=env.id, payload={"message": "expected register or list"}).to_json())
                return

            if env.type == "register":
                token = (env.payload or {}).get("registryToken", "")
                if not self._check_token(token):
                    await ws.send(make_envelope("error", id=env.id, payload={"message": "invalid registry token"}).to_json())
                    return
                node_id = (env.payload or {}).get("nodeId") or env.from_node
                if not node_id:
                    await ws.send(make_envelope("error", id=env.id, payload={"message": "missing nodeId"}).to_json())
                    return

                entry = RegistryEntry(
                    node_id=node_id,
                    node_name=(env.payload or {}).get("nodeName", ""),
                    endpoint_url=(env.payload or {}).get("endpointUrl", ""),
                    capabilities=(env.payload or {}).get("capabilities", {}),
                    price_points=int((env.payload or {}).get("pricePoints", 1) or 1),
                    online=True,
                )
                await self.state.upsert(entry)
                self._ws_to_node[ws] = node_id
                await ws.send(make_envelope("register_ok", id=env.id, payload={"nodeId": node_id}).to_json())
            else:
                # Treat initial list as a normal request (no separate handshake).
                await self._handle(env, ws)

            async for raw in ws:
                try:
                    env = Envelope.from_json(raw)
                except Exception as e:
                    await ws.send(make_envelope("error", payload={"message": f"bad json: {e}"}).to_json())
                    continue
                await self._handle(env, ws)
        except asyncio.TimeoutError:
            with contextlib.suppress(Exception):
                await ws.send(make_envelope("error", payload={"message": "hello timeout"}).to_json())
        except websockets.ConnectionClosed:
            pass
        except Exception as e:
            logger.exception(f"registry handler error: {e}")
        finally:
            if ws in self._ws_to_node:
                node_id = self._ws_to_node.pop(ws)
                await self.state.set_offline(node_id)

    async def _handle(self, env: Envelope, ws: WebSocketServerProtocol) -> None:
        if env.type == "ping":
            await ws.send(make_envelope("pong", id=env.id).to_json())
            return

        if env.type == "list":
            # payload filters are optional; MVP supports capability key match.
            required = (env.payload or {}).get("requireCapabilities", [])
            require_online = bool((env.payload or {}).get("onlineOnly", True))
            nodes = await (self.state.list_online() if require_online else self.state.all_nodes())
            if required:
                req = set(required)
                nodes = [n for n in nodes if req.issubset(set(n.capabilities.keys())) or req.issubset({k for k, v in n.capabilities.items() if v})]

            await ws.send(
                make_envelope(
                    "list_result",
                    id=env.id,
                    payload={
                        "nodes": [
                            {
                                "nodeId": n.node_id,
                                "nodeName": n.node_name,
                                "endpointUrl": n.endpoint_url,
                                "capabilities": n.capabilities,
                                "pricePoints": n.price_points,
                                "online": n.online,
                                "completedTasks": n.completed_tasks,
                                "earnedPoints": n.earned_points,
                            }
                            for n in nodes
                        ]
                    },
                ).to_json()
            )
            return

        if env.type == "award":
            token = (env.payload or {}).get("registryToken", "")
            if not self._check_token(token):
                await ws.send(make_envelope("error", id=env.id, payload={"message": "invalid registry token"}).to_json())
                return
            node_id = (env.payload or {}).get("nodeId")
            points = int((env.payload or {}).get("points", 0) or 0)
            if not node_id or points <= 0:
                await ws.send(make_envelope("error", id=env.id, payload={"message": "missing nodeId/points"}).to_json())
                return
            await self.state.award(node_id, points)
            await ws.send(make_envelope("award_ok", id=env.id).to_json())
            return

        if env.type == "update":
            token = (env.payload or {}).get("registryToken", "")
            if not self._check_token(token):
                await ws.send(make_envelope("error", id=env.id, payload={"message": "invalid registry token"}).to_json())
                return
            node_id = (env.payload or {}).get("nodeId") or env.from_node
            if not node_id:
                await ws.send(make_envelope("error", id=env.id, payload={"message": "missing nodeId"}).to_json())
                return
            entry = RegistryEntry(
                node_id=node_id,
                node_name=(env.payload or {}).get("nodeName", ""),
                endpoint_url=(env.payload or {}).get("endpointUrl", ""),
                capabilities=(env.payload or {}).get("capabilities", {}),
                price_points=int((env.payload or {}).get("pricePoints", 1) or 1),
                online=True,
            )
            await self.state.upsert(entry)
            await ws.send(make_envelope("update_ok", id=env.id).to_json())
            return

        await ws.send(make_envelope("error", id=env.id, payload={"message": f"unknown type: {env.type}"}).to_json())


import contextlib  # keep top imports minimal
