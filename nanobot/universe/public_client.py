from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Any

import websockets

from nanobot.universe.protocol import Envelope, make_envelope


@dataclass
class PublicNode:
    node_id: str
    node_name: str
    endpoint_url: str
    capabilities: dict[str, Any]
    price_points: int


async def list_public_nodes(
    *,
    registry_url: str,
    require_capabilities: list[str] | None = None,
    online_only: bool = True,
    timeout_s: float = 10.0,
) -> list[PublicNode]:
    require_capabilities = require_capabilities or []
    async with websockets.connect(registry_url) as ws:
        req = make_envelope(
            "list",
            payload={
                "onlineOnly": bool(online_only),
                "requireCapabilities": require_capabilities,
            },
        )
        await ws.send(req.to_json())
        while True:
            env = Envelope.from_json(await ws.recv())
            if env.id != req.id:
                continue
            if env.type == "error":
                raise RuntimeError((env.payload or {}).get("message", "registry error"))
            if env.type != "list_result":
                continue
            nodes = []
            for n in (env.payload or {}).get("nodes", []):
                nodes.append(
                    PublicNode(
                        node_id=n.get("nodeId", ""),
                        node_name=n.get("nodeName", ""),
                        endpoint_url=n.get("endpointUrl", ""),
                        capabilities=n.get("capabilities", {}) or {},
                        price_points=int(n.get("pricePoints", 1) or 1),
                    )
                )
            return nodes


def pick_node(nodes: list[PublicNode], *, max_price_points: int | None = None) -> PublicNode:
    candidates = nodes
    if max_price_points is not None:
        candidates = [n for n in nodes if n.price_points <= max_price_points]
    if not candidates:
        raise RuntimeError("no eligible nodes found")
    min_price = min(n.price_points for n in candidates)
    cheapest = [n for n in candidates if n.price_points == min_price]
    return random.choice(cheapest)


async def call_node(
    *,
    endpoint_url: str,
    kind: str,
    prompt: str,
    service_token: str = "",
    timeout_s: float = 120.0,
) -> str:
    async with websockets.connect(endpoint_url) as ws:
        req = make_envelope(
            "task_run",
            payload={"kind": kind, "prompt": prompt, "serviceToken": service_token},
        )
        await ws.send(req.to_json())
        while True:
            env = Envelope.from_json(await ws.recv())
            if env.id != req.id:
                continue
            if env.type == "task_result":
                return (env.payload or {}).get("content", "")
            if env.type in {"task_error", "error"}:
                raise RuntimeError((env.payload or {}).get("message", "task failed"))


async def delegate_task(
    *,
    registry_url: str,
    kind: str,
    prompt: str,
    require_capability: str | None = None,
    to_node_id: str | None = None,
    service_token: str = "",
    max_price_points: int | None = None,
) -> tuple[PublicNode, str]:
    cap = require_capability or kind
    nodes = await list_public_nodes(registry_url=registry_url, require_capabilities=[cap] if cap else [])
    if to_node_id:
        for n in nodes:
            if n.node_id == to_node_id:
                out = await call_node(endpoint_url=n.endpoint_url, kind=kind, prompt=prompt, service_token=service_token)
                return n, out
        raise RuntimeError(f"node not found/online: {to_node_id}")

    node = pick_node(nodes, max_price_points=max_price_points)
    out = await call_node(endpoint_url=node.endpoint_url, kind=kind, prompt=prompt, service_token=service_token)
    return node, out

