"""Relay server for the public universe.

Nodes maintain long-lived connections to this relay. Clients send relay_request
messages to the relay, which forwards them to target nodes without exposing
node endpoints.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import time
from typing import Any
from uuid import uuid4

from loguru import logger

import websockets
from websockets.server import WebSocketServerProtocol

from zerobot.universe.protocol import Envelope, make_envelope


@dataclass
class RelayServerConfig:
    host: str = "0.0.0.0"
    port: int = 19001
    relay_token: str = ""  # optional shared token for nodes/clients
    pending_ttl_s: int = 120  # cleanup timeout for pending requests


class RelayServer:
    def __init__(self, cfg: RelayServerConfig | None = None) -> None:
        self.cfg = cfg or RelayServerConfig()
        self.bound_port: int = self.cfg.port
        self._server: websockets.server.Serve | None = None
        self._nodes: dict[str, WebSocketServerProtocol] = {}
        self._pending: dict[str, dict[str, Any]] = {}

    async def start(self) -> None:
        self._server = await websockets.serve(self._handler, self.cfg.host, self.cfg.port, ping_interval=20, ping_timeout=20)
        try:
            if self._server.sockets:
                self.bound_port = int(self._server.sockets[0].getsockname()[1])
        except Exception:
            self.bound_port = self.cfg.port
        logger.info(f"Universe relay listening on ws://{self.cfg.host}:{self.bound_port}")

    async def stop(self) -> None:
        if not self._server:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None

    def _check_token(self, provided: str) -> bool:
        if not self.cfg.relay_token:
            return True
        return provided == self.cfg.relay_token

    async def _handler(self, ws: WebSocketServerProtocol) -> None:
        node_id: str | None = None
        try:
            async for raw in ws:
                await self._cleanup_pending()
                try:
                    env = Envelope.from_json(raw)
                except Exception as e:
                    await ws.send(make_envelope("error", payload={"message": f"bad json: {e}"}).to_json())
                    continue

                if env.type == "ping":
                    await ws.send(make_envelope("pong", id=env.id).to_json())
                    continue

                if env.type == "relay_hello":
                    token = (env.payload or {}).get("relayToken", "")
                    if not self._check_token(token):
                        await ws.send(make_envelope("error", id=env.id, payload={"message": "invalid relay token"}).to_json())
                        continue
                    node_id = (env.payload or {}).get("nodeId")
                    if not node_id:
                        await ws.send(make_envelope("error", id=env.id, payload={"message": "missing nodeId"}).to_json())
                        continue
                    self._nodes[node_id] = ws
                    await ws.send(make_envelope("relay_hello_ok", id=env.id, payload={"nodeId": node_id}).to_json())
                    continue

                if env.type == "relay_request":
                    token = (env.payload or {}).get("relayToken", "")
                    if not self._check_token(token):
                        await ws.send(make_envelope("error", id=env.id, payload={"message": "invalid relay token"}).to_json())
                        continue
                    target = (env.payload or {}).get("nodeId")
                    if not target:
                        await ws.send(make_envelope("error", id=env.id, payload={"message": "missing nodeId"}).to_json())
                        continue
                    node_ws = self._nodes.get(target)
                    if not node_ws:
                        await ws.send(make_envelope("error", id=env.id, payload={"message": "node offline"}).to_json())
                        continue

                    request_id = env.id
                    internal_id = str(uuid4())
                    self._pending[internal_id] = {
                        "client_ws": ws,
                        "client_id": request_id,
                        "created_ts": time.monotonic(),
                    }
                    forward = make_envelope(
                        "relay_task",
                        id=internal_id,
                        payload={
                            "nodeId": target,
                            "kind": (env.payload or {}).get("kind"),
                            "prompt": (env.payload or {}).get("prompt"),
                            "serviceToken": (env.payload or {}).get("serviceToken", ""),
                            "clientId": (env.payload or {}).get("clientId"),
                        },
                    )
                    await node_ws.send(forward.to_json())
                    continue

                if env.type == "relay_result":
                    # Result from node -> client
                    entry = self._pending.pop(env.id, None)
                    if entry:
                        client_ws = entry.get("client_ws")
                        client_id = entry.get("client_id") or env.id
                        response = make_envelope("relay_response", id=client_id, payload=env.payload or {})
                        await self._safe_send(client_ws, response.to_json())
                    continue
        except websockets.ConnectionClosed:
            pass
        finally:
            # Clean up node mapping if this ws was a node
            if node_id and self._nodes.get(node_id) is ws:
                self._nodes.pop(node_id, None)
            # Remove pending for this client
            dead = [
                req_id
                for req_id, entry in self._pending.items()
                if entry.get("client_ws") is ws
            ]
            for req_id in dead:
                self._pending.pop(req_id, None)

    async def _safe_send(self, ws: WebSocketServerProtocol | None, payload: str) -> None:
        if not ws:
            return
        try:
            await ws.send(payload)
        except Exception:
            return

    async def _cleanup_pending(self) -> None:
        ttl = int(self.cfg.pending_ttl_s or 0)
        if ttl <= 0 or not self._pending:
            return
        now = time.monotonic()
        expired = [rid for rid, entry in self._pending.items() if now - entry.get("created_ts", now) > ttl]
        for rid in expired:
            entry = self._pending.pop(rid, None)
            if not entry:
                continue
            client_ws = entry.get("client_ws")
            client_id = entry.get("client_id") or rid
            response = make_envelope(
                "relay_response",
                id=client_id,
                payload={"ok": False, "message": "timeout"},
            )
            await self._safe_send(client_ws, response.to_json())
