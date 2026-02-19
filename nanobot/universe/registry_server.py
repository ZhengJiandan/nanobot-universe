"""Public Universe Registry (central directory).

MVP: a single, centralized WebSocket registry where service nodes can register
their endpoint + capabilities, and clients can query available nodes.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
import time
from typing import Any
import hashlib
from uuid import uuid4

from loguru import logger

import websockets
from websockets.server import WebSocketServerProtocol

from nanobot.universe.protocol import Envelope, make_envelope
from nanobot.universe.registry_state import RegistryEntry, RegistryState, KnowledgePack
from nanobot.universe.ratelimit import RateLimiter


@dataclass
class RegistryServerConfig:
    host: str = "0.0.0.0"
    port: int = 18999
    hello_timeout_s: float = 10.0
    registry_token: str = ""  # if set, required for register/update
    state_file: str = ""  # optional state file for persistence
    ttl_seconds: int = 120  # offline TTL for stale nodes (seconds)
    save_interval_s: int = 10  # periodic save interval (seconds)
    metrics_host: str = "0.0.0.0"
    metrics_port: int | None = None  # set to enable HTTP health/metrics
    rate_limit_per_min: int = 120  # per client IP
    rate_limit_burst: int = 120
    list_requires_token: bool = False  # if true, list requires registry token
    initial_points: int = 10
    preauth_ttl_seconds: int = 300
    knowledge_max_bytes: int = 50000


class RegistryServer:
    def __init__(self, state: RegistryState | None = None, cfg: RegistryServerConfig | None = None) -> None:
        self.state = state or RegistryState()
        self.cfg = cfg or RegistryServerConfig()
        self.bound_port: int = self.cfg.port
        self._server: websockets.server.Serve | None = None
        self._ws_to_node: dict[WebSocketServerProtocol, str] = {}
        self._save_task: asyncio.Task[None] | None = None
        self._ttl_task: asyncio.Task[None] | None = None
        self._metrics_task: asyncio.Task[None] | None = None
        self._metrics_server: asyncio.AbstractServer | None = None
        self._state_path: Path | None = Path(self.cfg.state_file).expanduser() if self.cfg.state_file else None
        self._start_ts = time.time()
        self._limiter = RateLimiter(self.cfg.rate_limit_per_min, self.cfg.rate_limit_burst)
        self._rate_limited_count = 0
        self._preauth_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._state_path:
            await self.state.load(self._state_path)
        self._server = await websockets.serve(self._handler, self.cfg.host, self.cfg.port, ping_interval=20, ping_timeout=20)
        try:
            if self._server.sockets:
                self.bound_port = int(self._server.sockets[0].getsockname()[1])
        except Exception:
            self.bound_port = self.cfg.port
        logger.info(f"Universe registry listening on ws://{self.cfg.host}:{self.bound_port}")

        if self._state_path:
            self._save_task = asyncio.create_task(self._save_loop())
        if self.cfg.ttl_seconds > 0:
            self._ttl_task = asyncio.create_task(self._ttl_loop())
        if self.cfg.preauth_ttl_seconds > 0:
            self._preauth_task = asyncio.create_task(self._preauth_loop())
        if self.cfg.metrics_port:
            self._metrics_task = asyncio.create_task(self._metrics_loop())

    async def stop(self) -> None:
        if not self._server:
            return
        if self._save_task:
            self._save_task.cancel()
            with contextlib.suppress(Exception):
                await self._save_task
            self._save_task = None
        if self._ttl_task:
            self._ttl_task.cancel()
            with contextlib.suppress(Exception):
                await self._ttl_task
            self._ttl_task = None
        if self._preauth_task:
            self._preauth_task.cancel()
            with contextlib.suppress(Exception):
                await self._preauth_task
            self._preauth_task = None
        if self._metrics_task:
            self._metrics_task.cancel()
            with contextlib.suppress(Exception):
                await self._metrics_task
            self._metrics_task = None
        if self._metrics_server:
            self._metrics_server.close()
            await self._metrics_server.wait_closed()
            self._metrics_server = None
        if self._state_path:
            await self.state.save(self._state_path)
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

            if not self._allow_client(ws):
                self._rate_limited_count += 1
                await ws.send(make_envelope("error", id=env.id, payload={"message": "rate limited"}).to_json())
                return

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
                is_new = False
                async with self.state.lock:
                    is_new = node_id not in self.state.nodes

                entry = RegistryEntry(
                    node_id=node_id,
                    node_name=(env.payload or {}).get("nodeName", ""),
                    endpoint_url=(env.payload or {}).get("endpointUrl", ""),
                    capabilities=(env.payload or {}).get("capabilities", {}),
                    capability_card=self._sanitize_capability_card((env.payload or {}).get("capabilityCard", {}) or {}),
                    price_points=int((env.payload or {}).get("pricePoints", 1) or 1),
                    online=True,
                )
                if is_new:
                    entry.balance = int(self.cfg.initial_points or 0)
                await self.state.upsert(entry)
                self._ws_to_node[ws] = node_id
                if self._state_path:
                    await self.state.save(self._state_path)
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
                if self._state_path:
                    await self.state.save(self._state_path)

    async def _handle(self, env: Envelope, ws: WebSocketServerProtocol) -> None:
        if not self._allow_client(ws):
            self._rate_limited_count += 1
            await ws.send(make_envelope("error", id=env.id, payload={"message": "rate limited"}).to_json())
            return

        if env.type == "ping":
            await ws.send(make_envelope("pong", id=env.id).to_json())
            return

        if env.type == "list":
            # payload filters are optional; MVP supports capability key match.
            if self.cfg.list_requires_token:
                token = (env.payload or {}).get("registryToken", "")
                if not self._check_token(token):
                    await ws.send(
                        make_envelope("error", id=env.id, payload={"message": "invalid registry token"}).to_json()
                    )
                    return
            required = (env.payload or {}).get("requireCapabilities", [])
            require_online = bool((env.payload or {}).get("onlineOnly", True))
            page = int((env.payload or {}).get("page", 1) or 1)
            page_size = int((env.payload or {}).get("pageSize", 50) or 50)
            page = max(1, page)
            page_size = max(1, min(page_size, 200))
            await self.state.apply_ttl(self.cfg.ttl_seconds)
            if required:
                nodes = await self.state.candidates_for_caps(required)
            else:
                nodes = await (self.state.list_online() if require_online else self.state.all_nodes())
            if require_online:
                nodes = [n for n in nodes if n.online]
            nodes.sort(key=lambda n: n.node_id)
            total = len(nodes)
            start = (page - 1) * page_size
            end = start + page_size
            nodes = nodes[start:end]

            await ws.send(
                make_envelope(
                    "list_result",
                    id=env.id,
                    payload={
                        "page": page,
                        "pageSize": page_size,
                        "total": total,
                        "nodes": [
                            {
                                "nodeId": n.node_id,
                                "nodeName": n.node_name,
                                "capabilities": n.capabilities,
                                "capabilityCard": n.capability_card,
                                "pricePoints": n.price_points,
                                "online": n.online,
                                "completedTasks": n.completed_tasks,
                                "earnedPoints": n.earned_points,
                                "balance": n.balance,
                                "spentPoints": n.spent_points,
                                "heldPoints": n.held_points,
                                "successCount": n.success_count,
                                "failCount": n.fail_count,
                                "avgLatencyMs": int(n.total_latency_ms / max(1, n.success_count + n.fail_count)),
                                "lastSeenTs": n.last_seen_ts,
                            }
                            for n in nodes
                        ]
                    },
                ).to_json()
            )
            return

        if env.type == "knowledge_publish":
            token = (env.payload or {}).get("registryToken", "")
            if not self._check_token(token):
                await ws.send(make_envelope("error", id=env.id, payload={"message": "invalid registry token"}).to_json())
                return
            payload = env.payload or {}
            name = str(payload.get("name", "")).strip()
            kind = str(payload.get("kind", "")).strip()
            content = str(payload.get("content", ""))
            if not name or not kind or not content:
                await ws.send(
                    make_envelope("error", id=env.id, payload={"message": "missing name/kind/content"}).to_json()
                )
                return
            owner = str(payload.get("ownerNode") or payload.get("nodeId") or env.from_node or "").strip()
            if not owner:
                await ws.send(make_envelope("error", id=env.id, payload={"message": "missing ownerNode"}).to_json())
                return
            size_bytes = len(content.encode("utf-8"))
            max_bytes = int(self.cfg.knowledge_max_bytes or 0)
            if max_bytes > 0 and size_bytes > max_bytes:
                await ws.send(
                    make_envelope(
                        "error",
                        id=env.id,
                        payload={"message": f"content too large (>{max_bytes} bytes)"},
                    ).to_json()
                )
                return

            pack_id = str(payload.get("id") or "").strip()
            if not pack_id:
                pack_id = str(uuid4())

            existing = await self.state.get_knowledge(pack_id)
            if existing and existing.owner_node != owner:
                await ws.send(make_envelope("error", id=env.id, payload={"message": "owner mismatch"}).to_json())
                return
            if existing and not bool(payload.get("allowUpdate", False)):
                await ws.send(make_envelope("error", id=env.id, payload={"message": "pack exists (set allowUpdate)"}).to_json())
                return

            tags = payload.get("tags", []) or []
            if not isinstance(tags, list):
                tags = []
            clean_tags = []
            for t in tags:
                if isinstance(t, str):
                    t = t.strip()
                    if t:
                        clean_tags.append(t[:32])
            clean_tags = clean_tags[:20]

            summary = str(payload.get("summary", "")).strip()
            version = str(payload.get("version", "1.0")).strip() or "1.0"
            content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
            now_ts = time.time()
            created_ts = existing.created_ts if existing else now_ts

            pack = KnowledgePack(
                pack_id=pack_id,
                name=name[:120],
                kind=kind[:60],
                summary=summary[:500],
                content=content,
                tags=clean_tags,
                version=version[:50],
                owner_node=owner,
                created_ts=created_ts,
                updated_ts=now_ts,
                content_hash=content_hash,
                size_bytes=size_bytes,
            )
            await self.state.upsert_knowledge(pack)
            if self._state_path:
                await self.state.save(self._state_path)
            await ws.send(
                make_envelope(
                    "knowledge_publish_ok",
                    id=env.id,
                    payload={
                        "id": pack_id,
                        "sizeBytes": size_bytes,
                        "contentHash": content_hash,
                        "updatedTs": now_ts,
                    },
                ).to_json()
            )
            return

        if env.type == "knowledge_list":
            token = (env.payload or {}).get("registryToken", "")
            if self.cfg.registry_token and not self._check_token(token):
                await ws.send(make_envelope("error", id=env.id, payload={"message": "invalid registry token"}).to_json())
                return
            payload = env.payload or {}
            kind = payload.get("kind")
            tag = payload.get("tag")
            owner = payload.get("ownerNode")
            limit = int(payload.get("limit", 50) or 50)
            packs = await self.state.list_knowledge(kind=kind, tag=tag, owner=owner, limit=limit)
            await ws.send(
                make_envelope(
                    "knowledge_list_result",
                    id=env.id,
                    payload={
                        "packs": [p.to_dict(include_content=False) for p in packs],
                    },
                ).to_json()
            )
            return

        if env.type == "knowledge_get":
            token = (env.payload or {}).get("registryToken", "")
            if self.cfg.registry_token and not self._check_token(token):
                await ws.send(make_envelope("error", id=env.id, payload={"message": "invalid registry token"}).to_json())
                return
            pack_id = (env.payload or {}).get("id")
            if not pack_id:
                await ws.send(make_envelope("error", id=env.id, payload={"message": "missing id"}).to_json())
                return
            pack = await self.state.get_knowledge(str(pack_id))
            if not pack:
                await ws.send(make_envelope("error", id=env.id, payload={"message": "pack not found"}).to_json())
                return
            await ws.send(
                make_envelope(
                    "knowledge_get_result",
                    id=env.id,
                    payload=pack.to_dict(include_content=True),
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
            payer_node_id = (env.payload or {}).get("payerNode") or None
            if not node_id or points <= 0:
                await ws.send(make_envelope("error", id=env.id, payload={"message": "missing nodeId/points"}).to_json())
                return
            ok = await self.state.award(node_id, points, payer_node_id=payer_node_id)
            if not ok:
                await ws.send(make_envelope("error", id=env.id, payload={"message": "insufficient balance or node missing"}).to_json())
                return
            if self._state_path:
                await self.state.save(self._state_path)
            await ws.send(make_envelope("award_ok", id=env.id).to_json())
            return

        if env.type == "report":
            token = (env.payload or {}).get("registryToken", "")
            if not self._check_token(token):
                await ws.send(make_envelope("error", id=env.id, payload={"message": "invalid registry token"}).to_json())
                return
            node_id = (env.payload or {}).get("nodeId")
            ok = bool((env.payload or {}).get("ok", False))
            latency_ms = (env.payload or {}).get("latencyMs")
            if not node_id:
                await ws.send(make_envelope("error", id=env.id, payload={"message": "missing nodeId"}).to_json())
                return
            await self.state.report(node_id, ok, latency_ms=latency_ms)
            if self._state_path:
                await self.state.save(self._state_path)
            await ws.send(make_envelope("report_ok", id=env.id).to_json())
            return

        if env.type == "reserve":
            token = (env.payload or {}).get("registryToken", "")
            if not self._check_token(token):
                await ws.send(make_envelope("error", id=env.id, payload={"message": "invalid registry token"}).to_json())
                return
            node_id = (env.payload or {}).get("nodeId")
            payer_node = (env.payload or {}).get("payerNode")
            points = int((env.payload or {}).get("points", 0) or 0)
            if not node_id or not payer_node or points <= 0:
                await ws.send(make_envelope("error", id=env.id, payload={"message": "missing nodeId/payerNode/points"}).to_json())
                return
            rid = await self.state.reserve(str(payer_node), str(node_id), points)
            if not rid:
                await ws.send(make_envelope("error", id=env.id, payload={"message": "insufficient balance or node missing"}).to_json())
                return
            if self._state_path:
                await self.state.save(self._state_path)
            await ws.send(make_envelope("reserve_ok", id=env.id, payload={"reservationId": rid}).to_json())
            return

        if env.type == "commit":
            token = (env.payload or {}).get("registryToken", "")
            if not self._check_token(token):
                await ws.send(make_envelope("error", id=env.id, payload={"message": "invalid registry token"}).to_json())
                return
            rid = (env.payload or {}).get("reservationId")
            if not rid:
                await ws.send(make_envelope("error", id=env.id, payload={"message": "missing reservationId"}).to_json())
                return
            ok = await self.state.commit(str(rid))
            if not ok:
                await ws.send(make_envelope("error", id=env.id, payload={"message": "reservation not found"}).to_json())
                return
            if self._state_path:
                await self.state.save(self._state_path)
            await ws.send(make_envelope("commit_ok", id=env.id).to_json())
            return

        if env.type == "cancel":
            token = (env.payload or {}).get("registryToken", "")
            if not self._check_token(token):
                await ws.send(make_envelope("error", id=env.id, payload={"message": "invalid registry token"}).to_json())
                return
            rid = (env.payload or {}).get("reservationId")
            if not rid:
                await ws.send(make_envelope("error", id=env.id, payload={"message": "missing reservationId"}).to_json())
                return
            ok = await self.state.cancel(str(rid))
            if not ok:
                await ws.send(make_envelope("error", id=env.id, payload={"message": "reservation not found"}).to_json())
                return
            if self._state_path:
                await self.state.save(self._state_path)
            await ws.send(make_envelope("cancel_ok", id=env.id).to_json())
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
            ws_node = self._ws_to_node.get(ws)
            if not ws_node or ws_node != node_id:
                await ws.send(make_envelope("error", id=env.id, payload={"message": "unauthorized update"}).to_json())
                return
            entry = RegistryEntry(
                node_id=node_id,
                node_name=(env.payload or {}).get("nodeName", ""),
                endpoint_url=(env.payload or {}).get("endpointUrl", ""),
                capabilities=(env.payload or {}).get("capabilities", {}),
                capability_card=self._sanitize_capability_card((env.payload or {}).get("capabilityCard", {}) or {}),
                price_points=int((env.payload or {}).get("pricePoints", 1) or 1),
                online=True,
            )
            await self.state.upsert(entry)
            if self._state_path:
                await self.state.save(self._state_path)
            await ws.send(make_envelope("update_ok", id=env.id).to_json())
            return

        if env.type == "resolve":
            token = (env.payload or {}).get("registryToken", "")
            if not self._check_token(token):
                await ws.send(make_envelope("error", id=env.id, payload={"message": "invalid registry token"}).to_json())
                return
            node_id = (env.payload or {}).get("nodeId")
            if not node_id:
                await ws.send(make_envelope("error", id=env.id, payload={"message": "missing nodeId"}).to_json())
                return
            async with self.state.lock:
                entry = self.state.nodes.get(node_id)
            if not entry:
                await ws.send(make_envelope("error", id=env.id, payload={"message": "node not found"}).to_json())
                return
            await ws.send(
                make_envelope(
                    "resolve_ok",
                    id=env.id,
                    payload={
                        "nodeId": entry.node_id,
                        "endpointUrl": entry.endpoint_url,
                        "online": entry.online,
                        "lastSeenTs": entry.last_seen_ts,
                    },
                ).to_json()
            )
            return

        if env.type == "sync":
            token = (env.payload or {}).get("registryToken", "")
            if not self._check_token(token):
                await ws.send(make_envelope("error", id=env.id, payload={"message": "invalid registry token"}).to_json())
                return
            nodes = (env.payload or {}).get("nodes", [])
            now_ts = time.time()
            for item in nodes:
                entry = RegistryEntry.from_dict(item)
                entry.capability_card = self._sanitize_capability_card(entry.capability_card)
                if entry.online:
                    entry.last_seen_ts = now_ts
                await self.state.upsert(entry)
            if self._state_path:
                await self.state.save(self._state_path)
            await ws.send(make_envelope("sync_ok", id=env.id).to_json())
            return

        if env.type == "leaderboard":
            sort_by = (env.payload or {}).get("sortBy", "earnedPoints")
            limit = int((env.payload or {}).get("limit", 20) or 20)
            limit = max(1, min(limit, 200))
            nodes = await self.state.all_nodes()
            if sort_by == "balance":
                nodes.sort(key=lambda n: n.balance, reverse=True)
            elif sort_by == "completedTasks":
                nodes.sort(key=lambda n: n.completed_tasks, reverse=True)
            else:
                nodes.sort(key=lambda n: n.earned_points, reverse=True)
            nodes = nodes[:limit]
            await ws.send(
                make_envelope(
                    "leaderboard_result",
                    id=env.id,
                    payload={
                        "sortBy": sort_by,
                        "limit": limit,
                        "nodes": [
                            {
                                "nodeId": n.node_id,
                                "nodeName": n.node_name,
                                "balance": n.balance,
                                "earnedPoints": n.earned_points,
                                "spentPoints": n.spent_points,
                                "completedTasks": n.completed_tasks,
                                "online": n.online,
                                "lastSeenTs": n.last_seen_ts,
                            }
                            for n in nodes
                        ],
                    },
                ).to_json()
            )
            return

        await ws.send(make_envelope("error", id=env.id, payload={"message": f"unknown type: {env.type}"}).to_json())

    async def _save_loop(self) -> None:
        assert self._state_path is not None
        while True:
            await asyncio.sleep(self.cfg.save_interval_s)
            await self.state.save(self._state_path)

    async def _ttl_loop(self) -> None:
        while True:
            await asyncio.sleep(max(5, int(self.cfg.ttl_seconds // 2) or 5))
            await self.state.apply_ttl(self.cfg.ttl_seconds)

    async def _preauth_loop(self) -> None:
        while True:
            await asyncio.sleep(max(5, int(self.cfg.preauth_ttl_seconds // 2) or 5))
            expired = await self.state.expire_reservations(self.cfg.preauth_ttl_seconds)
            if expired and self._state_path:
                await self.state.save(self._state_path)

    async def _metrics_loop(self) -> None:
        self._metrics_server = await asyncio.start_server(
            self._handle_metrics_request,
            host=self.cfg.metrics_host,
            port=int(self.cfg.metrics_port or 0),
        )
        async with self._metrics_server:
            await self._metrics_server.serve_forever()

    async def _handle_metrics_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            data = await reader.read(1024)
            line = data.splitlines()[0].decode("utf-8", errors="ignore") if data else ""
            path = "/"
            if line.startswith("GET "):
                parts = line.split()
                if len(parts) >= 2:
                    path = parts[1]
            if path.startswith("/health"):
                body = self._health_body()
                self._write_http(writer, 200, "application/json", body)
            elif path.startswith("/metrics"):
                body = self._metrics_body()
                self._write_http(writer, 200, "text/plain; charset=utf-8", body)
            else:
                body = "{\"status\":\"not_found\"}"
                self._write_http(writer, 404, "application/json", body)
        except Exception:
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    def _health_body(self) -> str:
        return (
            "{\"status\":\"ok\","
            f"\"uptimeSeconds\":{int(time.time() - self._start_ts)},"
            f"\"nodesTotal\":{len(self.state.nodes)}"
            "}"
        )

    def _metrics_body(self) -> str:
        total = len(self.state.nodes)
        online = len([n for n in self.state.nodes.values() if n.online])
        uptime = int(time.time() - self._start_ts)
        last_saved = int(self.state.last_saved_ts or 0)
        return (
            "# HELP registry_nodes_total Total nodes in registry\n"
            "# TYPE registry_nodes_total gauge\n"
            f"registry_nodes_total {total}\n"
            "# HELP registry_nodes_online Online nodes in registry\n"
            "# TYPE registry_nodes_online gauge\n"
            f"registry_nodes_online {online}\n"
            "# HELP registry_uptime_seconds Registry uptime\n"
            "# TYPE registry_uptime_seconds counter\n"
            f"registry_uptime_seconds {uptime}\n"
            "# HELP registry_last_saved_ts Last state save unix ts\n"
            "# TYPE registry_last_saved_ts gauge\n"
            f"registry_last_saved_ts {last_saved}\n"
            "# HELP registry_rate_limited_total Rate limited requests\n"
            "# TYPE registry_rate_limited_total counter\n"
            f"registry_rate_limited_total {self._rate_limited_count}\n"
        )

    def _write_http(self, writer: asyncio.StreamWriter, status: int, ctype: str, body: str) -> None:
        reason = {200: "OK", 404: "Not Found"}.get(status, "OK")
        payload = body.encode("utf-8")
        headers = [
            f"HTTP/1.1 {status} {reason}",
            f"Content-Type: {ctype}",
            f"Content-Length: {len(payload)}",
            "Connection: close",
            "",
            "",
        ]
        writer.write("\r\n".join(headers).encode("utf-8") + payload)

    def _allow_client(self, ws: WebSocketServerProtocol) -> bool:
        key = "unknown"
        try:
            if ws.remote_address and ws.remote_address[0]:
                key = str(ws.remote_address[0])
        except Exception:
            pass
        return self._limiter.allow(key)

    def _sanitize_capability_card(self, card: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(card, dict):
            return {}
        cleaned: dict[str, Any] = {}
        # Simple strings
        for key in ("schemaVersion", "summary", "region"):
            val = card.get(key)
            if isinstance(val, str) and val.strip():
                cleaned[key] = val.strip()
        # Lists of strings
        for key in ("skills", "languages", "tags"):
            val = card.get(key)
            if isinstance(val, list):
                items = [str(x).strip() for x in val if isinstance(x, str) and x.strip()]
                if items:
                    cleaned[key] = items
        # Tools (list of objects)
        tools = card.get("tools")
        if isinstance(tools, list):
            cleaned_tools = []
            for item in tools:
                if not isinstance(item, dict):
                    continue
                tool = {}
                for k in ("name", "scope", "notes"):
                    v = item.get(k)
                    if isinstance(v, str) and v.strip():
                        tool[k] = v.strip()
                if tool.get("name"):
                    cleaned_tools.append(tool)
            if cleaned_tools:
                cleaned["tools"] = cleaned_tools
        # Models (list of objects)
        models = card.get("models")
        if isinstance(models, list):
            cleaned_models = []
            for item in models:
                if not isinstance(item, dict):
                    continue
                model = {}
                for k in ("id", "provider"):
                    v = item.get(k)
                    if isinstance(v, str) and v.strip():
                        model[k] = v.strip()
                ctx = item.get("contextTokens")
                if isinstance(ctx, int) and ctx > 0:
                    model["contextTokens"] = ctx
                if model.get("id"):
                    cleaned_models.append(model)
            if cleaned_models:
                cleaned["models"] = cleaned_models
        # Pricing
        pricing = card.get("pricing")
        if isinstance(pricing, dict):
            p = {}
            unit = pricing.get("unit")
            if isinstance(unit, str) and unit.strip():
                p["unit"] = unit.strip()
            per_req = pricing.get("perRequest")
            per_1k = pricing.get("per1kTokens")
            if isinstance(per_req, (int, float)):
                p["perRequest"] = per_req
            if isinstance(per_1k, (int, float)):
                p["per1kTokens"] = per_1k
            if p:
                cleaned["pricing"] = p
        # Limits
        limits = card.get("limits")
        if isinstance(limits, dict):
            l = {}
            for k in ("maxTokens", "timeoutSec", "rateLimitPerMin", "rateLimitPerMinByNode", "concurrency"):
                v = limits.get(k)
                if isinstance(v, int) and v > 0:
                    l[k] = v
            if l:
                cleaned["limits"] = l
        # Availability
        availability = card.get("availability")
        if isinstance(availability, dict):
            a = {}
            for k in ("status", "hours"):
                v = availability.get(k)
                if isinstance(v, str) and v.strip():
                    a[k] = v.strip()
            uptime = availability.get("uptime90d")
            if isinstance(uptime, (int, float)):
                a["uptime90d"] = float(uptime)
            if a:
                cleaned["availability"] = a
        # Auth
        auth = card.get("auth")
        if isinstance(auth, dict):
            a = {}
            mode = auth.get("mode")
            if isinstance(mode, str) and mode.strip():
                a["mode"] = mode.strip()
            required = auth.get("required")
            if isinstance(required, bool):
                a["required"] = required
            if a:
                cleaned["auth"] = a
        # Contact
        contact = card.get("contact")
        if isinstance(contact, dict):
            c = {}
            for k in ("owner", "website"):
                v = contact.get(k)
                if isinstance(v, str) and v.strip():
                    c[k] = v.strip()
            if c:
                cleaned["contact"] = c
        # Examples
        examples = card.get("examples")
        if isinstance(examples, list):
            cleaned_examples = []
            for item in examples:
                if not isinstance(item, dict):
                    continue
                ex = {}
                for k in ("input", "output"):
                    v = item.get(k)
                    if isinstance(v, str) and v.strip():
                        ex[k] = v.strip()
                if ex:
                    cleaned_examples.append(ex)
            if cleaned_examples:
                cleaned["examples"] = cleaned_examples
        return cleaned


import contextlib  # keep top imports minimal
