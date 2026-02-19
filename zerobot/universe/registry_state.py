from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4
import json
from pathlib import Path
import time


def _utc_ts() -> float:
    return datetime.now(timezone.utc).timestamp()


@dataclass
class RegistryEntry:
    node_id: str
    node_name: str = ""
    endpoint_url: str = ""
    capabilities: dict[str, Any] = field(default_factory=dict)
    capability_card: dict[str, Any] = field(default_factory=dict)
    price_points: int = 1
    online: bool = False
    last_seen_ts: float = field(default_factory=_utc_ts)

    completed_tasks: int = 0
    earned_points: int = 0
    balance: int = 0
    spent_points: int = 0
    held_points: int = 0
    success_count: int = 0
    fail_count: int = 0
    total_latency_ms: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "nodeId": self.node_id,
            "nodeName": self.node_name,
            "endpointUrl": self.endpoint_url,
            "capabilities": self.capabilities,
            "capabilityCard": self.capability_card,
            "pricePoints": self.price_points,
            "online": self.online,
            "lastSeenTs": self.last_seen_ts,
            "completedTasks": self.completed_tasks,
            "earnedPoints": self.earned_points,
            "balance": self.balance,
            "spentPoints": self.spent_points,
            "heldPoints": self.held_points,
            "successCount": self.success_count,
            "failCount": self.fail_count,
            "totalLatencyMs": self.total_latency_ms,
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "RegistryEntry":
        return RegistryEntry(
            node_id=data.get("nodeId", ""),
            node_name=data.get("nodeName", ""),
            endpoint_url=data.get("endpointUrl", ""),
            capabilities=data.get("capabilities", {}) or {},
            capability_card=data.get("capabilityCard", {}) or {},
            price_points=int(data.get("pricePoints", 1) or 1),
            online=bool(data.get("online", False)),
            last_seen_ts=float(data.get("lastSeenTs", _utc_ts())),
            completed_tasks=int(data.get("completedTasks", 0) or 0),
            earned_points=int(data.get("earnedPoints", 0) or 0),
            balance=int(data.get("balance", 0) or 0),
            spent_points=int(data.get("spentPoints", 0) or 0),
            held_points=int(data.get("heldPoints", 0) or 0),
            success_count=int(data.get("successCount", 0) or 0),
            fail_count=int(data.get("failCount", 0) or 0),
            total_latency_ms=int(data.get("totalLatencyMs", 0) or 0),
        )


@dataclass
class KnowledgePack:
    pack_id: str
    name: str
    kind: str
    summary: str = ""
    content: str = ""
    tags: list[str] = field(default_factory=list)
    version: str = "1.0"
    owner_node: str = ""
    created_ts: float = field(default_factory=_utc_ts)
    updated_ts: float = field(default_factory=_utc_ts)
    content_hash: str = ""
    size_bytes: int = 0

    def to_dict(self, *, include_content: bool = True) -> dict[str, Any]:
        data = {
            "id": self.pack_id,
            "name": self.name,
            "kind": self.kind,
            "summary": self.summary,
            "tags": self.tags,
            "version": self.version,
            "ownerNode": self.owner_node,
            "createdTs": self.created_ts,
            "updatedTs": self.updated_ts,
            "contentHash": self.content_hash,
            "sizeBytes": self.size_bytes,
        }
        if include_content:
            data["content"] = self.content
        return data

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "KnowledgePack":
        return KnowledgePack(
            pack_id=data.get("id", ""),
            name=data.get("name", ""),
            kind=data.get("kind", ""),
            summary=data.get("summary", ""),
            content=data.get("content", ""),
            tags=data.get("tags", []) or [],
            version=data.get("version", "1.0"),
            owner_node=data.get("ownerNode", ""),
            created_ts=float(data.get("createdTs", _utc_ts())),
            updated_ts=float(data.get("updatedTs", _utc_ts())),
            content_hash=data.get("contentHash", ""),
            size_bytes=int(data.get("sizeBytes", 0) or 0),
        )


class RegistryState:
    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.nodes: dict[str, RegistryEntry] = {}
        self.cap_index: dict[str, set[str]] = {}
        self.last_saved_ts: float = 0.0
        self.reservations: dict[str, dict[str, Any]] = {}
        self.knowledge_packs: dict[str, KnowledgePack] = {}

    async def upsert(self, entry: RegistryEntry) -> None:
        async with self.lock:
            old = self.nodes.get(entry.node_id)
            if old:
                # Preserve basic counters
                entry.completed_tasks = old.completed_tasks
                entry.earned_points = old.earned_points
                entry.balance = old.balance
                entry.spent_points = old.spent_points
                entry.held_points = old.held_points
                entry.success_count = old.success_count
                entry.fail_count = old.fail_count
                entry.total_latency_ms = old.total_latency_ms
            self.nodes[entry.node_id] = entry
            self._reindex(entry, old)

    async def set_offline(self, node_id: str) -> None:
        async with self.lock:
            if node_id in self.nodes:
                self.nodes[node_id].online = False
                self.nodes[node_id].last_seen_ts = _utc_ts()

    async def list_online(self) -> list[RegistryEntry]:
        async with self.lock:
            return [e for e in self.nodes.values() if e.online]

    async def all_nodes(self) -> list[RegistryEntry]:
        async with self.lock:
            return list(self.nodes.values())

    async def award(self, node_id: str, points: int, payer_node_id: str | None = None) -> bool:
        async with self.lock:
            e = self.nodes.get(node_id)
            if not e:
                return False
            if payer_node_id:
                payer = self.nodes.get(payer_node_id)
                if not payer:
                    return False
                if payer.balance < int(points):
                    return False
                payer.balance -= int(points)
                payer.spent_points += int(points)
                payer.last_seen_ts = _utc_ts()
            e.completed_tasks += 1
            e.earned_points += int(points)
            e.balance += int(points)
            e.last_seen_ts = _utc_ts()
            return True

    async def touch(self, node_id: str) -> None:
        async with self.lock:
            if node_id in self.nodes:
                self.nodes[node_id].last_seen_ts = _utc_ts()

    async def apply_ttl(self, ttl_seconds: int) -> None:
        if ttl_seconds <= 0:
            return
        cutoff = time.time() - ttl_seconds
        async with self.lock:
            for e in self.nodes.values():
                if e.online and e.last_seen_ts < cutoff:
                    e.online = False

    async def candidates_for_caps(self, required: list[str]) -> list[RegistryEntry]:
        if not required:
            return await self.all_nodes()
        async with self.lock:
            ids: set[str] | None = None
            for cap in required:
                cap_ids = self.cap_index.get(cap, set())
                ids = cap_ids if ids is None else ids & cap_ids
            if not ids:
                return []
            return [self.nodes[nid] for nid in ids if nid in self.nodes]

    async def save(self, path: Path) -> None:
        async with self.lock:
            data = {
                "nodes": [e.to_dict() for e in self.nodes.values()],
                "reservations": list(self.reservations.values()),
                "knowledgePacks": [p.to_dict(include_content=True) for p in self.knowledge_packs.values()],
            }
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, indent=2))
        tmp.replace(path)
        self.last_saved_ts = time.time()

    async def load(self, path: Path) -> None:
        if not path.exists():
            return
        try:
            data = json.loads(path.read_text())
            nodes = data.get("nodes", [])
            async with self.lock:
                for item in nodes:
                    entry = RegistryEntry.from_dict(item)
                    self.nodes[entry.node_id] = entry
                    self._reindex(entry, None)
                self.reservations = {}
                for item in data.get("reservations", []) or []:
                    if not isinstance(item, dict):
                        continue
                    rid = item.get("id")
                    if rid:
                        self.reservations[rid] = item
                self.knowledge_packs = {}
                for item in data.get("knowledgePacks", []) or []:
                    if not isinstance(item, dict):
                        continue
                    pack = KnowledgePack.from_dict(item)
                    if pack.pack_id:
                        self.knowledge_packs[pack.pack_id] = pack
        except Exception:
            # Corrupt state shouldn't crash the registry
            return

    def _reindex(self, entry: RegistryEntry, old: RegistryEntry | None) -> None:
        if old:
            for cap in (old.capabilities or {}).keys():
                ids = self.cap_index.get(cap)
                if ids and old.node_id in ids:
                    ids.discard(old.node_id)
                    if not ids:
                        self.cap_index.pop(cap, None)
        for cap, enabled in (entry.capabilities or {}).items():
            if not enabled:
                continue
            self.cap_index.setdefault(cap, set()).add(entry.node_id)

    async def reserve(self, payer_node_id: str, provider_node_id: str, points: int) -> str | None:
        async with self.lock:
            payer = self.nodes.get(payer_node_id)
            provider = self.nodes.get(provider_node_id)
            if not payer or not provider:
                return None
            points = int(points)
            if points <= 0:
                return None
            if payer.balance < points:
                return None
            payer.balance -= points
            payer.held_points += points
            payer.last_seen_ts = _utc_ts()
            rid = str(uuid4())
            self.reservations[rid] = {
                "id": rid,
                "payerNode": payer_node_id,
                "providerNode": provider_node_id,
                "points": points,
                "createdTs": _utc_ts(),
            }
            return rid

    async def commit(self, reservation_id: str) -> bool:
        async with self.lock:
            item = self.reservations.pop(reservation_id, None)
            if not item:
                return False
            payer_id = item.get("payerNode")
            provider_id = item.get("providerNode")
            points = int(item.get("points", 0) or 0)
            if not payer_id or not provider_id or points <= 0:
                return False
            payer = self.nodes.get(payer_id)
            provider = self.nodes.get(provider_id)
            if not payer or not provider:
                return False
            payer.held_points = max(0, payer.held_points - points)
            payer.spent_points += points
            payer.last_seen_ts = _utc_ts()
            provider.balance += points
            provider.earned_points += points
            provider.completed_tasks += 1
            provider.last_seen_ts = _utc_ts()
            return True

    async def cancel(self, reservation_id: str) -> bool:
        async with self.lock:
            item = self.reservations.pop(reservation_id, None)
            if not item:
                return False
            payer_id = item.get("payerNode")
            points = int(item.get("points", 0) or 0)
            if not payer_id or points <= 0:
                return False
            payer = self.nodes.get(payer_id)
            if not payer:
                return False
            payer.balance += points
            payer.held_points = max(0, payer.held_points - points)
            payer.last_seen_ts = _utc_ts()
            return True

    async def report(self, node_id: str, ok: bool, latency_ms: int | None = None) -> bool:
        async with self.lock:
            e = self.nodes.get(node_id)
            if not e:
                return False
            if ok:
                e.success_count += 1
            else:
                e.fail_count += 1
            if latency_ms is not None:
                try:
                    e.total_latency_ms += int(latency_ms)
                except Exception:
                    pass
            e.last_seen_ts = _utc_ts()
            return True

    async def expire_reservations(self, ttl_seconds: int) -> int:
        if ttl_seconds <= 0:
            return 0
        cutoff = _utc_ts() - float(ttl_seconds)
        expired: list[dict[str, Any]] = []
        async with self.lock:
            for rid, item in list(self.reservations.items()):
                try:
                    created = float(item.get("createdTs", 0) or 0)
                except Exception:
                    created = 0.0
                if created and created < cutoff:
                    expired.append(self.reservations.pop(rid))
            for item in expired:
                payer_id = item.get("payerNode")
                points = int(item.get("points", 0) or 0)
                if not payer_id or points <= 0:
                    continue
                payer = self.nodes.get(payer_id)
                if not payer:
                    continue
                payer.balance += points
                payer.held_points = max(0, payer.held_points - points)
                payer.last_seen_ts = _utc_ts()
        return len(expired)

    async def upsert_knowledge(self, pack: KnowledgePack) -> None:
        async with self.lock:
            self.knowledge_packs[pack.pack_id] = pack

    async def get_knowledge(self, pack_id: str) -> KnowledgePack | None:
        async with self.lock:
            return self.knowledge_packs.get(pack_id)

    async def list_knowledge(
        self, *, kind: str | None = None, tag: str | None = None, owner: str | None = None, limit: int = 50
    ) -> list[KnowledgePack]:
        async with self.lock:
            packs = list(self.knowledge_packs.values())
        if kind:
            packs = [p for p in packs if p.kind == kind]
        if owner:
            packs = [p for p in packs if p.owner_node == owner]
        if tag:
            packs = [p for p in packs if tag in (p.tags or [])]
        packs.sort(key=lambda p: p.updated_ts, reverse=True)
        return packs[: max(1, min(limit, 200))]
