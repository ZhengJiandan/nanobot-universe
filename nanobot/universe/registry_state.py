from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def _utc_ts() -> float:
    return datetime.now(timezone.utc).timestamp()


@dataclass
class RegistryEntry:
    node_id: str
    node_name: str = ""
    endpoint_url: str = ""
    capabilities: dict[str, Any] = field(default_factory=dict)
    price_points: int = 1
    online: bool = False
    last_seen_ts: float = field(default_factory=_utc_ts)

    completed_tasks: int = 0
    earned_points: int = 0


class RegistryState:
    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.nodes: dict[str, RegistryEntry] = {}

    async def upsert(self, entry: RegistryEntry) -> None:
        async with self.lock:
            old = self.nodes.get(entry.node_id)
            if old:
                # Preserve basic counters
                entry.completed_tasks = old.completed_tasks
                entry.earned_points = old.earned_points
            self.nodes[entry.node_id] = entry

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

    async def award(self, node_id: str, points: int) -> None:
        async with self.lock:
            e = self.nodes.get(node_id)
            if not e:
                return
            e.completed_tasks += 1
            e.earned_points += int(points)
            e.last_seen_ts = _utc_ts()

