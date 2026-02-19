"""Registry bridge: synchronize multiple registries."""

from __future__ import annotations

import asyncio

import typer
from rich.console import Console

import websockets

from zerobot.universe.protocol import Envelope, make_envelope

app = typer.Typer(name="zerobot-universe-registry-bridge", help="Sync multiple registries")
console = Console()


async def _fetch_nodes(registry_url: str) -> list[dict]:
    async with websockets.connect(registry_url) as ws:
        req = make_envelope("list", payload={"onlineOnly": True, "page": 1, "pageSize": 200})
        await ws.send(req.to_json())
        while True:
            env = Envelope.from_json(await ws.recv())
            if env.id == req.id and env.type == "list_result":
                return (env.payload or {}).get("nodes", [])


async def _push_sync(registry_url: str, nodes: list[dict], token: str) -> None:
    async with websockets.connect(registry_url) as ws:
        req = make_envelope("sync", payload={"nodes": nodes, "registryToken": token})
        await ws.send(req.to_json())
        _ = await ws.recv()


async def _sync_once(registries: list[str], token: str) -> None:
    snapshots: dict[str, list[dict]] = {}
    for reg in registries:
        try:
            snapshots[reg] = await _fetch_nodes(reg)
        except Exception as e:
            console.print(f"[yellow]![/yellow] Failed to fetch from {reg}: {e}")
    for target in registries:
        merged = []
        for reg, nodes in snapshots.items():
            if reg == target:
                continue
            merged.extend(nodes)
        if not merged:
            continue
        try:
            await _push_sync(target, merged, token)
        except Exception as e:
            console.print(f"[yellow]![/yellow] Failed to sync to {target}: {e}")


@app.command()
def run(
    registry: list[str] = typer.Option(..., "--registry", help="Registry ws://... (repeatable)"),
    token: str = typer.Option("", "--token", help="Registry token for sync (if required)"),
    interval: int = typer.Option(15, "--interval", help="Sync interval seconds"),
):
    """Run the registry bridge."""

    async def _loop():
        while True:
            await _sync_once(registry, token)
            await asyncio.sleep(interval)

    try:
        asyncio.run(_loop())
    except KeyboardInterrupt:
        console.print("\nStopped.")


if __name__ == "__main__":
    app()
