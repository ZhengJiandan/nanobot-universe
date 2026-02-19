"""Console entrypoint for the Public Universe Relay (independent process)."""

from __future__ import annotations

import asyncio

import typer
from rich.console import Console

from nanobot.universe.relay_server import RelayServer, RelayServerConfig

app = typer.Typer(name="nanobot-universe-relay", help="Public Universe relay (task forwarding)")
console = Console()


@app.command("run")
def run(
    host: str = typer.Option("0.0.0.0", "--host", help="Bind host"),
    port: int = typer.Option(19001, "--port", help="Bind port"),
    token: str = typer.Option("", "--token", help="Optional relay token (shared secret)"),
):
    """Run the relay server."""
    server = RelayServer(cfg=RelayServerConfig(host=host, port=port, relay_token=token))

    async def _main():
        await server.start()
        await asyncio.Future()

    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        console.print("\nStopping relay...")


if __name__ == "__main__":
    app()
