"""Console entrypoint for the Public Universe Registry (independent process)."""

from __future__ import annotations

import asyncio

import typer
from rich.console import Console

from nanobot.universe.registry_server import RegistryServer, RegistryServerConfig

app = typer.Typer(name="nanobot-universe-registry", help="Public Universe registry (central directory)")
console = Console()


@app.command("run")
def run(
    host: str = typer.Option("0.0.0.0", "--host", help="Bind host"),
    port: int = typer.Option(18999, "--port", help="Bind port"),
    token: str = typer.Option("", "--token", help="Optional registry token (required for register/update/award)"),
):
    """Run the registry server."""
    server = RegistryServer(cfg=RegistryServerConfig(host=host, port=port, registry_token=token))

    async def _main():
        await server.start()
        await asyncio.Future()

    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        console.print("\nStopping registry...")


if __name__ == "__main__":
    app()
