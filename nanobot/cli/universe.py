"""Universe CLI commands (client-side).

These commands manage org memberships, friends, and basic messaging.
"""

from __future__ import annotations

import asyncio
from uuid import uuid4

import typer
from rich.console import Console
from rich.table import Table

from nanobot.config.loader import load_config, save_config
from nanobot.config.schema import UniverseMembership
from nanobot.universe.client import UniverseClient

app = typer.Typer(help="Universe: org-scoped nanobot network")
console = Console()


def _ensure_node_id() -> str:
    cfg = load_config()
    if not cfg.universe.node_id:
        cfg.universe.node_id = str(uuid4())
        cfg.universe.enabled = True
        save_config(cfg)
    return cfg.universe.node_id


def _get_membership(org_id: str | None) -> UniverseMembership:
    cfg = load_config()
    memberships = [m for m in cfg.universe.memberships if m.enabled]
    if not memberships:
        raise typer.BadParameter("No universe memberships. Run: nanobot universe join ...")

    if org_id:
        for m in memberships:
            if m.org_id == org_id:
                return m
        raise typer.BadParameter(f"Unknown org_id: {org_id}")

    if len(memberships) == 1:
        return memberships[0]

    raise typer.BadParameter("Multiple memberships found; specify --org ORG_ID")


async def _client_from_config(org_id: str | None) -> UniverseClient:
    cfg = load_config()
    node_id = _ensure_node_id()
    m = _get_membership(org_id)
    node_name = cfg.universe.node_name or ""
    return UniverseClient(
        hub_url=m.hub_url,
        org_id=m.org_id,
        join_secret=m.join_secret,
        node_id=node_id,
        node_name=node_name,
        capabilities={"dm": True, "friend": True},
    )


@app.command("join")
def join(
    hub: str = typer.Option(..., "--hub", help="Hub WebSocket URL, e.g. ws://host:18888"),
    org: str = typer.Option(..., "--org", help="Org ID"),
    join_secret: str = typer.Option(..., "--join-secret", help="Org join secret/invite token"),
    label: str = typer.Option("", "--label", help="Optional label for this membership"),
    test: bool = typer.Option(True, "--test/--no-test", help="Test connection after saving"),
):
    """Join an org-scoped Universe hub."""
    cfg = load_config()
    _ensure_node_id()
    cfg.universe.enabled = True

    # Upsert membership
    existing = None
    for m in cfg.universe.memberships:
        if m.org_id == org:
            existing = m
            break
    if existing is None:
        cfg.universe.memberships.append(
            UniverseMembership(org_id=org, hub_url=hub, join_secret=join_secret, label=label, enabled=True)
        )
    else:
        existing.hub_url = hub
        existing.join_secret = join_secret
        existing.label = label or existing.label
        existing.enabled = True

    save_config(cfg)
    console.print(f"[green]✓[/green] Joined org [cyan]{org}[/cyan] at {hub}")

    if test:
        async def _test():
            client = await _client_from_config(org)
            async with client:
                nodes = await client.list_nodes()
                return nodes

        try:
            nodes = asyncio.run(_test())
            console.print(f"[green]✓[/green] Connected. Online nodes: {len(nodes)}")
        except Exception as e:
            console.print(f"[yellow]![/yellow] Saved, but connection test failed: {e}")


@app.command("status")
def status():
    """Show local Universe config + joined networks."""
    cfg = load_config()
    node_id = cfg.universe.node_id or "[dim]not set (run join)[/dim]"
    console.print(f"Node ID: [cyan]{node_id}[/cyan]")
    if cfg.universe.node_name:
        console.print(f"Node Name: {cfg.universe.node_name}")

    table = Table(title="Memberships")
    table.add_column("Org", style="cyan")
    table.add_column("Enabled")
    table.add_column("Hub URL")
    table.add_column("Label")
    for m in cfg.universe.memberships:
        table.add_row(m.org_id or "-", "✓" if m.enabled else "✗", m.hub_url or "-", m.label or "-")
    console.print(table)


@app.command("nodes")
def nodes(org: str = typer.Option(None, "--org", help="Org ID (if multiple memberships)")):
    """List online nodes in the org."""
    async def _run():
        client = await _client_from_config(org)
        async with client:
            return await client.list_nodes()

    nodes = asyncio.run(_run())
    table = Table(title="Online Nodes")
    table.add_column("Node ID", style="cyan")
    table.add_column("Name")
    table.add_column("Capabilities")
    for n in nodes:
        table.add_row(n.get("nodeId", ""), n.get("nodeName", ""), str(n.get("capabilities", {})))
    console.print(table)


friend_app = typer.Typer(help="Manage friends (explicit mutual add)")
app.add_typer(friend_app, name="friend")


@friend_app.command("request")
def friend_request(
    to: str = typer.Option(..., "--to", help="Target node_id"),
    message: str = typer.Option("", "--message", "-m", help="Optional message"),
    org: str = typer.Option(None, "--org", help="Org ID (if multiple memberships)"),
):
    """Send a friend request to another node."""
    async def _run():
        client = await _client_from_config(org)
        async with client:
            return await client.send_friend_request(to, message=message)

    req_id = asyncio.run(_run())
    console.print(f"[green]✓[/green] Friend request sent. requestId={req_id}")


@friend_app.command("pending")
def friend_pending(org: str = typer.Option(None, "--org", help="Org ID (if multiple memberships)")):
    """List pending friend requests addressed to you."""
    async def _run():
        client = await _client_from_config(org)
        async with client:
            return await client.get_pending_friend_requests()

    reqs = asyncio.run(_run())
    table = Table(title="Pending Friend Requests")
    table.add_column("Request ID", style="cyan")
    table.add_column("From")
    table.add_column("Message")
    for r in reqs:
        table.add_row(r.get("requestId", ""), r.get("fromNode", ""), r.get("message", ""))
    console.print(table)


@friend_app.command("accept")
def friend_accept(
    request_id: str = typer.Argument(..., help="Request ID"),
    org: str = typer.Option(None, "--org", help="Org ID (if multiple memberships)"),
):
    """Accept a pending friend request."""
    async def _run():
        client = await _client_from_config(org)
        async with client:
            await client.accept_friend_request(request_id)

    asyncio.run(_run())
    console.print("[green]✓[/green] Friend request accepted.")


@friend_app.command("list")
def friend_list(org: str = typer.Option(None, "--org", help="Org ID (if multiple memberships)")):
    """List your friends (node_ids)."""
    async def _run():
        client = await _client_from_config(org)
        async with client:
            return await client.get_friends()

    friends = asyncio.run(_run())
    for f in friends:
        console.print(f"- {f}")


@app.command("dm")
def dm(
    to: str = typer.Option(..., "--to", help="Target node_id (must be a friend)"),
    content: str = typer.Option(..., "--content", "-m", help="Message content"),
    org: str = typer.Option(None, "--org", help="Org ID (if multiple memberships)"),
):
    """Send a DM to a friend."""
    async def _run():
        client = await _client_from_config(org)
        async with client:
            await client.dm(to, content)

    asyncio.run(_run())
    console.print("[green]✓[/green] Sent.")


@app.command("listen")
def listen(org: str = typer.Option(None, "--org", help="Org ID (if multiple memberships)")):
    """Listen for inbound Universe events (DMs, friend requests, presence)."""

    async def _run():
        client = await _client_from_config(org)
        async with client:
            async def _handler(env):
                t = env.type
                if t == "dm_deliver":
                    console.print(f"[cyan]DM[/cyan] from {env.payload.get('fromNode')}: {env.payload.get('content')}")
                elif t == "friend_request_deliver":
                    console.print(
                        f"[cyan]Friend Request[/cyan] from {env.payload.get('fromNode')}: {env.payload.get('message')}\n"
                        f"  requestId={env.payload.get('requestId')}"
                    )
                elif t == "friend_added":
                    console.print(f"[green]Friend added[/green]: {env.payload.get('friendNode')}")
                elif t == "presence":
                    node = env.payload.get("nodeId")
                    online = env.payload.get("online")
                    console.print(f"[dim]presence[/dim] {node} -> {'online' if online else 'offline'}")
                else:
                    console.print(f"[dim]{t}[/dim] {env.payload}")

            console.print("[dim]Listening... Ctrl+C to stop[/dim]")
            await client.listen(_handler)

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        console.print("\nStopped.")


# ============================================================================
# Public Universe (central registry + direct calls)
# ============================================================================

public_app = typer.Typer(help="Public universe: opt-in registry discovery + direct task calls")
app.add_typer(public_app, name="public")


@public_app.command("enable")
def public_enable(
    registry: str = typer.Option(None, "--registry", help="Registry URL, e.g. ws://host:18999"),
    provide: bool = typer.Option(None, "--provide/--no-provide", help="Whether to provide public service"),
    token: str = typer.Option(None, "--service-token", help="Optional service token required by this node"),
    allow_agent_tasks: bool = typer.Option(
        None, "--allow-agent-tasks/--no-allow-agent-tasks", help="Allow this node to serve kind=nanobot.agent"
    ),
    auto_delegate: bool = typer.Option(
        None, "--auto-delegate/--no-auto-delegate", help="Auto-delegate to universe when local run is blocked"
    ),
):
    """Enable public universe mode (opt-in)."""
    cfg = load_config()
    _ensure_node_id()
    cfg.universe.public_enabled = True
    if registry is not None:
        cfg.universe.public_registry_url = registry
    if provide is not None:
        cfg.universe.public_provide_service = provide
    if token is not None:
        cfg.universe.public_service_token = token
    if allow_agent_tasks is not None:
        cfg.universe.public_allow_agent_tasks = allow_agent_tasks
    if auto_delegate is not None:
        cfg.universe.public_auto_delegate_enabled = auto_delegate
    save_config(cfg)
    console.print("[green]✓[/green] Public universe enabled.")


@public_app.command("disable")
def public_disable():
    """Disable public universe mode."""
    cfg = load_config()
    cfg.universe.public_enabled = False
    cfg.universe.public_provide_service = False
    save_config(cfg)
    console.print("[green]✓[/green] Public universe disabled.")


@public_app.command("list")
def public_list(
    registry: str = typer.Option(None, "--registry", help="Registry URL override"),
    require_cap: str = typer.Option("llm.chat", "--require-cap", help="Required capability key"),
    all: bool = typer.Option(False, "--all", help="Include offline nodes"),
):
    """List nodes in the public registry."""
    from nanobot.universe.protocol import Envelope, make_envelope
    import websockets

    cfg = load_config()
    reg = registry or cfg.universe.public_registry_url

    async def _run():
        async with websockets.connect(reg) as ws:
            req = make_envelope("list", payload={"onlineOnly": not all, "requireCapabilities": [require_cap] if require_cap else []})
            await ws.send(req.to_json())
            # ack + result
            while True:
                env = Envelope.from_json(await ws.recv())
                if env.id == req.id and env.type in {"list_result", "error"}:
                    return env

    env = asyncio.run(_run())
    if env.type == "error":
        raise typer.Exit(code=1)

    table = Table(title=f"Public Nodes (registry={reg})")
    table.add_column("Node ID", style="cyan")
    table.add_column("Name")
    table.add_column("Endpoint")
    table.add_column("Price")
    table.add_column("Done")
    table.add_column("Points")
    for n in (env.payload or {}).get("nodes", []):
        table.add_row(
            n.get("nodeId", ""),
            n.get("nodeName", ""),
            n.get("endpointUrl", ""),
            str(n.get("pricePoints", "")),
            str(n.get("completedTasks", "")),
            str(n.get("earnedPoints", "")),
        )
    console.print(table)


@public_app.command("serve")
def public_serve(
    registry: str = typer.Option(None, "--registry", help="Registry URL override"),
    host: str = typer.Option(None, "--host", help="Bind host for node service"),
    port: int = typer.Option(None, "--port", help="Bind port for node service"),
    advertise: str = typer.Option(None, "--advertise", help="Advertised endpoint URL, e.g. ws://YOUR_IP:18998"),
    name: str = typer.Option(None, "--name", help="Node display name override"),
    price: int = typer.Option(None, "--price", help="Price points per task"),
    registry_token: str = typer.Option(None, "--registry-token", help="Registry token (if required)"),
):
    """Run a public service node and register it in the registry."""
    import websockets

    from nanobot.universe.node_server import NodeServer, NodeServerConfig
    from nanobot.universe.protocol import Envelope, make_envelope

    cfg = load_config()
    node_id = _ensure_node_id()
    if not cfg.universe.public_enabled:
        console.print("[yellow]![/yellow] Public universe is disabled. Run: nanobot universe public enable")
        raise typer.Exit(1)
    if not cfg.universe.public_provide_service:
        console.print("[yellow]![/yellow] This node is not set to provide service. Re-run enable with --provide")
        raise typer.Exit(1)

    reg = registry or cfg.universe.public_registry_url
    bind_host = host or cfg.universe.public_service_host
    bind_port = port or cfg.universe.public_service_port
    svc_token = cfg.universe.public_service_token

    node_name = name or cfg.universe.node_name or ""
    price_points = int(price if price is not None else cfg.universe.public_price_points)
    caps = cfg.universe.public_capabilities or {"llm.chat": True}
    # Advertise nanobot.agent capability only when explicitly allowed.
    if cfg.universe.public_allow_agent_tasks:
        caps = {**caps, "nanobot.agent": True}
    else:
        caps = {k: v for k, v in caps.items() if k != "nanobot.agent"}
    reg_token = registry_token if registry_token is not None else cfg.universe.public_registry_token

    async def _register_loop(endpoint_url: str):
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(reg) as ws:
                    env = make_envelope(
                        "register",
                        from_node=node_id,
                        payload={
                            "nodeId": node_id,
                            "nodeName": node_name,
                            "endpointUrl": endpoint_url,
                            "capabilities": caps,
                            "pricePoints": price_points,
                            "registryToken": reg_token,
                        },
                    )
                    await ws.send(env.to_json())
                    # wait for ok
                    resp = Envelope.from_json(await ws.recv())
                    if resp.type != "register_ok":
                        raise RuntimeError(resp.payload.get("message", "register failed"))
                    console.print(f"[green]✓[/green] Registered in registry {reg} as {node_id}")

                    backoff = 1.0
                    # keep connection open; periodic update as heartbeat
                    while True:
                        await asyncio.sleep(30)
                        upd = make_envelope(
                            "update",
                            from_node=node_id,
                            payload={
                                "nodeId": node_id,
                                "nodeName": node_name,
                                "endpointUrl": endpoint_url,
                                "capabilities": caps,
                                "pricePoints": price_points,
                                "registryToken": reg_token,
                            },
                        )
                        await ws.send(upd.to_json())
                        _ = await ws.recv()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                console.print(f"[yellow]![/yellow] Registry connection failed ({e}); retrying in {backoff:.0f}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _run():
        server = NodeServer(NodeServerConfig(host=bind_host, port=bind_port, service_token=svc_token))
        await server.start()

        endpoint_url = advertise
        if not endpoint_url:
            # Default to localhost for MVP; for LAN/public usage set --advertise.
            endpoint_url = f"ws://127.0.0.1:{server.bound_port}"

        reg_task = asyncio.create_task(_register_loop(endpoint_url))
        try:
            console.print("[dim]Serving... Ctrl+C to stop[/dim]")
            await asyncio.Future()
        finally:
            reg_task.cancel()
            await server.stop()

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        console.print("\nStopped.")


@public_app.command("call")
def public_call(
    prompt: str = typer.Option(..., "--prompt", "-m", help="Task prompt"),
    registry: str = typer.Option(None, "--registry", help="Registry URL override"),
    kind: str = typer.Option("llm.chat", "--kind", help="Task kind: llm.chat | echo"),
    require_cap: str = typer.Option("", "--require-cap", help="Required capability key (default: kind)"),
    to: str = typer.Option(None, "--to", help="Directly call a specific node_id (skip selection)"),
    service_token: str = typer.Option("", "--service-token", help="Service token if required by provider node"),
    award_points: int = typer.Option(0, "--award", help="Award points to the provider in registry (MVP bookkeeping)"),
    registry_token: str = typer.Option("", "--registry-token", help="Registry token (if award requires it)"),
):
    """Call a public node to execute a task (direct node-to-node)."""
    import websockets

    from nanobot.universe.protocol import Envelope, make_envelope

    cfg = load_config()
    reg = registry or cfg.universe.public_registry_url
    cap = require_cap or kind

    async def _pick_target() -> dict:
        async with websockets.connect(reg) as ws:
            req = make_envelope("list", payload={"onlineOnly": True, "requireCapabilities": [cap] if cap else []})
            await ws.send(req.to_json())
            while True:
                env = Envelope.from_json(await ws.recv())
                if env.id == req.id and env.type == "list_result":
                    nodes = (env.payload or {}).get("nodes", [])
                    if to:
                        for n in nodes:
                            if n.get("nodeId") == to:
                                return n
                        raise RuntimeError(f"node not found/online: {to}")
                    if not nodes:
                        raise RuntimeError("no online nodes match requirement")
                    # MVP selection: cheapest first, then arbitrary.
                    nodes.sort(key=lambda n: int(n.get("pricePoints", 1) or 1))
                    return nodes[0]
        raise RuntimeError("registry did not respond")

    async def _call(endpoint_url: str, node_id: str) -> str:
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
        raise RuntimeError("provider disconnected")

    async def _award(node_id: str):
        if award_points <= 0:
            return
        async with websockets.connect(reg) as ws:
            req = make_envelope("award", payload={"nodeId": node_id, "points": award_points, "registryToken": registry_token})
            await ws.send(req.to_json())
            env = Envelope.from_json(await ws.recv())
            if env.type != "award_ok":
                raise RuntimeError((env.payload or {}).get("message", "award failed"))

    async def _run():
        target = await _pick_target()
        endpoint = target.get("endpointUrl", "")
        node_id = target.get("nodeId", "")
        if not endpoint:
            raise RuntimeError("target missing endpointUrl")
        out = await _call(endpoint, node_id)
        await _award(node_id)
        return node_id, out

    node_id, out = asyncio.run(_run())
    console.print(f"[dim]provider[/dim] {node_id}")
    console.print(out)
