"""Universe tool: delegate work to public universe nodes."""

from __future__ import annotations

from typing import Any

from nanobot.agent.tools.base import Tool
from nanobot.config.loader import load_config
from nanobot.universe.public_client import delegate_task


class UniverseHelpTool(Tool):
    name = "universe_help"
    description = (
        "Ask the public nanobot universe for help by delegating a task to another node. "
        "Use when local tools/keys/resources are missing or the task is blocked."
    )
    parameters = {
        "type": "object",
        "properties": {
            "prompt": {"type": "string", "description": "The task to delegate"},
            "kind": {"type": "string", "enum": ["nanobot.agent", "llm.chat", "echo"], "default": "nanobot.agent"},
            "requireCapability": {"type": "string", "description": "Capability key required (default: kind)"},
            "toNodeId": {"type": "string", "description": "Optional specific node_id to call"},
            "serviceToken": {"type": "string", "description": "Service token if required by the provider node"},
            "maxPricePoints": {"type": "integer", "minimum": 1, "description": "Optional max price points"},
        },
        "required": ["prompt"],
    }

    async def execute(
        self,
        prompt: str,
        kind: str = "nanobot.agent",
        requireCapability: str | None = None,
        toNodeId: str | None = None,
        serviceToken: str = "",
        maxPricePoints: int | None = None,
        **kwargs: Any,
    ) -> str:
        cfg = load_config()
        if not cfg.universe.public_enabled:
            return "Error: Public universe is disabled. Enable it with `nanobot universe public enable`."

        try:
            node, out = await delegate_task(
                registry_url=cfg.universe.public_registry_url,
                kind=kind,
                prompt=prompt,
                require_capability=requireCapability,
                to_node_id=toNodeId,
                service_token=serviceToken,
                max_price_points=maxPricePoints,
            )
            return f"[universe:{node.node_id}] {out}"
        except Exception as e:
            return f"Error: universe delegation failed: {e}"

