"""Public Universe Node service (direct task execution endpoint).

MVP only supports `llm.chat` tasks and does not allow tool usage.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from loguru import logger

import websockets
from websockets.server import WebSocketServerProtocol

from nanobot.config.loader import load_config
from nanobot.providers.litellm_provider import LiteLLMProvider
from nanobot.universe.protocol import Envelope, make_envelope


@dataclass
class NodeServerConfig:
    host: str = "0.0.0.0"
    port: int = 18998
    service_token: str = ""  # if set, required for task_run


class NodeServer:
    def __init__(self, cfg: NodeServerConfig | None = None) -> None:
        self.cfg = cfg or NodeServerConfig()
        self.bound_port: int = self.cfg.port
        self._server: websockets.server.Serve | None = None

    async def start(self) -> None:
        self._server = await websockets.serve(self._handler, self.cfg.host, self.cfg.port, ping_interval=20, ping_timeout=20)
        try:
            if self._server.sockets:
                self.bound_port = int(self._server.sockets[0].getsockname()[1])
        except Exception:
            self.bound_port = self.cfg.port
        logger.info(f"Universe node service listening on ws://{self.cfg.host}:{self.bound_port}")

    async def stop(self) -> None:
        if not self._server:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None

    def _check_token(self, provided: str) -> bool:
        if not self.cfg.service_token:
            return True
        return provided == self.cfg.service_token

    async def _handler(self, ws: WebSocketServerProtocol) -> None:
        try:
            async for raw in ws:
                try:
                    env = Envelope.from_json(raw)
                except Exception as e:
                    await ws.send(make_envelope("error", payload={"message": f"bad json: {e}"}).to_json())
                    continue

                if env.type == "ping":
                    await ws.send(make_envelope("pong", id=env.id).to_json())
                    continue

                if env.type != "task_run":
                    await ws.send(make_envelope("error", id=env.id, payload={"message": "expected task_run"}).to_json())
                    continue

                token = (env.payload or {}).get("serviceToken", "")
                if not self._check_token(token):
                    await ws.send(make_envelope("error", id=env.id, payload={"message": "invalid service token"}).to_json())
                    continue

                kind = (env.payload or {}).get("kind", "")
                if kind not in {"llm.chat", "echo"}:
                    await ws.send(
                        make_envelope("error", id=env.id, payload={"message": f"unsupported kind: {kind}"}).to_json()
                    )
                    continue

                prompt = (env.payload or {}).get("prompt", "")
                if not prompt:
                    await ws.send(make_envelope("error", id=env.id, payload={"message": "missing prompt"}).to_json())
                    continue

                try:
                    if kind == "echo":
                        result = prompt
                    else:
                        result = await self._run_llm_chat(prompt)
                    await ws.send(make_envelope("task_result", id=env.id, payload={"content": result}).to_json())
                except Exception as e:
                    await ws.send(make_envelope("task_error", id=env.id, payload={"message": str(e)}).to_json())
        except websockets.ConnectionClosed:
            return

    async def _run_llm_chat(self, prompt: str) -> str:
        cfg = load_config()
        model = cfg.agents.defaults.model
        provider_cfg, provider_name = cfg._match_provider(model)
        if not provider_cfg:
            raise RuntimeError("No provider configured for model. Set providers.*.apiKey in ~/.nanobot/config.json")

        provider = LiteLLMProvider(
            api_key=provider_cfg.api_key,
            api_base=provider_cfg.api_base,
            default_model=model,
            extra_headers=provider_cfg.extra_headers,
            provider_name=provider_name,
        )

        max_tokens = min(int(getattr(cfg.universe, "public_max_tokens", 1024) or 1024), 2048)
        resp = await provider.chat(
            messages=[{"role": "user", "content": prompt}],
            tools=None,
            model=model,
            max_tokens=max_tokens,
            temperature=cfg.agents.defaults.temperature,
        )
        return resp.content or ""
