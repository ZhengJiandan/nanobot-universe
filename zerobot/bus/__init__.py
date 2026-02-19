"""Message bus module for decoupled channel-agent communication."""

from zerobot.bus.events import InboundMessage, OutboundMessage
from zerobot.bus.queue import MessageBus

__all__ = ["MessageBus", "InboundMessage", "OutboundMessage"]
