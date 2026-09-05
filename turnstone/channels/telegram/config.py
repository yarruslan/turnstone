"""Telegram-specific configuration."""

from __future__ import annotations

from dataclasses import dataclass, field

from turnstone.channels._config import ChannelConfig


@dataclass
class TelegramConfig(ChannelConfig):
    """Configuration for the Telegram channel adapter.

    Extends :class:`ChannelConfig` with Telegram-specific settings such as the
    bot token, the allowed-chat allowlist, and the streaming parameters.

    Telegram's hard per-message limit is 4096 characters; the adapter chunks
    longer content at :attr:`max_message_length` so a single assistant turn can
    exceed it.
    """

    bot_token: str = ""
    allowed_chat_ids: list[int] = field(default_factory=list)  # empty = all
    max_message_length: int = 4096  # Telegram's hard per-message limit
    streaming_edit_interval: float = 1.5  # seconds between message edits
