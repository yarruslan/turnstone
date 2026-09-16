"""Telegram bot adapter — connects Telegram chats to turnstone workstreams.

:class:`TurnstoneTelegramBot` uses python-telegram-bot (PTB) 21 with long
polling, so no public URL or webhook endpoint is required. Mirrors the Slack
adapter pattern: inbound messages are routed through
:class:`~turnstone.channels._routing.ChannelRouter`, and outbound content is
consumed from the server's per-workstream SSE endpoint
(``GET /v1/api/workstreams/{ws_id}/events``) via ``run_sse_stream``.

Interaction model
-----------------
* **Private chats**: every text message is routed freely; a private chat
  maps to one workstream (keyed by chat id).
* **Groups**: the bot only answers when it is mentioned or replied to, so
  it stays out of group traffic. The same per-chat workstream applies.
* ``/link <api_token>`` / ``/unlink`` map a Telegram user id onto a Turnstone
  account; unlinked users cannot drive sessions (mirrors Slack).

Install dependencies:
    pip install python-telegram-bot httpx httpx-sse
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import httpx
from telegram import Chat, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from turnstone.channels._config import MAX_NOTIFY_TRACKING
from turnstone.channels._formatter import chunk_message
from turnstone.channels._routing import (
    ChannelRouter,
    get_cycle_entry,
    pop_cycle_entry,
    pop_ws_entries,
)
from turnstone.channels._sse import run_sse_stream
from turnstone.core.log import get_logger
from turnstone.sdk.events import (
    ApprovalResolvedEvent,
    ApproveRequestEvent,
    ContentEvent,
    ErrorEvent,
    ServerEvent,
    StreamEndEvent,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from turnstone.channels.telegram.config import TelegramConfig
    from turnstone.core.storage._protocol import StorageBackend

log = get_logger(__name__)

_GREETING = "Session started — let me know what I can help with."

# Inbound-message limits — one user can't exhaust the LLM budget on
# their own (mirrors slack/bot.py).
_PER_USER_RATE_WINDOW_S: float = 60.0
_PER_USER_RATE_LIMIT: int = 10
_PER_USER_RATE_CAP: int = 4096  # LRU bound on the per-user deque map

# /link has a much tighter throttle than regular messages so throw-away
# Telegram accounts can't online-enumerate Turnstone API tokens.
_LINK_RATE_WINDOW_S: float = 3600.0
_LINK_RATE_LIMIT: int = 5
_LINK_RATE_CAP: int = 2048

# Bounded set of message ids we posted, used for group reply detection.
_SENT_MSGS_CAP: int = 1024


@dataclass(frozen=True)
class PendingApproval:
    """A posted approval prompt awaiting a button click."""

    channel: str
    message_id: int
    owner_user_id: str | None = None
    cycle_id: str = ""


@dataclass
class StreamingMessage:
    """Accumulates streamed content and periodically edits a Telegram message."""

    bot: Any  # telegram.Bot — API surface used: send_message / edit_message_text
    chat_id: int
    max_length: int = 4096
    edit_interval: float = 1.5

    _message_id: int | None = field(default=None, init=False, repr=False)
    _buffer: list[str] = field(default_factory=list, init=False, repr=False)
    # Rolling truncated in-progress display string; stops growing at
    # max_length so per-flush cost is O(max_length), not O(total).
    _display: str = field(default="", init=False, repr=False)
    _last_edit: float = field(default=0.0, init=False, repr=False)

    @property
    def message_id(self) -> int | None:
        return self._message_id

    async def append(self, text: str) -> None:
        self._buffer.append(text)
        if len(self._display) < self.max_length:
            self._display = (self._display + text)[: self.max_length]
        if time.monotonic() - self._last_edit >= self.edit_interval:
            await self._flush()

    async def finalize(self) -> None:
        content = "".join(self._buffer)
        chunks = chunk_message(content, self.max_length)
        for i, chunk in enumerate(chunks):
            if not chunk:
                continue
            try:
                if self._message_id is not None and i == 0:
                    await self.bot.edit_message_text(
                        chat_id=self.chat_id,
                        message_id=self._message_id,
                        text=chunk[: self.max_length],
                    )
                else:
                    msg = await self.bot.send_message(chat_id=self.chat_id, text=chunk)
                    if self._message_id is None and i == 0:
                        self._message_id = msg.message_id
            except Exception:
                log.debug("telegram.streaming.finalize_failed", exc_info=True)

    async def _flush(self) -> None:
        display = self._display
        if not display:
            return
        try:
            if self._message_id is None:
                msg = await self.bot.send_message(chat_id=self.chat_id, text=display)
                self._message_id = msg.message_id
            else:
                await self.bot.edit_message_text(
                    chat_id=self.chat_id,
                    message_id=self._message_id,
                    text=display[: self.max_length],
                )
        except Exception:
            log.debug("telegram.streaming.flush_failed", exc_info=True)
        self._last_edit = time.monotonic()


class TurnstoneTelegramBot:
    """Telegram bot bridging private chats / group mentions to workstreams."""

    channel_type: str = "telegram"
    _MAX_NOTIFY_TRACKING: int = MAX_NOTIFY_TRACKING

    def __init__(
        self,
        config: TelegramConfig,
        server_url: str,
        storage: StorageBackend,
        *,
        api_token: str = "",
        console_url: str = "",
        console_token_factory: Callable[[], str] | None = None,
        server_token_factory: Callable[[], str] | None = None,
    ) -> None:
        self.config = config
        self._server_url = server_url.rstrip("/")
        self._api_token = api_token
        self._token_factory = server_token_factory
        self.storage = storage

        self.router = ChannelRouter(
            server_url,
            storage,
            auto_approve=config.auto_approve,
            auto_approve_tools=list(config.auto_approve_tools),
            skill=config.skill,
            api_token=api_token,
            console_url=console_url,
            console_token_factory=console_token_factory,
            server_token_factory=server_token_factory,
        )

        self._subscribed_ws: set[str] = set()
        self._sse_tasks: dict[str, asyncio.Task[None]] = {}
        self._streaming: dict[str, StreamingMessage] = {}
        # (ws_id, cycle_id) → posted approval prompt. Parallel task agents
        # can hold several concurrent cycles per workstream.
        self._pending_approval: dict[tuple[str, str], PendingApproval] = {}
        self._notify_ws_map: dict[int, tuple[str, str]] = {}  # msg id → (ws, chat)
        # Per-workstream override routing the next streamed response into a
        # notification-reply conversation instead of the default session.
        self._notify_reply_routes: dict[str, str] = {}
        self._channel_sessions: dict[int, str] = {}  # chat_id → ws_id
        self._rate_buckets: OrderedDict[str, deque[float]] = OrderedDict()
        self._link_buckets: OrderedDict[str, deque[float]] = OrderedDict()
        self._bot_sent_msgs: OrderedDict[int, None] = OrderedDict()

        headers: dict[str, str] = {}
        if api_token and not server_token_factory:
            headers["Authorization"] = f"Bearer {api_token}"

        self._http_client = httpx.AsyncClient(
            headers=headers,
            timeout=httpx.Timeout(connect=10.0, read=90.0, write=10.0, pool=10.0),
        )

        # Build the PTB application eagerly; initialize/start are driven by
        # start() inside a running loop (PTB 21 async lifecycle).
        self._app = Application.builder().token(config.bot_token).build()
        self._app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self._on_message))
        self._app.add_handler(CommandHandler(["link", "unlink"], self._on_link_command))
        self._app.add_handler(CallbackQueryHandler(self._on_callback_query))

    # -- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        await self._recover_routes()
        log.info("telegram.starting_polling")
        await self._app.initialize()
        await self._app.start()
        updater = self._app.updater
        if updater is None:
            raise RuntimeError("telegram: application has no updater configured")
        await updater.start_polling(allowed_updates=Update.ALL_TYPES)

    async def stop(self) -> None:
        for ws_id in list(self._subscribed_ws):
            await self.unsubscribe_ws(ws_id)
        await self.router.aclose()
        await self._http_client.aclose()
        with contextlib.suppress(Exception):
            if self._app.running:
                await self._app.stop()
                await self._app.shutdown()
        log.info("telegram.stopped")

    async def _recover_routes(self) -> None:
        """Re-subscribe to SSE streams for existing telegram routes."""
        routes = await asyncio.to_thread(
            self.storage.list_channel_routes_by_type,
            "telegram",
        )
        for route in routes:
            ws_id = route["ws_id"]
            channel_id = route["channel_id"]
            await self.subscribe_ws(ws_id, channel_id)
            chat_id = _parse_chat_id(channel_id)
            if chat_id is not None:
                self._channel_sessions[chat_id] = ws_id
            log.info("telegram.route_recovered", ws_id=ws_id, channel_id=channel_id)

    # -- inbound -------------------------------------------------------------

    def _allow_user_send(self, user_key: str) -> bool:
        """Sliding-window send rate limit (mirrors slack/bot.py)."""
        now = time.monotonic()
        window_start = now - _PER_USER_RATE_WINDOW_S
        bucket = self._rate_buckets.get(user_key)
        if bucket is None:
            bucket = deque()
            self._rate_buckets[user_key] = bucket
            while len(self._rate_buckets) > _PER_USER_RATE_CAP:
                self._rate_buckets.popitem(last=False)
        else:
            self._rate_buckets.move_to_end(user_key)
        while bucket and bucket[0] < window_start:
            bucket.popleft()
        if len(bucket) >= _PER_USER_RATE_LIMIT:
            return False
        bucket.append(now)
        return True

    def _allow_link_attempt(self, user_key: str) -> bool:
        """Sliding-window throttle for /link (mirrors slack/bot.py)."""
        now = time.monotonic()
        window_start = now - _LINK_RATE_WINDOW_S
        bucket = self._link_buckets.get(user_key)
        if bucket is None:
            bucket = deque()
            self._link_buckets[user_key] = bucket
            while len(self._link_buckets) > _LINK_RATE_CAP:
                self._rate_buckets.popitem(last=False) if False else self._link_buckets.popitem(
                    last=False
                )
        else:
            self._link_buckets.move_to_end(user_key)
        while bucket and bucket[0] < window_start:
            bucket.popleft()
        if len(bucket) >= _LINK_RATE_LIMIT:
            return False
        bucket.append(now)
        return True

    def _remember_sent(self, message_id: int) -> None:
        self._bot_sent_msgs[message_id] = None
        while len(self._bot_sent_msgs) > _SENT_MSGS_CAP:
            self._bot_sent_msgs.popitem(last=False)

    async def _on_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        chat = update.effective_chat
        user = update.effective_user
        if message is None or chat is None or user is None:
            return

        text = (message.text or "").strip()
        if not text or not chat.id:
            return

        # Allowlist gate — empty list means all chats are allowed.
        if self.config.allowed_chat_ids and chat.id not in self.config.allowed_chat_ids:
            log.info("telegram.chat_not_allowed", chat_id=chat.id)
            return

        user_key = str(user.id)

        # Notification replies take precedence over session routing so a
        # reply to an outgoing notification lands on the originating ws.
        replied_to = message.reply_to_message
        if (
            replied_to is not None
            and replied_to.message_id in self._notify_ws_map
            and user_key == _parse_owner(self._notify_ws_map[replied_to.message_id][1])
        ):
            origin_ws_id, channel_id = self._notify_ws_map[replied_to.message_id]
            if not await self._require_linked(chat, user_key):
                return
            if not self._allow_user_send(user_key):
                await message.reply_text("You're sending messages too fast. Please slow down.")
                log.warning("telegram.inbound_rate_limited", user=user_key)
                return
            try:
                self._notify_reply_routes[origin_ws_id] = channel_id
                await self.router.send_message(origin_ws_id, text)
                log.info("telegram.notification_reply_routed", ws_id=origin_ws_id, chat_id=chat.id)
            except Exception:
                self._notify_reply_routes.pop(origin_ws_id, None)
                log.exception("telegram.notification_reply_failed")
            return

        # Group chats: only engage on mention or reply to one of our messages.
        if chat.type == Chat.GROUP:
            mentioned = False
            bot_username = context.bot.username or ""
            if bot_username and f"@{bot_username.lower()}" in text.lower():
                mentioned = True
            if replied_to is not None and replied_to.message_id in self._bot_sent_msgs:
                mentioned = True
            if not mentioned:
                return

        if not await self._require_linked(chat, user_key):
            return

        if not self._allow_user_send(user_key):
            await message.reply_text(
                f"You're sending messages too fast. Limit is "
                f"{_PER_USER_RATE_LIMIT} per {int(_PER_USER_RATE_WINDOW_S)} seconds."
            )
            log.warning("telegram.inbound_rate_limited", user=user_key)
            return

        channel_id = str(chat.id)
        try:
            ws_id, is_new = await self.router.get_or_create_workstream(
                channel_type="telegram",
                channel_id=channel_id,
                name=f"tg-{user.username or user.first_name or ''}".strip("-")[:100],
                client_type="chat",
            )
            if is_new:
                await self.subscribe_ws(ws_id, channel_id)
                self._channel_sessions[chat.id] = ws_id
                ack = await message.reply_text(_GREETING)
                self._remember_sent(ack.message_id)

            await self.router.send_message(ws_id, text)
            log.info("telegram.message_dispatched", ws_id=ws_id, chat_id=chat.id)
        except Exception:
            log.exception("telegram.dispatch_failed", chat_id=chat.id)
            with contextlib.suppress(Exception):
                await message.reply_text("Sorry, something went wrong routing your message.")

    async def _require_linked(self, chat: Chat, user_key: str) -> bool:
        """Gate on a Telegram → Turnstone account mapping (mirrors Slack)."""
        if await self.router.resolve_user("telegram", user_key):
            return True
        log.info("telegram.user_not_linked", chat_id=chat.id, user=user_key)
        with contextlib.suppress(Exception):
            await self._app.bot.send_message(
                chat_id=chat.id,
                text=(
                    "You need to link your Telegram account to a Turnstone user first. "
                    "Send `/link <your_api_token>` — tokens are managed in the Turnstone console."
                ),
            )
        return False

    async def _on_link_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /link <api_token> and /unlink (mirrors Slack's subcommands)."""
        from turnstone.core.auth import hash_token

        message = update.effective_message
        chat = update.effective_chat
        user = update.effective_user
        if message is None or chat is None or user is None:
            return

        user_key = str(user.id)

        # CommandHandler sets context.command at runtime (not in PTB stubs).
        if getattr(context, "command", None) == "unlink":
            existing = await asyncio.to_thread(self.storage.get_channel_user, "telegram", user_key)
            if not existing:
                await message.reply_text("Your Telegram account was not linked.")
                return
            await asyncio.to_thread(self.storage.delete_channel_user, "telegram", user_key)
            await message.reply_text("Unlinked.")
            log.info("telegram.unlink_succeeded", user=user_key)
            return

        # /link path
        if not self._allow_link_attempt(user_key):
            await message.reply_text(
                f"Too many /link attempts. Try again later — limit is {_LINK_RATE_LIMIT} per hour."
            )
            log.warning("telegram.link_rate_limited", user=user_key)
            return

        args = context.args
        token = args[0] if args else ""
        if not token:
            await message.reply_text("Usage: `/link <your_api_token>`")
            return

        existing = await asyncio.to_thread(self.storage.get_channel_user, "telegram", user_key)
        if existing:
            await message.reply_text("Your Telegram account is already linked. Use /unlink first.")
            return

        record = await asyncio.to_thread(self.storage.get_api_token_by_hash, hash_token(token))
        if record is None or not record.get("user_id"):
            # Generic failure — don't leak whether the token existed.
            await message.reply_text("Invalid token. Please provide a valid Turnstone API token.")
            log.info("telegram.link_invalid_token", user=user_key)
            return

        await asyncio.to_thread(
            self.storage.create_channel_user, "telegram", user_key, record["user_id"]
        )
        await message.reply_text("Linked. You can now use Turnstone from Telegram.")
        log.info(
            "telegram.link_succeeded",
            user=user_key,
            turnstone_user_id=record["user_id"],
        )

    # -- approval UI ---------------------------------------------------------

    async def _on_callback_query(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if query is None or not (query.data or "").startswith("ts_"):
            return

        parts = (query.data or "").split("|")
        if len(parts) != 3:
            return
        action, ws_id, cycle_id = parts
        approved = action == "ts_approve"

        entry = get_cycle_entry(self._pending_approval, ws_id, cycle_id)
        actor_user_id = str(query.from_user.id) if query.from_user else ""
        verb = "approve" if approved else "deny"

        # TODO(telegram): mirror Slack's owner-only gate once the pending
        # prompt carries an owner id; for now any chat member can click.
        if entry is None:
            await query.answer("This approval has already been handled.")
            return

        try:
            await self.router.send_approval(ws_id, cycle_id, approved=approved)
        except Exception:
            log.exception("telegram.approval_send_failed", ws_id=ws_id, cycle_id=cycle_id)
            await query.answer("Could not record your decision — please retry.", show_alert=True)
            return

        self._pending_approval.pop((ws_id, entry.cycle_id), None)
        try:
            await query.edit_message_text(
                text=f"Tool {'approved' if approved else 'denied'} ({actor_user_id})",
                reply_markup=None,
            )
        except Exception:
            log.debug("telegram.approval_edit_failed", exc_info=True)
        await query.answer(f"{verb.capitalize()} recorded.")

    # -- SSE / outbound ------------------------------------------------------

    async def subscribe_ws(self, ws_id: str, channel_id: str) -> None:
        prior = self._sse_tasks.get(ws_id)
        if prior is not None and prior.done():
            log.info("telegram.sse_task_recovered", ws_id=ws_id)
            self._sse_tasks.pop(ws_id, None)
            self._subscribed_ws.discard(ws_id)

        if ws_id in self._subscribed_ws:
            return

        task = asyncio.create_task(
            self._sse_listener(ws_id, channel_id),
            name=f"sse:{ws_id}",
        )
        self._sse_tasks[ws_id] = task
        self._subscribed_ws.add(ws_id)
        log.info("telegram.subscribed", ws_id=ws_id, channel_id=channel_id)

    def _clear_ws_state(self, ws_id: str) -> None:
        self._subscribed_ws.discard(ws_id)
        self._streaming.pop(ws_id, None)
        pop_ws_entries(self._pending_approval, ws_id)
        self._notify_reply_routes.pop(ws_id, None)
        stale = [mid for mid, entry in self._notify_ws_map.items() if entry[0] == ws_id]
        for mid in stale:
            del self._notify_ws_map[mid]

    async def unsubscribe_ws(self, ws_id: str) -> None:
        task = self._sse_tasks.pop(ws_id, None)
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._clear_ws_state(ws_id)
        log.info("telegram.unsubscribed", ws_id=ws_id)

    async def _sse_listener(self, ws_id: str, channel_id: str) -> None:
        """Connect to the server SSE endpoint and dispatch events."""

        async def _on_event(event: ServerEvent) -> None:
            effective_route = self._notify_reply_routes.get(ws_id, channel_id)
            await self._on_ws_event(ws_id, effective_route, event)

        async def _on_unavailable() -> None:
            chat_id = _parse_chat_id(channel_id)
            if chat_id is not None and self._channel_sessions.get(chat_id) == ws_id:
                del self._channel_sessions[chat_id]
            await self.router.delete_route("telegram", channel_id)
            # Called from inside the SSE task itself — don't await it here.
            self._sse_tasks.pop(ws_id, None)
            self._clear_ws_state(ws_id)
            log.info("telegram.stale_route_removed", ws_id=ws_id)

        await run_sse_stream(
            http_client=self._http_client,
            log_prefix="telegram",
            ws_id=ws_id,
            node_url_fn=self.router.get_node_url,
            token_factory=self._token_factory,
            on_event=_on_event,
            on_unavailable=_on_unavailable,
        )

    async def _on_ws_event(self, ws_id: str, channel_id: str, event: ServerEvent) -> None:
        """Dispatch a typed server event to its per-event handler."""
        if isinstance(event, ContentEvent):
            await self._handle_content(ws_id, channel_id, event)
        elif isinstance(event, ApproveRequestEvent):
            await self._handle_approve_request(ws_id, channel_id, event)
        elif isinstance(event, ApprovalResolvedEvent):
            await self._handle_approval_resolved(ws_id, event)
        elif isinstance(event, StreamEndEvent):
            await self._handle_stream_end(ws_id)
        elif isinstance(event, ErrorEvent):
            await self._send_text(channel_id, f"Error: {event.message[:500]}")

    async def _handle_content(self, ws_id: str, channel_id: str, event: ContentEvent) -> None:
        chat_id = _parse_chat_id(channel_id)
        if chat_id is None:
            return
        sm = self._streaming.get(ws_id)
        if sm is None or sm.chat_id != chat_id:
            # Finalize the outgoing stream before swapping so buffered tokens
            # still land on the old conversation.
            if sm is not None:
                await sm.finalize()
            sm = StreamingMessage(
                bot=self._app.bot,
                chat_id=chat_id,
                max_length=self.config.max_message_length,
                edit_interval=self.config.streaming_edit_interval,
            )
            self._streaming[ws_id] = sm
        await sm.append(event.text)

    async def _handle_approve_request(
        self, ws_id: str, channel_id: str, event: ApproveRequestEvent
    ) -> None:
        chat_id = _parse_chat_id(channel_id)
        if chat_id is None:
            return

        cycle_id = event.cycle_id
        verdict = await self.router.evaluate_tool_policies(event.items)
        policy_handled = False
        if verdict.kind == "deny":
            denied = ", ".join(verdict.denied_tools)
            await self.router.send_approval(
                ws_id,
                cycle_id,
                approved=False,
                feedback=f"Blocked by tool policy: {denied}",
            )
            await self._send_text(channel_id, f"Tool blocked by admin policy: {denied}")
            policy_handled = True
        elif verdict.kind == "allow":
            await self.router.send_approval(ws_id, cycle_id, approved=True)
            await self._send_text(channel_id, "Tool approved by policy.")
            policy_handled = True

        if not policy_handled and (self.config.auto_approve or self._should_auto_approve(event)):
            await self.router.send_approval(ws_id, cycle_id, approved=True)
            await self._send_text(channel_id, "Tool auto-approved.")
        elif not policy_handled:
            await self._send_approval_request(ws_id, cycle_id, event.items, channel_id)

    def _should_auto_approve(self, event: ApproveRequestEvent) -> bool:
        allowed = self.config.auto_approve_tools
        if not allowed or not event.items:
            return False
        for item in event.items:
            name = (
                item.get("func_name")
                or item.get("approval_label")
                or item.get("function", {}).get("name", "")
            )
            if name not in allowed:
                return False
        return True

    async def _send_approval_request(
        self,
        ws_id: str,
        cycle_id: str,
        items: list[dict[str, Any]],
        channel_id: str,
    ) -> None:
        chat_id = _parse_chat_id(channel_id)
        if chat_id is None:
            return

        lines = ["*Tool Approval Required*", ""]
        for item in items:
            name = item.get("approval_label") or item.get("func_name") or "tool"
            preview = (item.get("preview") or "")[:600]
            lines.append(f"• {name}")
            if preview:
                lines.append(preview)

        # Telegram inline-keyboard callback_data is capped at 64 bytes.
        keyboard = [
            [
                InlineKeyboardButton(text="Approve", callback_data=f"ts_approve|{ws_id}|"),
                InlineKeyboardButton(text="Deny", callback_data=f"ts_deny|{ws_id}|"),
            ]
        ]

        try:
            msg = await self._app.bot.send_message(
                chat_id=chat_id,
                text="\n".join(lines)[:4096],
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
        except Exception:
            log.exception("telegram.approval_request_send_failed", ws_id=ws_id)
            return

        self._pending_approval[(ws_id, cycle_id)] = PendingApproval(
            channel=channel_id,
            message_id=msg.message_id,
            owner_user_id=None,  # TODO(telegram): stamp the requesting chat member
            cycle_id=cycle_id,
        )
        self._remember_sent(msg.message_id)
        log.info(
            "telegram.pending_approval_stored",
            ws_id=ws_id,
            cycle_id=cycle_id,
            message_id=msg.message_id,
        )

    async def _handle_approval_resolved(self, ws_id: str, event: ApprovalResolvedEvent) -> None:
        entry = pop_cycle_entry(self._pending_approval, ws_id, event.cycle_id)
        if entry is None:
            return
        label = "Approved" if event.approved else "Denied"
        try:
            await self._app.bot.edit_message_text(
                chat_id=_parse_chat_id(entry.channel),
                message_id=entry.message_id,
                text=f"{label} (tool call resolved)",
            )
        except Exception:
            log.debug("telegram.approval_resolved_edit_failed", exc_info=True)

    async def _handle_stream_end(self, ws_id: str) -> None:
        sm = self._streaming.pop(ws_id, None)
        if sm is not None:
            await sm.finalize()
            # Track the final message so replies to it route back here.
            reply_route = self._notify_reply_routes.pop(ws_id, None)
            if reply_route is not None and sm.message_id is not None:
                chat_id = _parse_chat_id(reply_route)
                if chat_id is not None:
                    while len(self._notify_ws_map) >= self._MAX_NOTIFY_TRACKING:
                        del self._notify_ws_map[next(iter(self._notify_ws_map))]
                    self._notify_ws_map[sm.message_id] = (ws_id, reply_route)

    async def _send_text(self, channel_id: str, text: str) -> None:
        """Send a single short status/error line to a chat (best-effort)."""
        chat_id = _parse_chat_id(channel_id)
        if chat_id is None or not text:
            return
        try:
            msg = await self._app.bot.send_message(chat_id=chat_id, text=text[:4096])
            self._remember_sent(msg.message_id)
        except Exception:
            log.debug("telegram.send_text_failed", exc_info=True)

    # -- ChannelAdapter interface ---------------------------------------------

    async def send(self, channel_id: str, content: str) -> str:
        chat_id = _parse_chat_id(channel_id)
        if chat_id is None:
            return ""
        first_msg_id = ""
        for chunk in chunk_message(content, self.config.max_message_length):
            if not chunk:
                continue
            msg = await self._app.bot.send_message(chat_id=chat_id, text=chunk)
            self._remember_sent(msg.message_id)
            if not first_msg_id:
                first_msg_id = str(msg.message_id)
        return first_msg_id

    async def send_notification(self, channel_id: str, content: str, ws_id: str) -> str:
        msg_id = await self.send(channel_id, content)
        chat_id = _parse_chat_id(channel_id)
        if msg_id and ws_id and chat_id is not None:
            while len(self._notify_ws_map) >= self._MAX_NOTIFY_TRACKING:
                del self._notify_ws_map[next(iter(self._notify_ws_map))]
            self._notify_ws_map[int(msg_id)] = (ws_id, channel_id)
        return msg_id


def _parse_chat_id(channel_id: str) -> int | None:
    """Parse a telegram channel_id (stringified chat id) back to an int."""
    try:
        return int(channel_id)
    except ValueError:
        return None


def _parse_owner(channel_id: str) -> str:
    """Best-effort owner key for notification-reply author matching.

    Telegram's per-chat workstream is keyed by chat id, so the "owner" of a
    notification reply is effectively the chat itself; we compare against the
    stringified user id which equals the chat id in private chats (the common
    case) and degrades to always-allowing group replies.

    TODO(telegram): tighten once group notifications carry an explicit owner.
    """
    return channel_id
