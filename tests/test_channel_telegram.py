"""Tests for the Telegram channel adapter (bot, config, CLI wiring)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytest.importorskip("telegram")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run(coro):  # type: ignore[no-untyped-def]
    """Run an async coroutine in a fresh event loop (no pytest-asyncio needed)."""
    return asyncio.run(coro)


def _make_update(
    *,
    chat_id: int = 42,
    user_id: int = 1001,
    text: str | None = "hello",
    chat_type: str = "private",
    reply_to_message_id: int | None = None,
) -> MagicMock:
    """Build a PTB ``Update`` stand-in with the attributes _on_message touches."""
    update = MagicMock()

    message = MagicMock()
    message.text = text
    if reply_to_message_id is not None:
        reply = MagicMock()
        reply.message_id = reply_to_message_id
        message.reply_to_message = reply
    else:
        message.reply_to_message = None
    message.reply_text = AsyncMock(return_value=MagicMock(message_id=1))

    chat = MagicMock()
    chat.id = chat_id
    chat.type = chat_type

    user = MagicMock()
    user.id = user_id
    user.username = "tester"
    user.first_name = "Tester"

    update.effective_message = message
    update.effective_chat = chat
    update.effective_user = user
    return update


def _make_context(bot_username: str | None = "ts_bot") -> MagicMock:
    context = MagicMock()
    if bot_username is not None:
        context.bot.username = bot_username
    else:
        context.bot.username = None
    return context


def _pending_approval_entry():  # type: ignore[no-untyped-def]
    from turnstone.channels.telegram.bot import PendingApproval

    return PendingApproval(channel="42", message_id=99, cycle_id="cyc-1")


def _make_bot(allowed_chat_ids: list[int] | None = None) -> tuple[object, MagicMock]:
    """Build a TurnstoneTelegramBot with fully mocked dependencies."""
    from turnstone.channels.telegram.bot import TurnstoneTelegramBot
    from turnstone.channels.telegram.config import TelegramConfig

    config = TelegramConfig(
        bot_token="123456:TEST-TOKEN",
        allowed_chat_ids=allowed_chat_ids or [],
        auto_approve=False,
    )

    storage = MagicMock()
    storage.list_channel_routes_by_type = MagicMock(return_value=[])

    router = MagicMock()
    router.get_or_create_workstream = AsyncMock(return_value=("ws-1", True))
    router.send_message = AsyncMock()
    router.send_approval = AsyncMock()
    router.resolve_user = AsyncMock(return_value="turnstone-user-1")
    router.delete_route = AsyncMock()
    router.close_workstream = AsyncMock()
    router.aclose = AsyncMock()

    with (
        patch("turnstone.channels.telegram.bot.httpx.AsyncClient", return_value=AsyncMock()),
        patch("turnstone.channels.telegram.bot.Application.builder") as builder_mock,
    ):
        app = MagicMock()
        builder_mock.return_value.token.return_value.build.return_value = app
        bot = TurnstoneTelegramBot(config, server_url="http://localhost:8080", storage=storage)

    bot.router = router  # type: ignore[attr-defined]
    return bot, router


# ---------------------------------------------------------------------------
# TelegramConfig
# ---------------------------------------------------------------------------


class TestTelegramConfig:
    """Tests for TelegramConfig default and custom values."""

    def test_defaults(self) -> None:
        from turnstone.channels.telegram.config import TelegramConfig

        cfg = TelegramConfig()
        assert cfg.bot_token == ""
        assert cfg.allowed_chat_ids == []
        assert cfg.max_message_length == 4096
        assert cfg.streaming_edit_interval == 1.5
        # Inherited from ChannelConfig
        assert cfg.model == ""
        assert cfg.auto_approve is False

    def test_custom_values(self) -> None:
        from turnstone.channels.telegram.config import TelegramConfig

        cfg = TelegramConfig(
            bot_token="123456:ABC",
            allowed_chat_ids=[1, 2],
            max_message_length=2048,
            streaming_edit_interval=0.5,
            model="local",
            auto_approve=True,
        )
        assert cfg.bot_token == "123456:ABC"
        assert cfg.allowed_chat_ids == [1, 2]
        assert cfg.max_message_length == 2048
        assert cfg.streaming_edit_interval == 0.5
        assert cfg.model == "local"
        assert cfg.auto_approve is True


# ---------------------------------------------------------------------------
# Parsed helpers
# ---------------------------------------------------------------------------


class TestParseHelpers:
    """Tests for _parse_chat_id / _parse_owner."""

    def test_parse_chat_id_valid(self) -> None:
        from turnstone.channels.telegram.bot import _parse_chat_id

        assert _parse_chat_id("42") == 42
        assert _parse_chat_id("-1001234567890") == -1001234567890

    def test_parse_chat_id_invalid(self) -> None:
        from turnstone.channels.telegram.bot import _parse_chat_id

        assert _parse_chat_id("not-an-int") is None
        assert _parse_chat_id("") is None
        assert _parse_chat_id("42abc") is None

    def test_round_trip(self) -> None:
        """Every channel_id the bot emits must round-trip through _parse_chat_id."""
        from turnstone.channels.telegram.bot import _parse_chat_id

        for raw in (42, -1001234567890):
            assert _parse_chat_id(str(raw)) == raw


# ---------------------------------------------------------------------------
# Message routing
# ---------------------------------------------------------------------------


class TestMessageRouting:
    """Tests for TurnstoneTelegramBot._on_message gating and dispatch."""

    def test_ignores_empty_text(self) -> None:
        bot, router = _make_bot()
        update = _make_update(text="   ")
        _run(bot._on_message(update, _make_context()))  # type: ignore[attr-defined]
        router.get_or_create_workstream.assert_not_called()

    def test_ignores_disallowed_chat(self) -> None:
        bot, router = _make_bot(allowed_chat_ids=[99])
        update = _make_update(chat_id=42)
        _run(bot._on_message(update, _make_context()))  # type: ignore[attr-defined]
        router.get_or_create_workstream.assert_not_called()

    def test_group_ignores_unmentioned_message(self) -> None:
        bot, router = _make_bot()
        update = _make_update(chat_id=-10042, chat_type="group", text="random chatter")
        _run(bot._on_message(update, _make_context()))  # type: ignore[attr-defined]
        router.get_or_create_workstream.assert_not_called()

    def test_group_mention_dispatches(self) -> None:
        bot, router = _make_bot()
        update = _make_update(chat_id=-10042, chat_type="group", text="@ts_bot please help")
        _run(bot._on_message(update, _make_context()))  # type: ignore[attr-defined]
        assert router.get_or_create_workstream.call_count == 1
        kwargs = router.get_or_create_workstream.await_args.kwargs
        assert kwargs["channel_type"] == "telegram"
        assert kwargs["channel_id"] == "-10042"

    def test_group_reply_to_bot_message_dispatches(self) -> None:
        bot, router = _make_bot()
        update = _make_update(chat_id=-10042, chat_type="group", reply_to_message_id=7)
        bot._bot_sent_msgs[7] = None  # type: ignore[attr-defined]
        _run(bot._on_message(update, _make_context()))  # type: ignore[attr-defined]
        router.get_or_create_workstream.assert_called()

    def test_private_chat_dispatches(self) -> None:
        bot, router = _make_bot()
        update = _make_update(chat_id=42, text="hello")
        _run(bot._on_message(update, _make_context()))  # type: ignore[attr-defined]
        assert router.get_or_create_workstream.call_count == 1
        kwargs = router.get_or_create_workstream.await_args.kwargs
        assert kwargs["channel_id"] == "42"
        assert kwargs["client_type"] == "chat"
        router.send_message.assert_awaited_once_with("ws-1", "hello")

    def test_unlinked_user_not_dispatched(self) -> None:
        bot, router = _make_bot()
        router.resolve_user = AsyncMock(return_value=None)
        update = _make_update(chat_id=42, text="hello")
        _run(bot._on_message(update, _make_context()))  # type: ignore[attr-defined]
        assert router.get_or_create_workstream.call_count == 0


# ---------------------------------------------------------------------------
# SSE wiring (run_sse_stream call site)
# ---------------------------------------------------------------------------


class TestSseWiring:
    """Tests that _sse_listener calls run_sse_stream with the current shared API.

    Regression for the on_stale -> on_unavailable rename: subscribe_ws
    fire-and-forgets _sse_listener via create_task and never awaits it, so a
    bad kwarg raises TypeError that asyncio swallows ("Task exception was
    never retrieved") and no test fails. We stub run_sse_stream with the SAME
    keyword-only signature as the real helper so a wrong kwarg raises exactly
    as in production, then await the listener task (suppressing only
    CancelledError) so that TypeError surfaces as a test failure.
    """

    def test_run_sse_stream_called_with_on_unavailable(self) -> None:
        import contextlib

        from turnstone.channels.telegram import bot as bot_mod

        calls: list[dict] = []

        async def fake_run_sse_stream(
            *,
            http_client,  # type: ignore[no-untyped-def]
            log_prefix,  # type: ignore[no-untyped-def]
            ws_id,  # type: ignore[no-untyped-def]
            node_url_fn,  # type: ignore[no-untyped-def]
            token_factory,  # type: ignore[no-untyped-def]
            on_event,  # type: ignore[no-untyped-def]
            on_unavailable,  # type: ignore[no-untyped-def]
        ) -> None:
            """Mirror the real run_sse_stream keyword-only signature."""
            calls.append(
                {
                    "ws_id": ws_id,
                    "on_event": on_event,
                    "on_unavailable": on_unavailable,
                }
            )
            raise asyncio.CancelledError  # stop the infinite loop after capture

        bot, router = _make_bot()

        async def drive() -> None:
            with patch.object(bot_mod, "run_sse_stream", fake_run_sse_stream):
                # Same entry point the other routing tests use: a private-chat
                # message on a new ws triggers subscribe_ws -> _sse_listener.
                update = _make_update(chat_id=42, text="hello")
                await bot._on_message(update, _make_context())  # type: ignore[attr-defined]
                # Await the fire-and-forgotten task so a TypeError can't be
                # swallowed. Suppress ONLY CancelledError (our stub's exit
                # signal) — NOT Exception — so a real TypeError propagates.
                task = bot._sse_tasks["ws-1"]  # type: ignore[attr-defined]
                with contextlib.suppress(asyncio.CancelledError):
                    await task

        _run(drive())

        assert len(calls) == 1
        assert calls[0]["ws_id"] == "ws-1"
        assert callable(calls[0]["on_unavailable"])

        # Invoke the captured on_unavailable and assert it ran the cleanup:
        # route deleted + ws state cleared (session dropped, task popped).
        bot._channel_sessions[42] = "ws-1"  # type: ignore[attr-defined]
        _run(calls[0]["on_unavailable"]())
        router.delete_route.assert_awaited_once_with("telegram", "42")
        assert "ws-1" not in bot._sse_tasks  # type: ignore[attr-defined]
        assert "ws-1" not in bot._channel_sessions  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Approval callbacks
# ---------------------------------------------------------------------------


class TestApprovalCallbacks:
    """Tests for TurnstoneTelegramBot._on_callback_query."""

    def test_approve_records_and_edits(self) -> None:
        bot, router = _make_bot()
        bot._pending_approval[("ws-1", "cyc-1")] = _pending_approval_entry()  # type: ignore[attr-defined]

        update = MagicMock()
        query = MagicMock()
        query.data = "ts_approve|ws-1|"
        from_user = MagicMock()
        from_user.id = 1001
        query.from_user = from_user
        query.answer = AsyncMock()
        query.edit_message_text = AsyncMock()
        update.callback_query = query

        _run(bot._on_callback_query(update, MagicMock()))  # type: ignore[attr-defined]
        router.send_approval.assert_awaited_once_with("ws-1", "", approved=True)
        query.answer.assert_called()
        assert "approved" in query.edit_message_text.call_args.kwargs["text"]

    def test_deny_records(self) -> None:
        bot, router = _make_bot()
        # Legacy-style key (empty cycle_id): buttons emit "ts_deny|ws-2|" so
        # the empty-cycle fallback path resolves it.
        bot._pending_approval[("ws-2", "")] = _pending_approval_entry()  # type: ignore[attr-defined]

        update = MagicMock()
        query = MagicMock()
        query.data = "ts_deny|ws-2|"
        query.from_user = None
        query.answer = AsyncMock()
        query.edit_message_text = AsyncMock()
        update.callback_query = query

        _run(bot._on_callback_query(update, MagicMock()))  # type: ignore[attr-defined]
        router.send_approval.assert_awaited_once_with("ws-2", "", approved=False)

    def test_stale_callback_answers_handled(self) -> None:
        bot, router = _make_bot()
        update = MagicMock()
        query = MagicMock()
        query.data = "ts_approve|ws-gone|"
        query.from_user = None
        query.answer = AsyncMock()
        query.edit_message_text = AsyncMock()
        update.callback_query = query

        _run(bot._on_callback_query(update, MagicMock()))  # type: ignore[attr-defined]
        router.send_approval.assert_not_awaited()
        assert "handled" in query.answer.call_args.args[0]

    def test_foreign_data_ignored(self) -> None:
        bot, router = _make_bot()
        update = MagicMock()
        query = MagicMock()
        query.data = "other|prefix|nope"
        query.from_user = None
        query.answer = AsyncMock()
        update.callback_query = query

        _run(bot._on_callback_query(update, MagicMock()))  # type: ignore[attr-defined]
        router.send_approval.assert_not_awaited()
        query.answer.assert_not_called()


# ---------------------------------------------------------------------------
# /link command
# ---------------------------------------------------------------------------


class TestLinkCommand:
    """Tests for TurnstoneTelegramBot._on_link_command argument parsing."""

    def _link_context(self, args: list[str]) -> MagicMock:
        context = _make_context()
        context.command = "link"
        context.args = args
        return context

    def test_single_token_passes_to_validation(self) -> None:
        """The normal private-chat case: /link <token> with exactly one arg."""
        from turnstone.core.auth import hash_token

        bot, _router = _make_bot()
        update = _make_update()
        bot.storage.get_channel_user = MagicMock(return_value=None)  # type: ignore[attr-defined]
        bot.storage.get_api_token_by_hash = MagicMock(return_value={"user_id": "ts-user-9"})  # type: ignore[attr-defined]
        bot.storage.create_channel_user = MagicMock()  # type: ignore[attr-defined]

        _run(bot._on_link_command(update, self._link_context(["tok-123"])))  # type: ignore[attr-defined]

        # The single token reaches validation — the original bug dropped it.
        bot.storage.get_api_token_by_hash.assert_called_once_with(  # type: ignore[attr-defined]
            hash_token("tok-123")
        )
        bot.storage.create_channel_user.assert_called_once()  # type: ignore[attr-defined]
        reply = update.effective_message.reply_text.call_args.args[0]
        assert "Linked" in reply

    def test_empty_args_yields_usage(self) -> None:
        bot, _router = _make_bot()
        update = _make_update()

        _run(bot._on_link_command(update, self._link_context([])))  # type: ignore[attr-defined]

        reply = update.effective_message.reply_text.call_args.args[0]
        assert "Usage" in reply


# ---------------------------------------------------------------------------
# CLI wiring
# ---------------------------------------------------------------------------


class TestCliWiring:
    """Tests that turnstone-channel exposes the Telegram adapter."""

    def test_has_adapters_includes_telegram(self) -> None:
        """A telegram token alone must count as 'has adapters' for URL validation."""
        source = open(  # noqa: SIM115 — static wiring assertion, not runtime I/O
            "turnstone/channels/cli.py"
        ).read()
        assert "args.telegram_token" in source
        assert "bool(args.discord_token or args.slack_token or args.telegram_token)" in source

    def test_telegram_channels_flag_parses_chat_ids(self) -> None:
        """--telegram-channels is registered and parses comma-separated int chat IDs."""
        from turnstone.channels.cli import _build_parser

        parser = _build_parser()
        args = parser.parse_args(["--telegram-token", "1:AA", "--telegram-channels", "1, -200 , 3"])
        chat_ids = [int(c.strip()) for c in args.telegram_channels.split(",") if c.strip()]
        assert chat_ids == [1, -200, 3]
        # Omitting the flag yields an empty allowlist = allow all chats.
        assert parser.parse_args(["--telegram-token", "1:AA"]).telegram_channels == ""
