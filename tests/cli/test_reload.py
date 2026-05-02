"""Unit tests for the ``murmur serve / worker --reload`` helper."""

from __future__ import annotations

import os

import pytest

from murmur.cli._reload import _strip_reload_flags, is_reload_child


class TestStripReloadFlags:
    """``_strip_reload_flags`` removes reload-only flags so the child
    doesn't recurse into another reload watcher."""

    def test_drops_bare_reload(self) -> None:
        assert _strip_reload_flags(["murmur", "serve", "--reload"]) == [
            "murmur",
            "serve",
        ]

    def test_drops_reload_dir_space_form(self) -> None:
        assert _strip_reload_flags(
            ["murmur", "serve", "--reload-dir", "./specs", "--port", "9000"]
        ) == ["murmur", "serve", "--port", "9000"]

    def test_drops_reload_dir_equals_form(self) -> None:
        assert _strip_reload_flags(
            ["murmur", "serve", "--reload-dir=./specs", "--port", "9000"]
        ) == ["murmur", "serve", "--port", "9000"]

    def test_drops_reload_include_and_exclude(self) -> None:
        assert _strip_reload_flags(
            [
                "murmur",
                "worker",
                "start",
                "--reload",
                "--reload-include",
                "*.py",
                "--reload-exclude=*.tmp",
                "--agents",
                "researcher",
            ]
        ) == ["murmur", "worker", "start", "--agents", "researcher"]

    def test_preserves_unrelated_flags(self) -> None:
        argv = [
            "murmur",
            "serve",
            "--specs",
            "./specs",
            "--port",
            "8420",
            "--broker",
            "kafka://x",
        ]
        assert _strip_reload_flags(argv) == argv

    def test_handles_repeated_reload_dir(self) -> None:
        assert _strip_reload_flags(
            [
                "murmur",
                "serve",
                "--reload",
                "--reload-dir",
                "./specs",
                "--reload-dir",
                "./src",
            ]
        ) == ["murmur", "serve"]


class TestReloadChildSentinel:
    """The sentinel env var is how the child knows to skip the reload
    branch — without it ``--reload`` would fork-bomb."""

    def test_unset_returns_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("_MURMUR_RELOAD_CHILD", raising=False)
        assert is_reload_child() is False

    def test_set_to_one_returns_true(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("_MURMUR_RELOAD_CHILD", "1")
        assert is_reload_child() is True

    def test_set_to_other_value_returns_false(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Truthy-but-not-"1" must NOT trigger child mode — keeps the
        # contract narrow.
        monkeypatch.setenv("_MURMUR_RELOAD_CHILD", "true")
        assert is_reload_child() is False

    def test_external_env_isolation(self) -> None:
        # Sanity: tests don't leak the sentinel into the wider process
        # env unless a test explicitly sets it via monkeypatch.
        assert os.environ.get("_MURMUR_RELOAD_CHILD") in (None, "0")
