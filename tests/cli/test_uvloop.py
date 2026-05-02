"""Tests for ``murmur.cli._uvloop`` — opt-in uvloop policy installation.

The actual policy install at runtime is verified through a roundtrip
when uvloop is on the platform; the fail-soft paths (extra missing,
Windows, env-var fallback) are verified directly. The argparse plumbing
(``--uvloop`` flag on serve / worker start) gets exercised through
``murmur.cli.build_parser``.
"""

from __future__ import annotations

import asyncio
import sys

import pytest

from murmur.cli import build_parser
from murmur.cli._uvloop import install_uvloop_policy, resolve_uvloop_enabled

# ---------------------------------------------------------------------------
# argparse — flag is present on serve and worker start
# ---------------------------------------------------------------------------


def test_serve_has_uvloop_flag() -> None:
    parser = build_parser()
    args = parser.parse_args(["serve", "--uvloop"])
    assert args.uvloop is True


def test_serve_uvloop_default_false() -> None:
    parser = build_parser()
    args = parser.parse_args(["serve"])
    assert args.uvloop is False


def test_worker_start_has_uvloop_flag() -> None:
    parser = build_parser()
    args = parser.parse_args(
        ["worker", "start", "--agents", "x", "--broker", "memory://", "--uvloop"]
    )
    assert args.uvloop is True


def test_worker_start_uvloop_default_false() -> None:
    parser = build_parser()
    args = parser.parse_args(
        ["worker", "start", "--agents", "x", "--broker", "memory://"]
    )
    assert args.uvloop is False


# ---------------------------------------------------------------------------
# resolve_uvloop_enabled — flag OR env var
# ---------------------------------------------------------------------------


def test_resolve_returns_false_when_neither_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MURMUR_USE_UVLOOP", raising=False)
    assert resolve_uvloop_enabled(False) is False


def test_resolve_flag_overrides_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MURMUR_USE_UVLOOP", raising=False)
    assert resolve_uvloop_enabled(True) is True


def test_resolve_env_truthy_values(monkeypatch: pytest.MonkeyPatch) -> None:
    for value in ("1", "true", "TRUE", "yes", "YES", "True"):
        monkeypatch.setenv("MURMUR_USE_UVLOOP", value)
        assert resolve_uvloop_enabled(False) is True, f"value={value!r} should enable"


def test_resolve_env_falsy_values(monkeypatch: pytest.MonkeyPatch) -> None:
    for value in ("0", "false", "no", "", "  "):
        monkeypatch.setenv("MURMUR_USE_UVLOOP", value)
        assert resolve_uvloop_enabled(False) is False, (
            f"value={value!r} should not enable"
        )


# ---------------------------------------------------------------------------
# install_uvloop_policy — fail-soft branches
# ---------------------------------------------------------------------------


def test_install_returns_false_when_disabled() -> None:
    assert install_uvloop_policy(enabled=False) is False


def test_install_warns_and_returns_false_on_windows(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sys, "platform", "win32")
    assert install_uvloop_policy(enabled=True) is False
    err = capsys.readouterr().err
    assert "Windows" in err
    assert "uvloop" in err


def test_install_warns_and_returns_false_when_extra_missing(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Simulate uvloop import failing even though the platform allows it."""
    monkeypatch.setattr(sys, "platform", "linux")
    # Block the import without unloading any real module the test process needs.
    monkeypatch.setitem(sys.modules, "uvloop", None)  # type: ignore[arg-type]
    assert install_uvloop_policy(enabled=True) is False
    err = capsys.readouterr().err
    assert "uvloop" in err
    assert "[uvloop]" in err  # mentions the extra to install


# ---------------------------------------------------------------------------
# install_uvloop_policy — happy path (only when uvloop is actually available)
# ---------------------------------------------------------------------------


def test_install_sets_policy_when_available() -> None:
    """When uvloop is on the platform AND installed, the policy is set
    on the running process. We restore the previous policy at the end so
    other tests aren't affected."""
    pytest.importorskip("uvloop")
    if sys.platform.startswith("win"):
        pytest.skip("uvloop has no Windows wheels")
    import uvloop

    saved = asyncio.get_event_loop_policy()
    try:
        installed = install_uvloop_policy(enabled=True)
        assert installed is True
        current = asyncio.get_event_loop_policy()
        assert isinstance(current, uvloop.EventLoopPolicy)
    finally:
        asyncio.set_event_loop_policy(saved)
