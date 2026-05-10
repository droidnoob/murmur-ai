"""``uvloop`` opt-in helper for CLI commands that own the asyncio loop.

``murmur serve`` and ``murmur worker start`` create the event loop
themselves via ``asyncio.run``. When the operator passes ``--uvloop``
(or sets ``MURMUR_USE_UVLOOP=1``), :func:`install_uvloop_policy` is
called once before ``asyncio.run`` so the new loop comes from
:class:`uvloop.EventLoopPolicy` rather than the stdlib default.

Three guardrails (locked in 2026-05-02):

1. **Never auto-enable on ``import murmur``.** Setting the event loop
   policy is process-wide state. Users have a right to control their
   own loop. Auto-swap on import is bad citizenship.
2. **Fail-soft when the extra isn't installed.** Print a clear stderr
   warning and fall back to the default loop. Don't raise.
3. **Fail-soft on Windows.** uvloop has no Windows wheels — detect the
   platform and skip with a warning rather than failing the install.

The user-side equivalent for code that owns its own ``asyncio.run`` is
documented in ``docs/concepts/runtime.md``; this module exists for the
CLI path only.
"""

from __future__ import annotations

import sys


def install_uvloop_policy(enabled: bool) -> bool:
    """Set :class:`uvloop.EventLoopPolicy` if ``enabled`` and the platform allows.

    Must be called BEFORE the loop starts (i.e. before ``asyncio.run``).
    Returns ``True`` when the policy was installed; ``False`` when the
    operator didn't ask, or asked but the extra is unavailable, or the
    platform is unsupported.

    Args:
        enabled: Operator opt-in (``--uvloop`` flag or
            ``MURMUR_USE_UVLOOP=1`` env var). When ``False``, no policy
            change happens and ``False`` is returned silently.

    Caveats logged to stderr (never stdout) when the operator asked
    but can't have it: missing extra → suggests
    ``pip install 'murmur-runtime[uvloop]'``; Windows → falls back silently
    after a one-line warning. Both cases continue with the default loop.
    """
    if not enabled:
        return False
    if sys.platform.startswith("win"):
        sys.stderr.write(
            "[uvloop] requested but uvloop has no Windows wheels; "
            "falling back to default loop.\n"
        )
        return False
    try:
        import uvloop
    except ImportError:
        sys.stderr.write(
            "[uvloop] requested but uvloop not installed; "
            "falling back to default loop. "
            "Install: pip install 'murmur-runtime[uvloop]'\n"
        )
        return False
    import asyncio

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    return True


def resolve_uvloop_enabled(flag_value: bool) -> bool:
    """Combine the ``--uvloop`` flag with the ``MURMUR_USE_UVLOOP`` env var.

    Either one is sufficient. The env var path lets ops enable uvloop
    fleet-wide via systemd / k8s deployment manifests without needing
    every CLI invocation to carry ``--uvloop``.
    """
    import os

    if flag_value:
        return True
    return os.environ.get("MURMUR_USE_UVLOOP", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }


__all__ = ["install_uvloop_policy", "resolve_uvloop_enabled"]
