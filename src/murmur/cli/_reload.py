"""``--reload`` helper shared by ``murmur serve`` and ``murmur worker start``.

FastStream and FastAPI both lean on `watchfiles
<https://github.com/samuelcolvin/watchfiles>`_ for their reload primitive
(verified live: faststream.ag2.ai/latest/getting-started/cli/ and
uvicorn.dev/settings/). Murmur takes the same approach, but rather than
threading uvicorn-specific reload semantics through ``serve`` and
re-rolling something else for ``worker``, both share one small wrapper:
re-exec the original command in a subprocess, watch the configured paths,
and restart the child when something matching the include set changes.

Why subprocess rather than uvicorn's built-in reload:

- uvicorn's reload requires the app passed as an importable string
  (``"module:attr"``) so it can re-import in a fresh subprocess. Murmur's
  ``AgentServer`` is constructed with runtime objects (registry, broker)
  that aren't easily re-imported by a string reference.
- ``Worker.start()`` is FastStream-driven, not uvicorn — uvicorn's reload
  doesn't apply.
- One mechanism for both subcommands keeps user mental model uniform.

Default include set is ``*.py``, ``*.yaml``, ``*.yml`` so YAML spec edits
trigger a reload. Override via ``--reload-include`` / ``--reload-exclude``.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Callable, Iterable
from fnmatch import fnmatch
from pathlib import Path

# Sentinel env var. The parent (the watcher) sets this to ``1`` before
# spawning the child; the child sees it on entry and skips the reload
# branch — without this, ``--reload`` would fork-bomb itself.
_CHILD_SENTINEL = "_MURMUR_RELOAD_CHILD"

_DEFAULT_INCLUDE: tuple[str, ...] = ("*.py", "*.yaml", "*.yml")


def is_reload_child() -> bool:
    """``True`` when the current process is a child spawned by :func:`reload_wrap`."""
    return os.environ.get(_CHILD_SENTINEL) == "1"


def reload_wrap(
    *,
    reload_dirs: Iterable[Path],
    includes: Iterable[str] | None,
    excludes: Iterable[str] | None,
    debounce_ms: int = 300,
) -> int:
    """Re-exec ``sys.argv`` (minus reload flags) in a subprocess; restart on changes.

    Returns the final exit code of the last child invocation, or ``0``
    on a clean ``KeyboardInterrupt``. Returns ``2`` if ``watchfiles``
    isn't installed (the user needs the ``[reload]`` extra).
    """
    try:
        from watchfiles import watch
    except ImportError:
        print(
            "[error] --reload requires the 'watchfiles' package. "
            "Install with: uv add 'murmur-runtime[reload]'",
            file=sys.stderr,
        )
        return 2

    child_argv = _strip_reload_flags(sys.argv)
    env = os.environ.copy()
    env[_CHILD_SENTINEL] = "1"

    inc = tuple(includes) if includes else _DEFAULT_INCLUDE
    exc = tuple(excludes) if excludes else ()
    flt = _make_filter(inc, exc)

    paths = [str(p) for p in reload_dirs]
    print(
        f"[reload] watching {', '.join(paths)} "
        f"(includes={','.join(inc)}; excludes={','.join(exc) or 'none'})",
        file=sys.stderr,
    )

    proc = subprocess.Popen(child_argv, env=env)
    try:
        for changes in watch(*paths, watch_filter=flt, debounce=debounce_ms):
            if not changes:
                continue
            print(
                f"[reload] {len(changes)} change(s) detected, restarting child...",
                file=sys.stderr,
            )
            _terminate(proc)
            proc = subprocess.Popen(child_argv, env=env)
    except KeyboardInterrupt:
        pass
    finally:
        _terminate(proc)
    return proc.returncode or 0


def _strip_reload_flags(argv: list[str]) -> list[str]:
    """Remove ``--reload`` and ``--reload-*`` from a CLI argv vector.

    Handles both space-separated (``--reload-dir foo``) and equals form
    (``--reload-dir=foo``). The space form means consuming the next
    token, hence ``skip_next``.
    """
    valued_flags = {"--reload-dir", "--reload-include", "--reload-exclude"}
    out: list[str] = []
    skip_next = False
    for arg in argv:
        if skip_next:
            skip_next = False
            continue
        if arg == "--reload":
            continue
        if arg in valued_flags:
            skip_next = True
            continue
        if any(arg.startswith(f"{f}=") for f in valued_flags):
            continue
        out.append(arg)
    return out


def _make_filter(
    includes: tuple[str, ...], excludes: tuple[str, ...]
) -> Callable[[object, str], bool]:
    """Build a ``watch_filter`` callable matching include/exclude globs.

    Excludes win — if a path matches an exclude glob, it's never reloaded
    even if it would otherwise match an include glob.
    """

    def filt(_change_type: object, path: str) -> bool:
        name = Path(path).name
        if any(fnmatch(name, pat) for pat in excludes):
            return False
        return any(fnmatch(name, pat) for pat in includes)

    return filt


def _terminate(proc: subprocess.Popen[bytes]) -> None:
    """Best-effort terminate-then-kill; bounded wall clock."""
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


__all__ = ["is_reload_child", "reload_wrap"]
