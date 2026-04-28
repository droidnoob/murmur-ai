#!/usr/bin/env python3
"""Generate a murmuration as ASCII art — shaped like a starling in flight.

The iconic emergent silhouette: a flock of starlings forming the silhouette
of a single starling. Density is a sum of Gaussian blobs laid out as body,
head, swept-back wings, and trailing tail; each render samples that field
through noise, so individual "birds" are scattered with the right
distribution.

Usage:
    uv run python scripts/murmuration.py
    uv run python scripts/murmuration.py --seed 7 --width 96 --height 28
    uv run python scripts/murmuration.py --scan 8        # browse seeds
"""

from __future__ import annotations

import argparse
import math
import random

# Sparse → dense character ramp. ``v`` reads as a tiny bird in profile in the
# mid-densities, which sells the "starling" feel inside the silhouette.
CHARS = " .·'\",:vV"


def density(x: float, y: float) -> float:
    """Density field for a starling silhouette in flight, facing right.

    ``x`` and ``y`` are roughly in ``[-1, 1]``. The bird's nose-to-tail axis
    runs along ``x``; wings extend along ``y``. The whole shape is a max of
    Gaussian blobs — body, head, two swept wings, and tail.
    """
    # ---- body: long horizontal ellipse ----
    body = math.exp(-(((x - 0.00) / 0.32) ** 2) - (((y - 0.00) / 0.075) ** 2))

    # ---- head: round, slightly forward of body, tucked tight ----
    head = math.exp(-(((x - 0.36) / 0.11) ** 2) - (((y - 0.00) / 0.085) ** 2))

    # ---- beak: small forward extension ----
    beak = 0.55 * math.exp(-(((x - 0.50) / 0.045) ** 2) - (((y - 0.00) / 0.030) ** 2))

    # ---- wings: sample along a swept-back arc on each side ----
    # Sweep increases with t: wing tip is up-and-back from the shoulder.
    wings = 0.0
    n_wing = 22
    for sign in (-1, 1):
        for k in range(n_wing):
            t = (k + 0.5) / n_wing  # 0 at shoulder → 1 at tip
            wx = -0.04 - 0.40 * (t**1.15)  # sweep back as we go out
            wy = sign * (0.07 + 0.62 * t)  # extend outward
            # Blob shrinks toward the tip; wing is fairly thin.
            ws = 0.115 * (1.0 - 0.55 * t)
            amp = 0.95 * (1.0 - 0.40 * t)
            r2 = (x - wx) ** 2 + (y - wy) ** 2
            blob = amp * math.exp(-r2 / (ws**2))
            if blob > wings:
                wings = blob

    # ---- tail: triangular fan trailing behind the body ----
    tail = 0.0
    n_tail = 8
    for k in range(n_tail):
        t = (k + 0.5) / n_tail
        tx = -0.32 - 0.18 * t
        # Two prongs spread vertically as we go back — gives the forked look.
        for sign in (-1, 1):
            ty = sign * 0.03 * t
            ta = 0.12 * (1.0 - 0.55 * t)
            tb = 0.045 + 0.05 * t
            amp = 0.85 * (1.0 - 0.30 * t)
            blob = amp * math.exp(-(((x - tx) / ta) ** 2) - (((y - ty) / tb) ** 2))
            if blob > tail:
                tail = blob

    return min(1.0, body + head + beak + wings + tail)


def render(width: int = 90, height: int = 26, seed: int = 42) -> str:
    """Return an ASCII murmuration of the given size."""
    rng = random.Random(seed)
    lines: list[str] = []

    # Map the full bird (wings extend a fair distance vertically) into the
    # canvas. Cells are ~2× taller than wide; the ``y`` scaling below leaves
    # room for both wings without squashing.
    x_span = 1.05  # half-width of canvas in normalized coords
    y_span = 0.90  # half-height in normalized coords

    for j in range(height):
        row: list[str] = []
        for i in range(width):
            x = ((i / (width - 1)) * 2 - 1) * x_span
            y = ((j / (height - 1)) * 2 - 1) * y_span

            d = density(x, y)

            # Hard cutoff for empty regions — keeps the void clean instead of
            # peppered with stray noise.
            if d < 0.07:
                row.append(" ")
                continue

            # Probabilistic placement: denser regions get more birds.
            if rng.random() > d * 1.10:
                row.append(" ")
                continue

            # Pick a character by density with a small jitter so equal-density
            # bands aren't visibly striped.
            idx = int(d * (len(CHARS) - 1) + rng.uniform(0.0, 0.9))
            idx = max(1, min(len(CHARS) - 1, idx))
            row.append(CHARS[idx])

        lines.append("".join(row).rstrip())

    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()

    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--width", type=int, default=90)
    ap.add_argument("--height", type=int, default=26)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument(
        "--scan",
        type=int,
        default=0,
        metavar="N",
        help="print N seeds in a row, separated by blank lines",
    )
    args = ap.parse_args()

    if args.scan:
        for s in range(args.seed, args.seed + args.scan):
            print(f"--- seed={s} ---")
            print(render(args.width, args.height, s))
            print()
    else:
        print(render(args.width, args.height, args.seed))


if __name__ == "__main__":
    main()
