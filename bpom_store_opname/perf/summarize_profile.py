"""Ringkas keluaran `py-spy record -f raw --threads --gil`: siapa yang memegang GIL server (hambatan satu inti) dan fungsi apa saja.

    python perf/summarize_profile.py <profile.raw> [top_n]

Format baris raw: "thread;frame;frame;... jumlah_sampel", frame = "fungsi (berkas:baris)". Persentase = porsi sampel
yang memegang GIL (bukan waktu dinding), jadi jumlahnya menggambarkan ke mana CPU bahasa Python terpakai.
"""
from __future__ import annotations

import re
import sys
from collections import Counter
from pathlib import Path

_LINE = re.compile(r":\d+\)$")


def parse(path: Path):
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        stack, _, count = line.rpartition(" ")
        if not stack or not count.isdigit():
            continue
        frames = [_LINE.sub(")", f.strip()) for f in stack.split(";")]
        yield frames, int(count)


def summarize(path: Path, top_n: int = 15) -> dict:
    total = 0
    threads: Counter = Counter()
    inclusive: Counter = Counter()
    leaves: Counter = Counter()
    for frames, n in parse(path):
        total += n
        threads[frames[0]] += n
        leaves[frames[-1]] += n
        for f in set(frames[1:]):
            inclusive[f] += n
    def pct(x):
        return round(100 * x / total, 1) if total else 0.0
    return {
        "samples_with_gil": total,
        "threads": [(name, pct(n)) for name, n in threads.most_common(6)],
        "top_inclusive": [(name, pct(n)) for name, n in inclusive.most_common(top_n)],
        "top_leaf": [(name, pct(n)) for name, n in leaves.most_common(top_n)],
    }


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    result = summarize(Path(sys.argv[1]), int(sys.argv[2]) if len(sys.argv) > 2 else 15)
    print(f"Sampel yang memegang GIL: {result['samples_with_gil']}")
    for title, key in (("Thread", "threads"), ("Fungsi (inklusif, termasuk yang dipanggilnya)", "top_inclusive"), ("Fungsi daun (tempat CPU benar-benar habis)", "top_leaf")):
        print(f"\n{title}:")
        for name, p in result[key]:
            print(f"  {p:5.1f}%  {name[:150]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
