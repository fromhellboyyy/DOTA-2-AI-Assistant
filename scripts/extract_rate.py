import argparse
import csv
import json
import re
from pathlib import Path

PUB_RE = re.compile(r"published=(\d+)")
NACK_RE = re.compile(r"nacks=(\d+)")


def _from_json_line(line: str) -> tuple[int | None, int | None]:
    start = line.find("{")
    if start < 0:
        return None, None
    try:
        record = json.loads(line[start:])
    except ValueError:
        return None, None
    if not isinstance(record, dict):
        return None, None
    published = record.get("published")
    nacks = record.get("nacks")
    return (
        int(published) if isinstance(published, int) else None,
        int(nacks) if isinstance(nacks, int) else None,
    )


def counters(log_path: Path) -> tuple[int, int]:
    published = 0
    nacks = 0
    for line in log_path.read_text(encoding="utf-8", errors="replace").splitlines():
        pub, nack = _from_json_line(line)
        if pub is None:
            pm = PUB_RE.search(line)
            pub = int(pm.group(1)) if pm else None
        if nack is None:
            nm = NACK_RE.search(line)
            nack = int(nm.group(1)) if nm else None
        if pub is not None:
            published = pub
        if nack is not None:
            nacks = nack
    return published, nacks


def wall_from_csv(tag: str) -> float:
    path = Path("results") / f"{tag}.csv"
    if not path.exists():
        return 0.0
    last = 0.0
    with path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            last = float(row["t"])
    return last


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--tag", required=True)
    p.add_argument("--logs-dir", default=".")
    p.add_argument("--out-dir", default="results")
    args = p.parse_args()
    log_path = Path(args.logs_dir) / f"logs_{args.tag}.txt"
    if not log_path.exists():
        print(f"[{args.tag}] missing {log_path}")
        return
    published, nacks = counters(log_path)
    wall = round(wall_from_csv(args.tag), 1)
    rate = round(published / wall, 2) if wall > 0 else 0.0
    data = {
        "published": published,
        "nacks": nacks,
        "wall_s": wall,
        "rate_per_s": rate,
    }
    out_dir = Path(args.out_dir)
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / f"{args.tag}_rate.json"
    out_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(
        f"[{args.tag}] published={published} nacks={nacks} wall={wall}s rate={rate}/s -> {out_path}"
    )


if __name__ == "__main__":
    main()
