import argparse
import csv
import re
import subprocess
import sys
import time
from pathlib import Path

QUEUE_NAME = "dota_inference_queue"


def run_cmd(cmd):
    return subprocess.run(cmd, text=True, capture_output=True)


def docker(*args):
    return run_cmd(["docker", *args])


def parse_mem(value: str) -> float:
    if not value:
        return 0.0

    first = value.strip().split()[0]
    match = re.match(r"([0-9.]+)\s*([KMGT]?i?B)", first, re.IGNORECASE)
    if not match:
        return 0.0

    num = float(match.group(1))
    unit = match.group(2).upper()

    factors = {
        "B": 1.0 / 1024.0 / 1024.0,
        "KB": 1.0 / 1024.0,
        "KIB": 1.0 / 1024.0,
        "MB": 1.0,
        "MIB": 1.0,
        "GB": 1024.0,
        "GIB": 1024.0,
        "TB": 1024.0 * 1024.0,
        "TIB": 1024.0 * 1024.0,
    }

    return num * factors.get(unit, 1.0)


def get_queue_stats():
    result = docker(
        "compose",
        "exec",
        "-T",
        "rabbitmq",
        "rabbitmqctl",
        "list_queues",
        "name",
        "messages",
        "messages_ready",
        "messages_unacknowledged",
        "consumers",
    )

    if result.returncode != 0:
        return None

    for line in result.stdout.splitlines():
        parts = line.split()
        if not parts:
            continue

        if parts[0] == QUEUE_NAME and len(parts) >= 5:
            try:
                total = int(parts[1])
                ready = int(parts[2])
                unacked = int(parts[3])
                return total, ready, unacked
            except ValueError:
                return None

    return 0, 0, 0


def get_memory_stats():
    result = docker("stats", "--no-stream", "--format", "{{.Name}}\t{{.MemUsage}}")

    rabbit_mb = 0.0
    worker_mb = 0.0
    producer_mb = 0.0

    if result.returncode != 0:
        return rabbit_mb, worker_mb, producer_mb

    for line in result.stdout.splitlines():
        if "\t" not in line:
            continue

        name, mem_usage = line.split("\t", 1)
        mb = parse_mem(mem_usage)
        lower_name = name.lower()

        if "rabbit" in lower_name:
            rabbit_mb += mb
        elif "worker" in lower_name:
            worker_mb += mb
        elif "producer" in lower_name:
            producer_mb += mb

    return round(rabbit_mb, 2), round(worker_mb, 2), round(producer_mb, 2)


def write_csv(path: Path, rows):
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "t",
                "depth",
                "ready",
                "unacked",
                "rabbit_mb",
                "worker_mb",
                "producer_mb",
            ]
        )

        for row in rows:
            writer.writerow(
                [
                    row["t"],
                    row["depth"],
                    row["ready"],
                    row["unacked"],
                    row["rabbit_mb"],
                    row["worker_mb"],
                    row["producer_mb"],
                ]
            )


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", required=True)
    parser.add_argument("--fast", type=float, default=1.0)
    parser.add_argument("--slow", type=float, default=5.0)
    parser.add_argument("--tail", type=int, default=3)
    parser.add_argument("--cap", type=float, default=30.0)
    parser.add_argument("--drain", action="store_true")
    args = parser.parse_args()

    results_dir = Path("results")
    results_dir.mkdir(exist_ok=True)

    csv_path = results_dir / f"{args.tag}.csv"
    final_path = results_dir / f"{args.tag}.final"
    drain_path = results_dir / f"{args.tag}.drain"

    rows = []
    start = time.monotonic()
    interval = args.fast
    last_stats = (0, 0, 0)
    drain_s = None
    seen_positive = False

    while True:
        elapsed = time.monotonic() - start
        if elapsed >= args.cap:
            break

        time.sleep(interval)

        stats = get_queue_stats()
        if stats is None:
            stats = last_stats
        else:
            last_stats = stats

        total, ready, unacked = stats
        rabbit_mb, worker_mb, producer_mb = get_memory_stats()

        elapsed = time.monotonic() - start

        rows.append(
            {
                "t": round(elapsed, 3),
                "depth": total,
                "ready": ready,
                "unacked": unacked,
                "rabbit_mb": rabbit_mb,
                "worker_mb": worker_mb,
                "producer_mb": producer_mb,
            }
        )

        if args.drain:
            if total > 0:
                seen_positive = True
            if seen_positive and total == 0:
                drain_s = elapsed
                break

        interval = args.fast if len(rows) < args.tail else args.slow

    final_stats = get_queue_stats() or last_stats
    final_total, _final_ready, _final_unacked = final_stats

    write_csv(csv_path, rows)
    final_path.write_text(str(final_total), encoding="ascii")

    if drain_s is not None:
        drain_path.write_text(str(drain_s), encoding="ascii")
    else:
        drain_path.write_text("", encoding="ascii")

    depths = [row["depth"] for row in rows]
    readies = [row["ready"] for row in rows]
    unackeds = [row["unacked"] for row in rows]
    rabbits = [row["rabbit_mb"] for row in rows]
    workers = [row["worker_mb"] for row in rows]
    producers = [row["producer_mb"] for row in rows]

    duration = rows[-1]["t"] if rows else 0.0
    depth_peak = max(depths) if depths else 0
    ready_peak = max(readies) if readies else 0
    unacked_peak = max(unackeds) if unackeds else 0
    depth_mean = round(sum(depths) / len(depths), 2) if depths else 0.0

    rabbit_peak = round(max(rabbits), 2) if rabbits else 0.0
    worker_peak = round(max(workers), 2) if workers else 0.0
    producer_peak = round(max(producers), 2) if producers else 0.0
    total_peak = round(rabbit_peak + worker_peak + producer_peak, 2)

    print(f"[{args.tag}] samples={len(rows)} duration={duration}s")
    print(
        f"[{args.tag}] depth total peak={depth_peak} ready peak={ready_peak} "
        f"unacked peak={unacked_peak} mean={depth_mean} final={final_total}"
    )
    print(
        f"[{args.tag}] mem peak rabbit={rabbit_peak}MB worker={worker_peak}MB "
        f"producer={producer_peak}MB total={total_peak}MB"
    )
    print(f"[{args.tag}] drain={drain_s if drain_s is not None else 'not_reached'}")
    print(f"[{args.tag}] written {csv_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
