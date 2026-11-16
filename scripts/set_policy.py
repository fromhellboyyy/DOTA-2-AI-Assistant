import argparse
import json
import subprocess
import sys

POLICY_NAME = "experiment_limit"
QUEUE_PATTERN = "^dota_inference_queue$"


def rabbit_ctl(*args):
    return subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "rabbitmq",
            "rabbitmqctl",
            *args,
        ],
        text=True,
        capture_output=True,
    )


def build_definition(size: int) -> str:
    if size == 20:
        definition = {
            "max-length": 20,
            "overflow": "reject-publish",
        }
    else:
        definition = {
            "max-length": size,
        }

    return json.dumps(definition, separators=(",", ":"))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", required=True, type=int)
    args = parser.parse_args()

    definition = build_definition(args.size)

    rabbit_ctl("clear_policy", POLICY_NAME)

    result = rabbit_ctl(
        "set_policy",
        "--apply-to",
        "queues",
        "--priority",
        "1",
        POLICY_NAME,
        QUEUE_PATTERN,
        definition,
    )

    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)

    if result.returncode != 0:
        print(
            f"set_policy failed rc={result.returncode} definition={definition}",
            file=sys.stderr,
        )
        return result.returncode or 1

    print(f"policy set definition={definition}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
