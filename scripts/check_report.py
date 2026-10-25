import sys
from pathlib import Path


def main():
    target = sys.argv[1] if len(sys.argv) > 1 else "results/section7.md"
    p = Path(target)

    if not p.exists():
        print("MISSING", target)
        return 1

    s = p.read_text(encoding="utf-8")

    mojibake = any(0x80 <= ord(c) <= 0xFF for c in s)
    braces = ("{" in s) or ("}" in s)

    typo_a = "\u043a\u043e\u044d\u0444\u0444\u0438\u0446\u0435\u043d\u0442" in s
    typo_b = "\u043a\u043e\u0435\u0444\u0444\u0438\u0446\u0435\u043d\u0442" in s

    ok = (not mojibake) and (not braces) and (not typo_a) and (not typo_b)

    print("file", target)
    print("chars", len(s))
    print("mojibake", mojibake)
    print("unresolved_braces", braces)
    print("typo_koefficient", typo_a or typo_b)
    print("verdict", "OK" if ok else "FAIL")

    return 0 if ok else 2


if __name__ == "__main__":
    sys.exit(main())
