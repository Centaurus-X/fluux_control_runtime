# -*- coding: utf-8 -*-

import argparse
import json
import os
import sys
import time


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Validate product runtime health snapshot.")
    parser.add_argument("--health-file", required=True)
    parser.add_argument("--expected-version", default="v35.1")
    parser.add_argument("--max-age-s", type=float, default=180.0)
    parser.add_argument("--allow-shutdown", action="store_true")
    return parser.parse_args(argv)


def load_json(path):
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def fail(message):
    print("runtime_healthcheck: FAIL:", message, file=sys.stderr)
    return 1


def main(argv=None):
    args = parse_args(argv)
    if not os.path.isfile(args.health_file):
        return fail("health file not found: %s" % args.health_file)
    try:
        data = load_json(args.health_file)
    except Exception as exc:
        return fail("cannot read health file: %s" % exc)

    version = str(data.get("runtime_health_version") or "")
    if version != str(args.expected_version):
        return fail("unexpected health version: %s" % version)

    status = str(data.get("status") or "")
    allowed_statuses = {"running"}
    if args.allow_shutdown:
        allowed_statuses.update({"shutdown_started", "shutdown_complete"})
    if status not in allowed_statuses:
        return fail("unexpected runtime status: %s" % status)

    created_ts = float(data.get("created_ts") or 0.0)
    age_s = time.time() - created_ts
    if created_ts <= 0.0 or age_s > float(args.max_age_s):
        return fail("health snapshot age is too high: %.3fs" % age_s)

    postgres = data.get("external_dependencies", {}).get("postgresql", {})
    if postgres.get("worker_local_dependency") is not False:
        return fail("postgresql must not be reported as local worker dependency")

    print("runtime_healthcheck: OK version=%s status=%s age_s=%.3f" % (version, status, age_s))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
