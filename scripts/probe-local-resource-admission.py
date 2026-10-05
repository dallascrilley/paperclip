#!/usr/bin/env python3
"""Read-only LocalModelGuard admission probe. Exit 75 means defer, never launch."""
import argparse
import fcntl
import json
import math
import os
import stat
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def decision(reason, memory=None):
    result = {"admitted": reason == "ready", "reason": reason}
    if memory is not None:
        result["memory"] = memory
    return result


def probe(guard_url, job_lock, required_gib, timeout):
    try:
        url = urllib.parse.urlsplit(guard_url)
        if (url.scheme != "http" or url.hostname not in ("127.0.0.1", "::1")
                or url.username or url.password or url.query or url.fragment
                or url.path not in ("", "/", "/health")):
            return decision("invalid_guard_target")
        endpoint = guard_url.rstrip("/")
        if not endpoint.endswith("/health"):
            endpoint += "/health"
        fd = os.open(job_lock, os.O_RDWR | os.O_NOFOLLOW)
    except (OSError, ValueError):
        return decision("job_gate_unavailable")
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
            return decision("job_gate_unavailable")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return decision("heavy_job_busy")
        except OSError:
            return decision("job_gate_unavailable")
        # Preserve the existing inode. Hold its gate through the fresh health read.
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
        try:
            with opener.open(endpoint, timeout=timeout) as response:
                body = response.read(262145)
                if len(body) > 262144:
                    return decision("guard_response_too_large")
                state = json.loads(body)
        except (OSError, ValueError, urllib.error.URLError):
            return decision("guard_unavailable")
        if not isinstance(state, dict) or not isinstance(state.get("memory"), dict):
            return decision("invalid_guard_response")
        memory = state["memory"]
        available, pressure = memory.get("available_gib"), memory.get("pressure_level")
        if (type(available) not in (int, float) or not math.isfinite(available)
                or available < 0 or type(pressure) is not int or pressure not in (1, 2, 4)):
            return decision("invalid_guard_response")
        if state.get("status") != "ready" or state.get("busy") is not False or "active_model" not in state:
            return decision("guard_not_ready")
        if state["active_model"] is not None:
            return decision("model_active")
        snapshot = {"available_gib": available, "pressure_level": pressure}
        if pressure != 1:
            return decision("memory_pressure", snapshot)
        if available < required_gib:
            return decision("insufficient_headroom", snapshot)
        return decision("ready", snapshot)
    finally:
        # Closing releases this process's acquisition. Never unlink a shared lock.
        os.close(fd)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guard-url", default="http://127.0.0.1:11437")
    parser.add_argument("--job-lock", default=str(Path.home() / "Library/Application Support/LocalModelGuard/heavy-job.lock"))
    parser.add_argument("--required-gib", type=float, default=4.0)
    parser.add_argument("--timeout", type=float, default=2.0)
    args = parser.parse_args()
    if (not math.isfinite(args.required_gib) or args.required_gib <= 0
            or not math.isfinite(args.timeout) or not 0 < args.timeout <= 5):
        parser.error("Positive finite headroom and a timeout of at most five seconds are required")
    result = probe(args.guard_url, args.job_lock, args.required_gib, args.timeout)
    print(json.dumps(result, allow_nan=False))
    return 0 if result["admitted"] else 75


if __name__ == "__main__":
    sys.exit(main())
