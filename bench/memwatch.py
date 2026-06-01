#!/usr/bin/env python3
import argparse
import json
import subprocess
import sys
import threading
import time

try:
    import psutil
except ModuleNotFoundError as exc:
    raise SystemExit("memwatch.py requires psutil. Install it with: python3 -m pip install psutil") from exc


def mib(n: int | float | None) -> float | None:
    if n is None:
        return None
    return round(n / 1024 / 1024, 2)


def collect_tree(proc: psutil.Process):
    procs = [proc]
    try:
        procs += proc.children(recursive=True)
    except psutil.Error:
        pass

    rss = 0
    pss = 0
    uss = 0
    saw_pss = False
    saw_uss = False
    live = 0

    for p in procs:
        try:
            info = p.memory_info()
            rss += getattr(info, "rss", 0)
            live += 1

            try:
                full = p.memory_full_info()
                pss_value = getattr(full, "pss", None)
                uss_value = getattr(full, "uss", None)
                if pss_value is not None:
                    saw_pss = True
                    pss += pss_value
                if uss_value is not None:
                    saw_uss = True
                    uss += uss_value
            except psutil.Error:
                pass
        except psutil.Error:
            continue

    return {
        "rss_mb": mib(rss),
        "pss_mb": mib(pss) if saw_pss else None,
        "uss_mb": mib(uss) if saw_uss else None,
        "process_count": live,
    }


def stream_lines(pipe, sink, echo):
    for line in iter(pipe.readline, ""):
        sink.append(line.rstrip("\n"))
        if echo:
            print(line, end="", file=sys.stderr)
    pipe.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=float, default=0.25)
    parser.add_argument("--out", default="")
    parser.add_argument("--echo-child", action="store_true")
    parser.add_argument("cmd", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    if not args.cmd:
        raise SystemExit("usage: memwatch.py [--out result.json] -- command args...")

    if args.cmd[0] == "--":
        args.cmd = args.cmd[1:]

    start = time.time()
    child = subprocess.Popen(
        args.cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    root = psutil.Process(child.pid)

    stdout_lines: list[str] = []
    stderr_lines: list[str] = []
    stdout_thread = threading.Thread(
        target=stream_lines,
        args=(child.stdout, stdout_lines, args.echo_child),
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=stream_lines,
        args=(child.stderr, stderr_lines, args.echo_child),
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()

    samples = []
    peak = {"rss_mb": 0, "pss_mb": None, "uss_mb": None, "process_count": 0}

    while child.poll() is None:
        try:
            sample = collect_tree(root)
            sample["t"] = round(time.time() - start, 3)
            samples.append(sample)

            peak["rss_mb"] = max(peak["rss_mb"] or 0, sample["rss_mb"] or 0)
            if sample["pss_mb"] is not None:
                peak["pss_mb"] = max(peak["pss_mb"] or 0, sample["pss_mb"])
            if sample["uss_mb"] is not None:
                peak["uss_mb"] = max(peak["uss_mb"] or 0, sample["uss_mb"])
            peak["process_count"] = max(peak["process_count"] or 0, sample["process_count"] or 0)
        except psutil.Error:
            pass

        time.sleep(args.interval)

    stdout_thread.join(timeout=1)
    stderr_thread.join(timeout=1)

    try:
        sample = collect_tree(root)
        sample["t"] = round(time.time() - start, 3)
        samples.append(sample)
        peak["rss_mb"] = max(peak["rss_mb"] or 0, sample["rss_mb"] or 0)
        if sample["pss_mb"] is not None:
            peak["pss_mb"] = max(peak["pss_mb"] or 0, sample["pss_mb"])
        if sample["uss_mb"] is not None:
            peak["uss_mb"] = max(peak["uss_mb"] or 0, sample["uss_mb"])
        peak["process_count"] = max(peak["process_count"] or 0, sample["process_count"] or 0)
    except psutil.Error:
        pass

    result = {
        "cmd": args.cmd,
        "exit_code": child.returncode,
        "duration_s": round(time.time() - start, 3),
        "peak": peak,
        "last": samples[-1] if samples else None,
        "samples": samples,
        "stdout_lines": stdout_lines,
        "stderr_lines": stderr_lines,
    }

    text = json.dumps(result, indent=2)
    if args.out:
        with open(args.out, "w") as f:
            f.write(text)
            f.write("\n")
    else:
        print(text)

    sys.exit(child.returncode)


if __name__ == "__main__":
    main()
