import re
import json
import time
import random
import logging
from time import sleep
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def get_iran_hosts(max_retries=3, timeout=5):
    retry_delay = 2
    for attempt in range(max_retries):
        try:
            r = requests.get("https://check-host.net/nodes/hosts", timeout=timeout)
            r.raise_for_status()
            data = r.json()
            iran = [h for h, n in data.get("nodes", {}).items()
                    if "Iran" in n.get("location", [])]
            logger.info(f"Found {len(iran)} Iranian nodes: {iran}")
            return iran
        except requests.exceptions.RequestException as e:
            logger.warning(f"get_iran_hosts attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                sleep(retry_delay)
    logger.error("get_iran_hosts failed after retries")
    return []


def extract_host_from_line(line: str):
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    m = re.match(r"^(?:hysteria2?|hy2)://(?:[^@/]*@)?", line, re.IGNORECASE)
    if not m:
        return None
    rest = line[m.end():]
    host_match = re.match(r"^(\[[0-9a-fA-F:]+\]|[^:/?#]+)", rest)
    if not host_match:
        return None
    host = host_match.group(1).strip("[]")
    return host if host else None


def check_host_submit(host, iran_nodes, max_retries=6):
    url = "https://check-host.net/check-ping"
    params = {"host": host, "node": iran_nodes}
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params,
                             headers={"Accept": "application/json"}, timeout=10)
            if r.status_code == 429:
                wait = (2 ** attempt) + random.uniform(0.5, 2.0)
                logger.warning(
                    f"429 submitting {host} (attempt {attempt + 1}/{max_retries}), "
                    f"sleeping {wait:.1f}s"
                )
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json().get("request_id")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code != 429:
                logger.error(f"HTTP error for {host}: {e}")
                return None
        except Exception as e:
            logger.error(f"submit error for {host}: {e}")
            return None
    logger.error(f"submit failed for {host} after {max_retries} retries (429)")
    return None


def check_ping_result(request_id, max_attempts=12, poll_interval=1.5):
    if not request_id:
        return {}
    url = f"https://check-host.net/check-result/{request_id}"
    for attempt in range(max_attempts):
        try:
            r = requests.get(url, headers={"Accept": "application/json"}, timeout=10)
            if r.status_code == 429:
                wait = (2 ** attempt) + random.uniform(0.5, 2.0)
                logger.warning(
                    f"429 polling {request_id} (attempt {attempt + 1}/{max_attempts}), "
                    f"sleeping {wait:.1f}s"
                )
                time.sleep(wait)
                continue
            r.raise_for_status()
            data = r.json()
            if any(data.get(n) for n in data if data.get(n)):
                pings = {}
                for node, d in data.items():
                    if not d:
                        continue
                    cnt, s = 0, 0
                    if d[0] is not None:
                        for v in d[0]:
                            if v and len(v) > 1 and v[0] == "OK":
                                cnt += 1
                                s += v[1]
                        if cnt:
                            pings[node] = (s / cnt) * 1000
                return pings
            time.sleep(poll_interval)
        except Exception as e:
            logger.error(f"poll error {request_id}: {e}")
            return {}
    return {}


def load_progress(progress_path: Path):
    """Load already-processed records from JSONL (for resume)."""
    done = {}
    if not progress_path.is_file():
        return done
    with progress_path.open("r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            try:
                rec = json.loads(ln)
                done[rec["host"]] = rec
            except Exception:
                continue
    return done


def append_progress(progress_path: Path, rec: dict):
    with progress_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def main(input_file="results/hysteria2.txt",
         output_file="results/working_hysteria_servers.txt",
         min_iran_ok_ratio=0.5,
         submit_delay=4.0,
         submit_retry_max=6,
         poll_workers=10,
         resume=True):
    input_path = Path(input_file)
    output_path = Path(output_file)

    if not input_path.is_file():
        logger.error(f"Input file not found: {input_path.resolve()}")
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    progress_path = output_path.parent / (output_path.name + ".progress.jsonl")

    lines = [ln for ln in input_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    logger.info(f"Read {len(lines)} lines")

    iran_nodes = get_iran_hosts()
    if not iran_nodes:
        logger.error("No Iranian nodes. Exiting.")
        return

    # Parse + dedup
    parsed = []
    seen = set()
    for idx, line in enumerate(lines):
        host = extract_host_from_line(line)
        if not host:
            logger.warning(f"[{idx}] parse failed: {line[:60]}...")
            continue
        if host in seen:
            logger.info(f"[{idx}] duplicate host skipped: {host}")
            continue
        seen.add(host)
        parsed.append((idx, line, host))

    logger.info(f"Parsed {len(parsed)} unique valid hosts (from {len(lines)} lines)")

    # Load prior progress (resume)
    prior = load_progress(progress_path) if resume else {}
    if prior:
        logger.info(f"Resume: found {len(prior)} previously completed hosts")
    pending = [(idx, line, host) for idx, line, host in parsed if host not in prior]
    logger.info(f"Pending hosts to process: {len(pending)}")

    # Stage 1: sequential submit
    logger.info(
        f"Stage 1: Submitting {len(pending)} requests sequentially "
        f"(delay={submit_delay}s, retries={submit_retry_max}) ..."
    )
    submitted = []
    for i, (idx, line, host) in enumerate(pending, 1):
        logger.info(f"  [{i}/{len(pending)}] Submitting {host} ...")
        req_id = check_host_submit(host, iran_nodes, max_retries=submit_retry_max)
        if req_id:
            submitted.append((idx, line, host, req_id))
            logger.info(f"    -> request_id={req_id}")
        else:
            logger.warning(f"    -> failed to submit")
            rec = {
                "idx": idx, "line": line, "host": host, "ok": False,
                "pings": {}, "reason": "submit_failed"
            }
            append_progress(progress_path, rec)
            prior[host] = rec
        time.sleep(submit_delay)

    logger.info(f"Stage 1 done: {len(submitted)}/{len(pending)} submitted")
    if not submitted:
        logger.info("Nothing new submitted. Building output from existing progress.")
    else:
        # Stage 2: parallel poll
        logger.info(
            f"Stage 2: Collecting {len(submitted)} results "
            f"with {poll_workers} workers ..."
        )

        def _collect(job):
            idx, line, host, req_id = job
            pings = check_ping_result(req_id)
            rec = {
                "idx": idx, "line": line, "host": host,
                "ok": bool(pings),
                "pings": {k: round(v, 2) for k, v in pings.items()},
            }
            append_progress(progress_path, rec)
            return rec

        with ThreadPoolExecutor(max_workers=poll_workers) as pool:
            futures = [pool.submit(_collect, job) for job in submitted]
            for fut in as_completed(futures):
                try:
                    r = fut.result()
                    prior[r["host"]] = r
                    logger.info(
                        f"  [{r['idx']}] {r['host']} -> "
                        f"{'OK' if r['ok'] else 'FAIL'} "
                        f"({len(r['pings'])} nodes)"
                    )
                except Exception as e:
                    logger.error(f"collect error: {e}")

    # Aggregate
    results = sorted(prior.values(), key=lambda x: x["idx"])
    total = len(results)
    working = [r for r in results if r.get("ok")]
    ratio = len(working) / total if total else 0
    logger.info(
        f"Result: {len(working)}/{total} servers responded "
        f"(ratio={ratio:.0%}, threshold={min_iran_ok_ratio:.0%})"
    )

    if ratio < min_iran_ok_ratio:
        logger.error(
            f"Threshold not met ({ratio:.0%} < {min_iran_ok_ratio:.0%}). "
            f"No file saved."
        )
        return

    with output_path.open("w", encoding="utf-8") as f:
        for r in working:
            f.write(r["line"] + "\n")
    logger.info(f"Saved {len(working)} servers to '{output_path}'")

    report_path = output_path.parent / (output_path.name + ".report.txt")
    with report_path.open("w", encoding="utf-8") as f:
        for r in results:
            status = "OK  " if r.get("ok") else "FAIL"
            pings = r.get("pings") or {}
            if pings:
                vals = list(pings.values())
                avg = sum(vals) / len(vals)
                best = min(vals)
                f.write(
                    f"[{status}] {r['host']:45s} "
                    f"nodes={len(pings)} avg={avg:7.1f}ms best={best:7.1f}ms\n"
                )
            else:
                reason = r.get("reason", "no_response")
                f.write(f"[{status}] {r['host']:45s} {reason}\n")
    logger.info(f"Report written to '{report_path}'")
    logger.info(f"Progress log: '{progress_path}'")


if __name__ == "__main__":
    main(
        input_file="results/hysteria2.txt",
        output_file="results/working_hysteria_servers.txt",
        min_iran_ok_ratio=0.5,
        submit_delay=4.0,
        submit_retry_max=6,
        poll_workers=10,
        resume=True,
    )