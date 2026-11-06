import re
import math
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


def compute_min_nodes(min_iran_ok_ratio: float, total_iran_nodes: int) -> int:
    """
    Convert ratio -> required minimum number of responding Iranian nodes.
      ratio >= 1.0  -> all nodes
      ratio <= 0    -> 1 node (any response)
      otherwise     -> ceil(ratio * total_nodes), at least 1
    """
    if min_iran_ok_ratio >= 1.0:
        return total_iran_nodes
    if min_iran_ok_ratio <= 0.0:
        return 1
    required = math.ceil(min_iran_ok_ratio * total_iran_nodes)
    return max(1, min(required, total_iran_nodes))


def main(input_file="results/hysteria2.txt",
         output_file="results/working_hysteria_servers.txt",
         min_iran_ok_ratio=1.0,
         submit_delay=4.0,
         submit_retry_max=6,
         poll_workers=10):
    input_path = Path(input_file)
    output_path = Path(output_file)

    if not input_path.is_file():
        logger.error(f"Input file not found: {input_path.resolve()}")
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)

    lines = [ln for ln in input_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    logger.info(f"Read {len(lines)} lines")

    iran_nodes = get_iran_hosts()
    if not iran_nodes:
        logger.error("No Iranian nodes. Exiting.")
        return

    total_iran_nodes = len(iran_nodes)
    min_required = compute_min_nodes(min_iran_ok_ratio, total_iran_nodes)
    logger.info(
        f"Policy: min_iran_ok_ratio={min_iran_ok_ratio} -> "
        f"require at least {min_required}/{total_iran_nodes} Iranian nodes "
        f"to respond per host."
    )

    # Group all lines by host (keep every config line for output)
    host_to_lines = {}
    host_order = []
    for idx, line in enumerate(lines):
        host = extract_host_from_line(line)
        if not host:
            logger.warning(f"[{idx}] parse failed: {line[:60]}...")
            continue
        if host not in host_to_lines:
            host_to_lines[host] = []
            host_order.append(host)
        host_to_lines[host].append(line)

    unique_hosts = host_order
    logger.info(
        f"Parsed {len(unique_hosts)} unique hosts "
        f"covering {sum(len(v) for v in host_to_lines.values())} config lines "
        f"(from {len(lines)} input lines)"
    )

    # Stage 1: submit (one per unique host)
    logger.info(
        f"Stage 1: Submitting {len(unique_hosts)} unique hosts sequentially "
        f"(delay={submit_delay}s, retries={submit_retry_max}) ..."
    )
    submitted = []
    for i, host in enumerate(unique_hosts, 1):
        logger.info(f"  [{i}/{len(unique_hosts)}] Submitting {host} ...")
        req_id = check_host_submit(host, iran_nodes, max_retries=submit_retry_max)
        if req_id:
            submitted.append((host, req_id))
            logger.info(f"    -> request_id={req_id}")
        else:
            logger.warning(f"    -> failed to submit")
        time.sleep(submit_delay)

    logger.info(f"Stage 1 done: {len(submitted)}/{len(unique_hosts)} submitted")
    if not submitted:
        logger.error("Nothing submitted. Exiting.")
        return

    # Stage 2: poll
    logger.info(
        f"Stage 2: Collecting {len(submitted)} results "
        f"with {poll_workers} workers ..."
    )

    def _collect(job):
        host, req_id = job
        pings = check_ping_result(req_id)
        return {"host": host, "pings": pings}

    host_results = {}
    with ThreadPoolExecutor(max_workers=poll_workers) as pool:
        futures = [pool.submit(_collect, job) for job in submitted]
        for fut in as_completed(futures):
            try:
                r = fut.result()
                host_results[r["host"]] = r
                n = len(r["pings"])
                meets = n >= min_required
                logger.info(
                    f"  {r['host']} -> {n}/{total_iran_nodes} nodes "
                    f"({'PASS' if meets else 'FAIL'} vs required {min_required})"
                )
            except Exception as e:
                logger.error(f"collect error: {e}")

    # Select hosts that meet the per-host node requirement
    selected_hosts = [
        h for h, r in host_results.items()
        if len(r["pings"]) >= min_required
    ]
    total = len(host_results)
    logger.info(
        f"Selection: {len(selected_hosts)}/{total} unique hosts "
        f"have at least {min_required}/{total_iran_nodes} nodes responding."
    )

    if not selected_hosts:
        logger.error("No hosts matched the criteria. No file saved.")
        return

    # Expand back: write ALL original lines for selected hosts
    selected_set = set(selected_hosts)
    output_lines = []
    for host in host_order:
        if host in selected_set:
            output_lines.extend(host_to_lines[host])

    with output_path.open("w", encoding="utf-8") as f:
        for ln in output_lines:
            f.write(ln + "\n")

    logger.info(
        f"Saved {len(output_lines)} config lines "
        f"({len(selected_hosts)} unique hosts) to '{output_path}'"
    )


if __name__ == "__main__":
    main(
        input_file="results/hysteria2.txt",
        output_file="results/working_hysteria_servers.txt",
        min_iran_ok_ratio=1.0,   # 1.0 -> all 8 nodes; 0.5 -> at least 4; 0.0 -> at least 1
        submit_delay=4.0,
        submit_retry_max=6,
        poll_workers=10,
    )