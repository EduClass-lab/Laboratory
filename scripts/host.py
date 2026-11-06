import re
import time
import logging
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def get_iran_hosts(max_retries=3, timeout=5):
    retry_delay = 1
    iran_hosts = []
    for attempt in range(max_retries):
        try:
            response = requests.get(
                "https://check-host.net/nodes/hosts",
                timeout=timeout
            )
            response.raise_for_status()
            data = response.json()
            for host, node_data in data.get("nodes", {}).items():
                if "Iran" in node_data.get("location", []):
                    iran_hosts.append(host)
            print("\niran_hosts:", iran_hosts)
            print("\niran_hosts valid servers Count:", len(iran_hosts))
            return iran_hosts
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                sleep(retry_delay)
    logger.error("All retries exhausted. Could not fetch data.")
    return []


def check_ping(request_id, max_attempts=10, poll_interval=1):
    if not request_id:
        return {}
    url = f"https://check-host.net/check-result/{request_id}"
    attempt = 0
    try:
        decoded_response = {}
        while attempt < max_attempts:
            response = requests.get(
                url, headers={"Accept": "application/json"}, timeout=10)
            response.raise_for_status()
            decoded_response = response.json()
            if any(decoded_response.get(node) for node in decoded_response
                   if decoded_response.get(node)):
                break
            time.sleep(poll_interval)
            attempt += 1

        pings = {}
        for node, data in decoded_response.items():
            if not data:
                continue
            count = 0
            ping_sum = 0
            if data and data[0] is not None:
                for value in data[0]:
                    if value and len(value) > 1 and value[0] == "OK":
                        count += 1
                        ping_sum += value[1]
                if count != 0:
                    pings[node] = (ping_sum / count) * 1000
        return pings
    except Exception as e:
        logging.error(f"check_ping failed for request_id {request_id}: {e}")
        return {}


def check_host(host, iran_hosts):
    if not host:
        return None
    url = "https://check-host.net/check-ping"
    params = {'host': host, 'node': iran_hosts}
    try:
        response = requests.get(
            url, params=params,
            headers={"Accept": "application/json"}, timeout=10
        )
        response.raise_for_status()
        return response.json().get("request_id")
    except Exception as e:
        logging.error(f"check_host failed for host {host}: {e}")
        return None


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


def has_ping(pings: dict) -> bool:
    return bool(pings) and any(v is not None for v in pings.values())


def submit_ping_job(idx, line, host, iran_nodes, inter_request_delay):
    if inter_request_delay:
        time.sleep(inter_request_delay)
    req_id = check_host(host, iran_nodes)
    return idx, line, host, req_id


def collect_ping_job(idx, line, host, req_id, poll_interval):
    if not req_id:
        return {"idx": idx, "line": line, "host": host,
                "pings": {}, "ok": False}
    pings = check_ping(req_id, poll_interval=poll_interval)
    return {"idx": idx, "line": line, "host": host,
            "pings": pings, "ok": has_ping(pings)}


def main(input_file="hysteria_servers.txt",
         output_file="working_hysteria_servers.txt",
         min_iran_nodes_ratio=0.5,
         submit_workers=5,
         poll_workers=20,
         inter_request_delay=0.5,
         poll_interval=1):
    with open(input_file, "r", encoding="utf-8") as f:
        lines = [ln for ln in f.read().splitlines() if ln.strip()]
    logger.info(f"Read {len(lines)} lines from input file")

    iran_nodes = get_iran_hosts()
    if not iran_nodes:
        logger.error("No Iranian nodes received. Exiting.")
        return

    submit_results = []
    logger.info(f"Stage 1: Submitting {len(lines)} ping requests with {submit_workers} workers ...")

    with ThreadPoolExecutor(max_workers=submit_workers) as pool:
        futures = {}
        for idx, line in enumerate(lines):
            host = extract_host_from_line(line)
            if not host:
                logger.warning(f"[{idx}] Could not extract host from: {line[:60]}...")
                continue
            futures[pool.submit(
                submit_ping_job, idx, line, host, iran_nodes, inter_request_delay
            )] = idx

        for fut in as_completed(futures):
            try:
                submit_results.append(fut.result())
            except Exception as e:
                logger.error(f"Submit job error: {e}")

    logger.info(f"Stage 1 complete: received {len(submit_results)} request_id(s)")

    logger.info(f"Stage 2: Collecting results with {poll_workers} workers ...")
    results = []
    with ThreadPoolExecutor(max_workers=poll_workers) as pool:
        futures = {
            pool.submit(collect_ping_job, idx, line, host, req_id, poll_interval): idx
            for idx, line, host, req_id in submit_results
        }
        for fut in as_completed(futures):
            try:
                r = fut.result()
                results.append(r)
                logger.info(
                    f"  [{r['idx']}] {r['host']} -> "
                    f"{'OK' if r['ok'] else 'FAIL'} "
                    f"(nodes={len(r['pings'])})"
                )
            except Exception as e:
                logger.error(f"Poll job error: {e}")

    results.sort(key=lambda r: r["idx"])

    total = len(results)
    working = [r for r in results if r["ok"]]
    success_ratio = len(working) / total if total else 0
    logger.info(
        f"Result: {len(working)}/{total} servers responded to ping "
        f"(ratio: {success_ratio:.0%})"
    )

    if success_ratio < min_iran_nodes_ratio:
        logger.error(
            f"Condition not met: ratio {success_ratio:.0%} < "
            f"{min_iran_nodes_ratio:.0%}. No file saved."
        )
        return

    with open(output_file, "w", encoding="utf-8") as f:
        for r in working:
            f.write(r["line"] + "\n")
    logger.info(f"Saved {len(working)} working servers to '{output_file}'.")

    with open(output_file + ".report.txt", "w", encoding="utf-8") as f:
        for r in results:
            status = "OK  " if r["ok"] else "FAIL"
            avg = (sum(r["pings"].values()) / len(r["pings"])) if r["pings"] else 0
            f.write(f"[{status}] {r['host']:40s} "
                    f"avg_ping={avg:7.1f}ms  nodes={len(r['pings'])}\n")


if __name__ == "__main__":
    main(
        input_file=".\results\hysteria2.txt",
        output_file=".\results\hysteria_servers.txt",
        min_iran_nodes_ratio=0.5,
        submit_workers=5,
        poll_workers=20,
        inter_request_delay=0.5,
    )
