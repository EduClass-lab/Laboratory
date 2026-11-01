#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import time
import base64
import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter


# ─────────────────────────────
# Configuration
# ─────────────────────────────

MIN_SERVERS = 10
DAYS_REQUIRED = 7

OUTPUT_TXT = "approved_hysteria2.txt"
OUTPUT_JSON = "approved_hysteria2.json"
OUTPUT_TIME = "last_update.txt"

GITHUB_API = "https://api.github.com"
RAW_BASE = "https://raw.githubusercontent.com"

MAX_FILE_SIZE = 2 * 1024 * 1024       # 2 MB — subscriptions are tiny
MAX_FILE_WORKERS = 8                   # concurrent file fetches per repo
MAX_REPO_WORKERS = 6                   # concurrent repo verifications

ALLOWED_EXTENSIONS = {"", ".txt"}      # only .txt or extension-less files

SKIP_EXTENSIONS = {
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico",
    ".zip", ".gz", ".tar", ".tgz", ".7z", ".rar",
    ".exe", ".bin", ".so", ".dll", ".dylib",
    ".woff", ".woff2", ".ttf", ".otf",
    ".pdf", ".mp4", ".mp3", ".wasm", ".map", ".lock",
}

TOKEN = os.getenv("GH_TOKEN") or os.getenv("GITHUB_TOKEN")

HEADERS = {
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}
if TOKEN:
    HEADERS["Authorization"] = f"Bearer {TOKEN}"


def _make_session():
    session = requests.Session()
    session.headers.update(HEADERS)
    adapter = HTTPAdapter(
        pool_connections=32,
        pool_maxsize=64,      # >= MAX_FILE_WORKERS * MAX_REPO_WORKERS
        max_retries=0,        # retries handled manually below
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


SESSION = _make_session()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# ─────────────────────────────
# HTTP helpers
# ─────────────────────────────

def github_get(url, params=None, retries=4):
    """GET request against the GitHub API with retry + rate-limit handling."""
    for attempt in range(retries):
        try:
            response = SESSION.get(url, params=params, timeout=30)

            if response.status_code == 403:
                remaining = response.headers.get("X-RateLimit-Remaining")
                if remaining == "0":
                    reset = response.headers.get("X-RateLimit-Reset")
                    if reset:
                        try:
                            wait_seconds = max(1, int(reset) - int(time.time()) + 1)
                        except ValueError:
                            wait_seconds = 60
                        wait_seconds = min(wait_seconds, 120)
                        logging.warning(
                            "GitHub rate limit reached. Waiting %s seconds.",
                            wait_seconds,
                        )
                        time.sleep(wait_seconds)
                        continue

            if response.status_code in (429, 500, 502, 503, 504):
                wait_seconds = 2 ** attempt
                logging.warning(
                    "HTTP %s. Retrying in %s seconds.",
                    response.status_code, wait_seconds,
                )
                time.sleep(wait_seconds)
                continue

            response.raise_for_status()
            return response

        except requests.RequestException as exc:
            logging.warning("Request error: %s", exc)
            if attempt == retries - 1:
                return None
            time.sleep(2 ** attempt)

    return None


def raw_get(owner, repo, branch, path, retries=3):
    """Fetch raw file content from the CDN (not subject to core rate limit)."""
    url = (
        f"{RAW_BASE}/{owner}/{repo}/"
        f"{quote(branch, safe='')}/{quote(path, safe='/')}"
    )
    for attempt in range(retries):
        try:
            response = SESSION.get(url, timeout=20)
            if response.status_code == 200:
                return response.text
            if response.status_code in (429, 500, 502, 503, 504):
                time.sleep(2 ** attempt)
                continue
            return None
        except requests.RequestException:
            time.sleep(2 ** attempt)
    return None


# ─────────────────────────────
# Search repositories
# ─────────────────────────────

def search_repositories(query, max_pages=2):
    """Search public GitHub repositories."""
    repositories = {}

    for page in range(1, max_pages + 1):
        response = github_get(
            f"{GITHUB_API}/search/repositories",
            params={
                "q": query,
                "sort": "updated",
                "order": "desc",
                "per_page": 100,
                "page": page,
            },
        )
        if response is None:
            break

        data = response.json()
        items = data.get("items", [])

        for repo in items:
            repositories[repo["full_name"]] = repo

        if len(items) < 100:
            break

        time.sleep(2)  # search API has stricter limits

    return list(repositories.values())


def discover_repositories():
    """Run several targeted search queries to find candidate repositories."""
    queries = [
        "hysteria2 in:readme",
        "hysteria2 in:description",
        "hysteria2 in:name",
        "hy2 in:readme",
    ]

    found = {}

    for query in queries:
        logging.info("Searching repositories: %s", query)
        for repo in search_repositories(query):
            found[repo["full_name"]] = repo
        time.sleep(2)

    logging.info("Discovered %s repositories", len(found))
    return list(found.values())


# ─────────────────────────────
# Daily commit verification (strict)
# ─────────────────────────────

def _commit_date(commit):
    """Return the UTC date of a commit, preferring committer.date, falling
    back to author.date. Returns None if no parseable date is found."""
    for key in ("committer", "author"):
        node = commit.get("commit", {}).get(key) or {}
        date_str = node.get("date")
        if date_str:
            try:
                parsed = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                return parsed.astimezone(timezone.utc).date()
            except ValueError:
                continue
    return None


def _required_dates():
    today = datetime.now(timezone.utc).date()
    return {today - timedelta(days=i) for i in range(DAYS_REQUIRED)}


def _fetch_commit_dates(owner, repo, since_str, until_str, path=None):
    """Return the set of UTC commit dates in the window, optionally for a
    specific file path. Returns None on fetch failure."""
    found = set()
    page = 1

    while True:
        params = {
            "since": since_str,
            "until": until_str,
            "per_page": 100,
            "page": page,
        }
        if path:
            params["path"] = path

        response = github_get(
            f"{GITHUB_API}/repos/{owner}/{repo}/commits",
            params=params,
        )
        if response is None:
            return None

        commits = response.json()
        if not isinstance(commits, list):
            return None
        if not commits:
            break

        for commit in commits:
            d = _commit_date(commit)
            if d is not None:
                found.add(d)

        if len(commits) < 100:
            break
        page += 1

    return found


def repo_has_daily_commits(owner, repo):
    """Default branch must have a commit on EVERY one of the last
    DAYS_REQUIRED UTC calendar dates (today included)."""
    required = _required_dates()
    since = datetime.combine(min(required), datetime.min.time(), tzinfo=timezone.utc)
    until = datetime.now(timezone.utc)

    found = _fetch_commit_dates(
        owner, repo,
        since.strftime("%Y-%m-%dT%H:%M:%SZ"),
        until.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )
    if found is None:
        return False, set()

    return required.issubset(found), found


def file_has_daily_commits(owner, repo, path):
    """The specific subscription file must have a commit on EVERY one of
    the last DAYS_REQUIRED UTC calendar dates (today included)."""
    required = _required_dates()
    since = datetime.combine(min(required), datetime.min.time(), tzinfo=timezone.utc)
    until = datetime.now(timezone.utc)

    found = _fetch_commit_dates(
        owner, repo,
        since.strftime("%Y-%m-%dT%H:%M:%SZ"),
        until.strftime("%Y-%m-%dT%H:%M:%SZ"),
        path=path,
    )
    if found is None:
        return False, set()

    return required.issubset(found), found


# ─────────────────────────────
# File discovery & filtering
# ─────────────────────────────

def get_repository_tree(owner, repo, branch):
    """Retrieve repository tree recursively."""
    response = github_get(
        f"{GITHUB_API}/repos/{owner}/{repo}/git/trees/"
        f"{quote(branch, safe='')}",
        params={"recursive": "1"},
    )
    if response is None:
        return []

    data = response.json()
    if data.get("truncated"):
        logging.warning("Repository tree was truncated: %s/%s", owner, repo)

    return data.get("tree", [])


def is_candidate(item):
    """Accept only .txt files and extension-less files, within size limit."""
    if item.get("type") != "blob":
        return False

    path = item.get("path", "")
    if not path:
        return False

    if item.get("size", 0) > MAX_FILE_SIZE:
        return False

    ext = os.path.splitext(path)[1].lower()
    if ext in SKIP_EXTENSIONS:
        return False
    if ext not in ALLOWED_EXTENSIONS:
        return False

    base = os.path.basename(path)
    if base.startswith("."):
        return False
    if base.lower() in {"license", "makefile", "dockerfile", "procfile"}:
        return False

    return True


# ─────────────────────────────
# Hysteria2 counting
# ─────────────────────────────

def count_hysteria2(content):
    """Count Hysteria2 URI occurrences in plain or Base64 text."""
    if not content:
        return 0

    content = content.strip()

    direct_count = len(
        re.findall(r"hysteria2://", content, flags=re.IGNORECASE)
    )
    if direct_count:
        return direct_count

    compact = re.sub(r"\s+", "", content)
    if not compact:
        return 0

    try:
        padding = "=" * (-len(compact) % 4)
        decoded = base64.b64decode(
            compact + padding, validate=False
        ).decode("utf-8", errors="ignore")
        return len(
            re.findall(r"hysteria2://", decoded, flags=re.IGNORECASE)
        )
    except Exception:
        return 0


def find_best_file(owner, repo, branch):
    """Find the file with the most Hysteria2 servers (concurrent scan)."""
    tree = get_repository_tree(owner, repo, branch)
    candidates = [item for item in tree if is_candidate(item)]

    if not candidates:
        return None, 0

    best_file = None
    best_count = 0

    with ThreadPoolExecutor(max_workers=MAX_FILE_WORKERS) as pool:
        futures = {
            pool.submit(raw_get, owner, repo, branch, item["path"]): item["path"]
            for item in candidates
        }
        for fut in as_completed(futures):
            path = futures[fut]
            try:
                content = fut.result()
            except Exception:
                continue
            if not content:
                continue
            count = count_hysteria2(content)
            if count > best_count:
                best_count = count
                best_file = path

    if best_file and best_count >= MIN_SERVERS:
        return best_file, best_count

    return None, 0


# ─────────────────────────────
# Repository verification
# ─────────────────────────────

def verify_repository(repo_data):
    """Verify a repository and return its selected subscription file."""
    full_name = repo_data.get("full_name", "")
    if "/" not in full_name:
        return None

    owner, repo = full_name.split("/", 1)
    branch = repo_data.get("default_branch", "main")

    logging.info("Checking %s", full_name)

    # ── Step 1: strict repo-level daily commit check ──
    ok, found = repo_has_daily_commits(owner, repo)
    if not ok:
        missing = sorted(_required_dates() - found)
        logging.info(
            "Rejected (repo-level commits): %s | missing dates: %s",
            full_name,
            ", ".join(d.isoformat() for d in missing) if missing else "n/a",
        )
        return None

    # ── Step 2: find best candidate file ──
    best_file, server_count = find_best_file(owner, repo, branch)
    if not best_file:
        logging.info(
            "Rejected (no file with >= %s servers): %s",
            MIN_SERVERS, full_name,
        )
        return None

    # ── Step 3: strict file-level daily commit check ──
    ok, found = file_has_daily_commits(owner, repo, best_file)
    if not ok:
        missing = sorted(_required_dates() - found)
        logging.info(
            "Rejected (file-level commits): %s/%s | missing dates: %s",
            full_name, best_file,
            ", ".join(d.isoformat() for d in missing) if missing else "n/a",
        )
        return None

    raw_url = (
        f"{RAW_BASE}/{owner}/{repo}/"
        f"{quote(branch, safe='')}/{quote(best_file, safe='/')}"
    )

    return {
        "repository": full_name,
        "branch": branch,
        "file": best_file,
        "servers": server_count,
        "url": raw_url,
    }


# ─────────────────────────────
# Output files
# ─────────────────────────────

def write_outputs(results):
    """Write approved links and metadata."""
    results = sorted(
        results,
        key=lambda item: item["servers"],
        reverse=True,
    )
    links = [item["url"] for item in results]

    with open(OUTPUT_TXT, "w", encoding="utf-8") as file:
        file.write("\n".join(links))
        if links:
            file.write("\n")

    with open(OUTPUT_JSON, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False, indent=2)

    now = datetime.now(timezone.utc).isoformat()
    with open(OUTPUT_TIME, "w", encoding="utf-8") as file:
        file.write(now + "\n")

    logging.info(
        "Updated %s with %s approved links",
        OUTPUT_TXT, len(links),
    )


# ─────────────────────────────
# Main
# ─────────────────────────────

def main():
    repositories = discover_repositories()
    approved = []

    with ThreadPoolExecutor(max_workers=MAX_REPO_WORKERS) as pool:
        futures = {
            pool.submit(verify_repository, repo): repo
            for repo in repositories
        }
        for fut in as_completed(futures):
            repo = futures[fut]
            try:
                result = fut.result()
            except Exception as exc:
                logging.exception(
                    "Error processing %s: %s",
                    repo.get("full_name", "unknown"), exc,
                )
                continue

            if result:
                approved.append(result)
                logging.info(
                    "APPROVED: %s | %s servers | %s",
                    result["repository"],
                    result["servers"],
                    result["file"],
                )

    write_outputs(approved)
    logging.info("Finished. Total approved repositories: %s", len(approved))


if __name__ == "__main__":
    main()
