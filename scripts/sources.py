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

import requests


# ─────────────────────────────
# Configuration
# ─────────────────────────────

MIN_SERVERS = 10
DAYS_REQUIRED = 7

OUTPUT_TXT = "approved_hysteria2.txt"
OUTPUT_JSON = "approved_hysteria2.json"
OUTPUT_TIME = "last_update.txt"

GITHUB_API = "https://api.github.com"

TOKEN = os.getenv("GH_TOKEN") or os.getenv("GITHUB_TOKEN")

HEADERS = {
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}

if TOKEN:
    HEADERS["Authorization"] = f"Bearer {TOKEN}"

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# ─────────────────────────────
# HTTP helpers
# ─────────────────────────────

def github_get(url, params=None, retries=4):
    """GET request with retry handling."""

    for attempt in range(retries):
        try:
            response = SESSION.get(
                url,
                params=params,
                timeout=30,
            )

            if response.status_code == 403:
                remaining = response.headers.get("X-RateLimit-Remaining")

                if remaining == "0":
                    reset = response.headers.get("X-RateLimit-Reset")

                    if reset:
                        wait_seconds = max(
                            int(reset) - int(time.time()) + 5,
                            5,
                        )
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
                    response.status_code,
                    wait_seconds,
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


# ─────────────────────────────
# Search repositories
# ─────────────────────────────

def search_repositories(query, max_pages=5):
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

        time.sleep(0.5)

    return list(repositories.values())


def discover_repositories():
    """Run several search queries to find candidate repositories."""

    queries = [
        "hysteria2 subscription",
        "hysteria2://",
        "hysteria2 config",
        "Hysteria2 nodes",
        "hy2 subscription",
        "hy2://",
    ]

    found = {}

    for query in queries:
        logging.info("Searching repositories: %s", query)

        repos = search_repositories(query)

        for repo in repos:
            found[repo["full_name"]] = repo

        time.sleep(1)

    logging.info("Discovered %s repositories", len(found))

    return list(found.values())


# ─────────────────────────────
# Daily commit verification
# ─────────────────────────────

def has_daily_commits(owner, repo):
    """
    Check whether repository default branch has at least one commit
    on each of the last 7 UTC calendar dates, including today.
    """

    now = datetime.now(timezone.utc)

    today = now.date()
    required_dates = {
        today - timedelta(days=i)
        for i in range(DAYS_REQUIRED)
    }

    since = datetime.combine(
        min(required_dates),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )

    since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
    until_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    found_dates = set()
    page = 1

    while True:
        response = github_get(
            f"{GITHUB_API}/repos/{owner}/{repo}/commits",
            params={
                "since": since_str,
                "until": until_str,
                "per_page": 100,
                "page": page,
            },
        )

        if response is None:
            return False

        commits = response.json()

        if not isinstance(commits, list):
            return False

        if not commits:
            break

        for commit in commits:
            commit_date = (
                commit.get("commit", {})
                .get("committer", {})
                .get("date")
            )

            if not commit_date:
                continue

            try:
                parsed = datetime.fromisoformat(
                    commit_date.replace("Z", "+00:00")
                )

                found_dates.add(parsed.astimezone(timezone.utc).date())

            except ValueError:
                continue

        if found_dates >= required_dates:
            return True

        if len(commits) < 100:
            break

        page += 1
        time.sleep(0.25)

    return found_dates >= required_dates


# ─────────────────────────────
# File discovery
# ─────────────────────────────

def get_repository_tree(owner, repo, branch):
    """Retrieve repository tree recursively."""

    response = github_get(
        f"{GITHUB_API}/repos/{owner}/{repo}/git/trees/{quote(branch, safe='')}",
        params={"recursive": "1"},
    )

    if response is None:
        return []

    data = response.json()

    if data.get("truncated"):
        logging.warning(
            "Repository tree was truncated: %s/%s",
            owner,
            repo,
        )

    return data.get("tree", [])


def get_file_content(owner, repo, path):
    """Retrieve file contents from GitHub."""

    encoded_path = quote(path, safe="/")

    response = github_get(
        f"{GITHUB_API}/repos/{owner}/{repo}/contents/{encoded_path}"
    )

    if response is None:
        return None

    data = response.json()

    if data.get("encoding") == "base64":
        try:
            content = base64.b64decode(
                data.get("content", "")
            ).decode("utf-8", errors="ignore")

            return content

        except Exception:
            return None

    return data.get("content")


# ─────────────────────────────
# Hysteria2 counting
# ─────────────────────────────

def count_hysteria2(content):
    """Count Hysteria2 URI occurrences in plain or Base64 text."""

    if not content:
        return 0

    content = content.strip()

    direct_count = len(
        re.findall(
            r"hysteria2://",
            content,
            flags=re.IGNORECASE,
        )
    )

    if direct_count:
        return direct_count

    # Try Base64-decoding subscription content.
    compact = re.sub(r"\s+", "", content)

    try:
        padding = "=" * (-len(compact) % 4)

        decoded = base64.b64decode(
            compact + padding
        ).decode("utf-8", errors="ignore")

        decoded_count = len(
            re.findall(
                r"hysteria2://",
                decoded,
                flags=re.IGNORECASE,
            )
        )

        return decoded_count

    except Exception:
        return 0


def is_candidate_file(path):
    """Filter likely text subscription files."""

    path_lower = path.lower()

    allowed_extensions = (
        ".txt",
        "raw",
        "",
    )

    return (
        path_lower.endswith(allowed_extensions)
        and not path_lower.endswith(".lock")
    )


def find_best_file(owner, repo, branch):
    """
    Find the file containing the highest number of Hysteria2 servers.
    """

    tree = get_repository_tree(owner, repo, branch)

    candidates = [
        item
        for item in tree
        if item.get("type") == "blob"
        and is_candidate_file(item.get("path", ""))
    ]

    best_file = None
    best_count = 0

    for item in candidates:
        path = item["path"]

        content = get_file_content(
            owner,
            repo,
            path,
        )

        if content is None:
            continue

        count = count_hysteria2(content)

        if count > best_count:
            best_count = count
            best_file = path

        if best_count >= MIN_SERVERS:
            # Still continue scanning because another file
            # might contain more servers.
            pass

    if best_file and best_count >= MIN_SERVERS:
        return best_file, best_count

    return None, 0


# ─────────────────────────────
# Repository verification
# ─────────────────────────────

def verify_repository(repo_data):
    """Verify repository and return its selected subscription file."""

    full_name = repo_data.get("full_name", "")

    if "/" not in full_name:
        return None

    owner, repo = full_name.split("/", 1)

    branch = repo_data.get("default_branch", "main")

    logging.info("Checking %s", full_name)

    if not has_daily_commits(owner, repo):
        logging.info(
            "Rejected: missing daily commits: %s",
            full_name,
        )
        return None

    best_file, server_count = find_best_file(
        owner,
        repo,
        branch,
    )

    if not best_file:
        logging.info(
            "Rejected: no file with at least %s servers: %s",
            MIN_SERVERS,
            full_name,
        )
        return None

    raw_url = (
        f"https://raw.githubusercontent.com/"
        f"{owner}/{repo}/{branch}/{best_file}"
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

    # One link per repository is already guaranteed.
    links = [
        item["url"]
        for item in results
    ]

    with open(
        OUTPUT_TXT,
        "w",
        encoding="utf-8",
    ) as file:
        file.write("\n".join(links))

        if links:
            file.write("\n")

    with open(
        OUTPUT_JSON,
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            results,
            file,
            ensure_ascii=False,
            indent=2,
        )

    now = datetime.now(timezone.utc).isoformat()

    with open(
        OUTPUT_TIME,
        "w",
        encoding="utf-8",
    ) as file:
        file.write(now + "\n")

    logging.info(
        "Updated %s with %s approved links",
        OUTPUT_TXT,
        len(links),
    )


# ─────────────────────────────
# Main
# ─────────────────────────────

def main():
    repositories = discover_repositories()

    approved = []

    for repo_data in repositories:
        try:
            result = verify_repository(repo_data)

            if result:
                approved.append(result)
                write_outputs(approved)
                logging.info(
                    "APPROVED: %s | %s servers | %s",
                    result["repository"],
                    result["servers"],
                    result["file"],
                )

        except Exception as exc:
            logging.exception(
                "Error processing %s: %s",
                repo_data.get("full_name", "unknown"),
                exc,
            )

        time.sleep(0.3)

    logging.info(
        "Finished. Total approved repositories: %s",
        len(approved),
    )


if __name__ == "__main__":
    main()
