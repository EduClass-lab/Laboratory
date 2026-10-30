#!/usr/bin/env python3

import base64
import concurrent.futures
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time

from datetime import datetime
from pathlib import Path
from urllib.parse import (
    parse_qs,
    unquote,
    urlsplit,
    urlunsplit,
)
from zoneinfo import ZoneInfo


# ============================================================
# Configuration
# ============================================================

ROOT = Path(__file__).resolve().parent.parent

SOURCES_FILE = ROOT / "sources.txt"
OUTPUT_FILE = ROOT / "results" / "hysteria2.txt"

now = datetime.now()

BACKUP_FILE = (
    ROOT
    / "results"
    / NOW.strftime("%Y")
    / NOW.strftime("%m")
    / f"{NOW.strftime('%d')}.txt"
)

SING_BOX = shutil.which("sing-box") or "sing-box"
CURL = shutil.which("curl") or "curl"

CONNECTIVITY_URL = "https://www.cloudflare.com/cdn-cgi/trace"

DOWNLOAD_BYTES = int(
    os.getenv("DOWNLOAD_BYTES", "10000000")
)

DOWNLOAD_URL = (
    "https://speed.cloudflare.com/__down"
    f"?bytes={DOWNLOAD_BYTES}"
)

MAX_WORKERS = max(
    1,
    int(os.getenv("MAX_WORKERS", "10")),
)

CONNECT_TIMEOUT = max(
    1,
    int(os.getenv("CONNECT_TIMEOUT", "15")),
)

DOWNLOAD_TIMEOUT = max(
    1,
    int(os.getenv("DOWNLOAD_TIMEOUT", "35")),
)

MIN_SPEED = max(
    0.0,
    float(os.getenv("MIN_SPEED", "0")),
)

STARTUP_TIMEOUT = max(
    1,
    int(os.getenv("STARTUP_TIMEOUT", "8")),
)

# Output is URI-only, with speed added to the URI fragment name.
OUTPUT_FORMAT = "uri"

print_lock = threading.Lock()


# ============================================================
# Logging
# ============================================================

def log(message):
    with print_lock:
        print(message, flush=True)


# ============================================================
# Source loading
# ============================================================

def load_sources():
    if not SOURCES_FILE.exists():
        raise FileNotFoundError(
            f"Sources file does not exist: {SOURCES_FILE}"
        )

    sources = []

    for line in SOURCES_FILE.read_text(
        encoding="utf-8",
        errors="replace",
    ).splitlines():

        line = line.strip()

        if not line or line.startswith("#"):
            continue

        if not line.lower().startswith(
            ("http://", "https://")
        ):
            log(f"[SKIP] Invalid source URL: {line}")
            continue

        sources.append(line)

    # Deduplicate source URLs while preserving their order.
    return list(dict.fromkeys(sources))


def download_source(url):
    try:
        result = subprocess.run(
            [
                CURL,
                "-fsSL",
                "--connect-timeout",
                str(CONNECT_TIMEOUT),
                "--max-time",
                "30",
                "--retry",
                "1",
                url,
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=35,
        )

        if result.returncode != 0:
            log(f"[SOURCE ERROR] Failed to download: {url}")
            return ""

        log(f"[SOURCE OK] {url}")

        return result.stdout.strip()

    except subprocess.TimeoutExpired:
        log(f"[SOURCE ERROR] Download timed out: {url}")
        return ""

    except Exception as exc:
        log(
            f"[SOURCE ERROR] {url}: "
            f"{type(exc).__name__}: {exc}"
        )
        return ""


# ============================================================
# Hysteria2 URI extraction and normalization
# ============================================================

HY2_PATTERN = re.compile(
    r"(?:hy2|hysteria2)://[^\s\"'<>]+",
    re.IGNORECASE,
)


def normalize_uri(uri):
    uri = uri.strip()

    # Remove trailing punctuation often included in source text.
    uri = uri.rstrip("),.;]}>'\"")

    # Normalize hy2:// to hysteria2://.
    if uri.lower().startswith("hy2://"):
        uri = "hysteria2://" + uri[6:]

    return uri


def extract_uris(text, max_decode_depth=5):
    """
    Extract Hysteria2 URIs from direct or Base64-encoded content.

    This supports:
      - Plain hysteria2:// links
      - Plain hy2:// links
      - Base64 subscription content
      - Mixed direct and encoded content
    """

    if not text:
        return []

    found = []

    # First, collect all directly visible URIs.
    for match in HY2_PATTERN.findall(text):
        found.append(normalize_uri(match))

    # Avoid infinite recursive decoding.
    if max_decode_depth <= 0:
        return list(dict.fromkeys(found))

    compact = re.sub(r"\s+", "", text)

    if not compact:
        return list(dict.fromkeys(found))

    # Attempt Base64 decoding even if direct URIs were found.
    # This allows mixed direct + encoded source content.
    try:
        padded = compact + "=" * (
            (-len(compact)) % 4
        )

        decoded_bytes = base64.urlsafe_b64decode(
            padded.encode("ascii")
        )

        decoded = decoded_bytes.decode(
            "utf-8",
            errors="ignore",
        )

        # Only recurse when decoding produced meaningful new text.
        if decoded and decoded.strip() != text.strip():
            decoded_uris = extract_uris(
                decoded,
                max_decode_depth=max_decode_depth - 1,
            )

            found.extend(decoded_uris)

    except Exception:
        pass

    # Deduplicate while preserving order.
    return list(dict.fromkeys(found))


def collect_servers():
    sources = load_sources()

    log(f"[INFO] Sources: {len(sources)}")

    all_servers = []

    for source in sources:
        content = download_source(source)

        servers = extract_uris(content)

        log(
            f"[INFO] Extracted {len(servers)} "
            f"Hysteria2 servers"
        )

        all_servers.extend(servers)

    unique_servers = list(dict.fromkeys(all_servers))

    log(
        f"[INFO] Total unique servers: "
        f"{len(unique_servers)}"
    )

    return unique_servers


# ============================================================
# Hysteria2 URI parsing
# ============================================================

def parse_hysteria2_uri(uri):
    uri = normalize_uri(uri)

    parsed = urlsplit(uri)

    if parsed.scheme.lower() != "hysteria2":
        raise ValueError("Unsupported URI scheme")

    if not parsed.hostname:
        raise ValueError("Missing server hostname")

    if parsed.port is None:
        raise ValueError("Missing server port")

    password = unquote(parsed.username or "")

    if not password:
        raise ValueError("Missing Hysteria2 password")

    query = parse_qs(
        parsed.query,
        keep_blank_values=True,
    )

    def get_param(*names, default=None):
        for name in names:
            values = query.get(name)

            if values:
                return values[0]

        return default

    server_name = get_param(
        "sni",
        "server_name",
        "servername",
        default=parsed.hostname,
    )

    insecure = get_param(
        "insecure",
        "allowInsecure",
        default="0",
    ).lower() in (
        "1",
        "true",
        "yes",
    )

    alpn = get_param("alpn")

    config = {
        "type": "hysteria2",
        "tag": "proxy",
        "server": parsed.hostname,
        "server_port": parsed.port,
        "password": password,
        "tls": {
            "enabled": True,
            "server_name": server_name,
            "insecure": insecure,
        },
    }

    if alpn:
        config["tls"]["alpn"] = [
            item.strip()
            for item in alpn.split(",")
            if item.strip()
        ]

    # Hysteria2 Salamander obfuscation.
    obfs_type = get_param("obfs")

    obfs_password = get_param(
        "obfs-password",
        "obfs_password",
    )

    if obfs_type and obfs_password:
        config["obfs"] = {
            "type": obfs_type,
            "password": obfs_password,
        }

    return config


# ============================================================
# Local SOCKS5 port
# ============================================================

def get_free_port():
    with socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM,
    ) as sock:

        sock.bind(("127.0.0.1", 0))

        return sock.getsockname()[1]


# ============================================================
# sing-box configuration
# ============================================================

def create_singbox_config(outbound, port):
    return {
        "log": {
            "level": "error",
        },
        "inbounds": [
            {
                "type": "socks",
                "tag": "socks-in",
                "listen": "127.0.0.1",
                "listen_port": port,
            }
        ],
        "outbounds": [
            outbound,
            {
                "type": "direct",
                "tag": "direct",
            },
        ],
        "route": {
            "final": "proxy",
        },
    }


def validate_singbox_config(config_file):
    """
    Validate the generated config before starting sing-box.
    """

    try:
        result = subprocess.run(
            [
                SING_BOX,
                "check",
                "-c",
                str(config_file),
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )

        if result.returncode == 0:
            return True

        error = (
            result.stderr.strip()
            or result.stdout.strip()
            or "Unknown sing-box config error"
        )

        log(f"[CONFIG ERROR] {error}")

        return False

    except Exception as exc:
        log(
            f"[CONFIG ERROR] "
            f"{type(exc).__name__}: {exc}"
        )

        return False


# ============================================================
# Proxy startup
# ============================================================

def wait_for_proxy(port, process):
    deadline = time.monotonic() + STARTUP_TIMEOUT

    while time.monotonic() < deadline:

        if process.poll() is not None:
            return False

        try:
            with socket.create_connection(
                ("127.0.0.1", port),
                timeout=0.5,
            ):
                return True

        except OSError:
            time.sleep(0.2)

    return False


# ============================================================
# Curl testing
# ============================================================

def curl_test(port, url, timeout):
    proxy = f"socks5h://127.0.0.1:{port}"

    command = [
        CURL,
        "-sS",
        "-L",
        "--connect-timeout",
        str(CONNECT_TIMEOUT),
        "--max-time",
        str(timeout),
        "--proxy",
        proxy,
        "-o",
        os.devnull,
        "-w",
        "%{http_code} %{speed_download}",
        url,
    ]

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout + 5,
        )

        if result.returncode != 0:
            return None

        parts = result.stdout.strip().split()

        if len(parts) != 2:
            return None

        status_code = int(parts[0])
        speed_bytes_per_second = float(parts[1])

        if status_code < 200 or status_code >= 400:
            return None

        return speed_bytes_per_second

    except subprocess.TimeoutExpired:
        return None

    except Exception:
        return None


# ============================================================
# Test one Hysteria2 server
# ============================================================

def test_server(uri):
    process = None

    try:
        outbound = parse_hysteria2_uri(uri)

        port = get_free_port()

        config = create_singbox_config(
            outbound,
            port,
        )

        with tempfile.TemporaryDirectory(
            prefix="hy2-test-"
        ) as temp_dir:

            config_file = (
                Path(temp_dir) / "config.json"
            )

            config_file.write_text(
                json.dumps(
                    config,
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            # Validate config before launching sing-box.
            if not validate_singbox_config(config_file):
                return None

            process = subprocess.Popen(
                [
                    SING_BOX,
                    "run",
                    "-c",
                    str(config_file),
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

            if not wait_for_proxy(port, process):
                return None

            # First check internet connectivity.
            connectivity = curl_test(
                port,
                CONNECTIVITY_URL,
                CONNECT_TIMEOUT,
            )

            if connectivity is None:
                return None

            # Then measure download speed.
            speed_bytes_per_second = curl_test(
                port,
                DOWNLOAD_URL,
                DOWNLOAD_TIMEOUT,
            )

            if speed_bytes_per_second is None:
                return None

            # Convert bytes/sec to megabits/sec.
            speed_mbps = (
                speed_bytes_per_second * 8 / 1_000_000
            )

            if speed_mbps < MIN_SPEED:
                return None

            return {
                "uri": normalize_uri(uri),
                "speed_mbps": speed_mbps,
            }

    except Exception as exc:
        log(
            f"[ERROR] Server test failed: "
            f"{type(exc).__name__}: {exc}"
        )

        return None

    finally:
        if process is not None:

            if process.poll() is None:
                process.terminate()

            try:
                process.wait(timeout=3)

            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


# ============================================================
# Test and sort servers
# ============================================================

def test_servers(servers):
    valid_servers = []

    total = len(servers)

    log(
        f"[INFO] Testing {total} servers "
        f"with {MAX_WORKERS} workers"
    )

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        futures = {
            executor.submit(test_server, server): server
            for server in servers
        }

        completed = 0

        for future in concurrent.futures.as_completed(
            futures
        ):

            completed += 1

            try:
                result = future.result()

            except Exception as exc:
                log(
                    f"[ERROR] Worker failed: "
                    f"{type(exc).__name__}: {exc}"
                )
                result = None

            if result is not None:
                valid_servers.append(result)

                log(
                    f"[{completed}/{total}] "
                    f"[VALID] "
                    f"{result['speed_mbps']:.2f} Mbps"
                )

            else:
                log(
                    f"[{completed}/{total}] "
                    f"[INVALID]"
                )

    # Fastest server first.
    valid_servers.sort(
        key=lambda item: item["speed_mbps"],
        reverse=True,
    )

    return valid_servers


# ============================================================
# Format URI name with speed
# ============================================================

def format_server_uri(uri, speed_mbps):
    """
    Add measured speed in MB/s after the country flag.

    Example:
        🇩🇪 Germany
        ->
        🇩🇪 10.65 MB/s Germany

    Existing speed labels are removed first, so reruns
    do not add duplicate labels.
    """

    uri = normalize_uri(uri)

    parts = urlsplit(uri)

    # Convert Mbps to MB/s.
    speed_mb = speed_mbps / 8

    speed_label = f"{speed_mb:.2f} MB/s"

    name = unquote(parts.fragment).strip()

    # Remove old MB/s labels anywhere in the name.
    name = re.sub(
        r"\s*\d+(?:\.\d+)?\s*MB/s",
        "",
        name,
        flags=re.IGNORECASE,
    )

    # Clean up duplicate spaces and separators.
    name = re.sub(
        r"\s*\|\s*\|?\s*",
        " ",
        name,
    )

    name = re.sub(
        r"\s{2,}",
        " ",
        name,
    ).strip()

    # Match a two-character country flag at the beginning.
    flag_pattern = re.compile(
        r"^(\s*[\U0001F1E6-\U0001F1FF]{2})"
    )

    match = flag_pattern.match(name)

    if match:
        flag = match.group(1)
        remaining_name = name[match.end():].strip()

        name = (
            f"{flag} {speed_label}"
            + (
                f" {remaining_name}"
                if remaining_name
                else ""
            )
        )

    else:
        # If no country flag exists, put speed at the beginning.
        name = f"{speed_label} {name}".strip()

    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path,
            parts.query,
            name,
        )
    )


# ============================================================
# Save results
# ============================================================

def save_results(valid_servers):
    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    BACKUP_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Sort fastest first before writing.
    valid_servers.sort(
        key=lambda item: item["speed_mbps"],
        reverse=True,
    )

    content = "\n".join(
        format_server_uri(
            server["uri"],
            server["speed_mbps"],
        )
        for server in valid_servers
    )

    if content:
        content += "\n"

    OUTPUT_FILE.write_text(
        content,
        encoding="utf-8",
    )

    BACKUP_FILE.write_text(
        content,
        encoding="utf-8",
    )

    log(
        f"[INFO] Saved {len(valid_servers)} "
        f"valid servers"
    )

    log(f"[INFO] Latest results: {OUTPUT_FILE}")
    log(f"[INFO] Daily backup: {BACKUP_FILE}")


# ============================================================
# Main
# ============================================================

def main():
    if shutil.which(SING_BOX) is None:
        log("[ERROR] sing-box executable not found")
        sys.exit(1)

    if shutil.which(CURL) is None:
        log("[ERROR] curl executable not found")
        sys.exit(1)

    if not SOURCES_FILE.exists():
        log(
            f"[ERROR] Sources file not found: "
            f"{SOURCES_FILE}"
        )
        sys.exit(1)

    servers = collect_servers()

    if not servers:
        log("[WARNING] No Hysteria2 servers found")

        save_results([])

        return

    valid_servers = test_servers(servers)

    save_results(valid_servers)

    log("")
    log("====================================")
    log("Hysteria2 testing completed")
    log(f"Total servers: {len(servers)}")
    log(f"Valid servers: {len(valid_servers)}")
    log(
        f"Invalid servers: "
        f"{len(servers) - len(valid_servers)}"
    )
    log("====================================")


if __name__ == "__main__":
    main()
