
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

from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit
from urllib.parse import urlsplit, urlunsplit
from datetime import datetime
now = datetime.now()

# ============================================================
# Configuration
# ============================================================

ROOT = Path(__file__).resolve().parent.parent

SOURCES_FILE = ROOT / "sources.txt"
OUTPUT_FILE = ROOT / "results" / "hysteria2.txt"
BACKUP_FILE = (
    ROOT
    / "results"
    / now.strftime("%Y")
    / now.strftime("%m")
    / f"{now.strftime('%d')}.txt"
)

SING_BOX = shutil.which("sing-box") or "sing-box"
CURL = shutil.which("curl") or "curl"

DOWNLOAD_URL = (
    "https://speed.cloudflare.com/__down?bytes=10000000"
)

CONNECTIVITY_URL = "https://www.cloudflare.com/cdn-cgi/trace"

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))

CONNECT_TIMEOUT = int(os.getenv("CONNECT_TIMEOUT", "15"))
DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", "35"))

MIN_SPEED = float(os.getenv("MIN_SPEED", "0"))

# Maximum bytes downloaded during the speed test.
DOWNLOAD_BYTES = int(os.getenv("DOWNLOAD_BYTES", "10000000"))

# Maximum time allowed for sing-box to start.
STARTUP_TIMEOUT = 8

# Test result format:
# server URI only, without additional metadata.
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
        encoding="utf-8"
    ).splitlines():

        line = line.strip()

        if not line or line.startswith("#"):
            continue

        if not line.lower().startswith(("http://", "https://")):
            log(f"[SKIP] Invalid source URL: {line}")
            continue

        sources.append(line)

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
            log(f"[SOURCE ERROR] {url}")
            return ""

        log(f"[SOURCE OK] {url}")

        return result.stdout.strip()

    except Exception as exc:
        log(f"[SOURCE ERROR] {url}: {exc}")
        return ""


# ============================================================
# Subscription decoding
# ============================================================

HY2_PATTERN = re.compile(
    r"(?:hy2|hysteria2)://[^\s\"'<>]+",
    re.IGNORECASE,
)


def normalize_uri(uri):
    uri = uri.strip()

    # Remove trailing punctuation commonly found in text sources.
    uri = uri.rstrip("),.;]}>'\"")

    if uri.lower().startswith("hy2://"):
        uri = "hysteria2://" + uri[6:]

    return uri


def extract_uris(text):
    if not text:
        return []

    found = []

    # First, extract links directly from the content.
    for match in HY2_PATTERN.findall(text):
        found.append(normalize_uri(match))

    if found:
        return found

    # If no links exist, attempt Base64 decoding.
    compact = re.sub(r"\s+", "", text)

    if not compact:
        return []

    try:
        padded = compact + "=" * (
            (-len(compact)) % 4
        )

        decoded = base64.urlsafe_b64decode(
            padded.encode("ascii")
        ).decode("utf-8", errors="ignore")

        if decoded and decoded != text:
            return extract_uris(decoded)

    except Exception:
        pass

    return []


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

    # Deduplicate while preserving original order.
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

    if not parsed.port:
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
    ).lower() in ("1", "true", "yes")

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


# ============================================================
# Proxy testing
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
        speed = float(parts[1])

        if status_code < 200 or status_code >= 400:
            return None

        return speed

    except Exception:
        return None


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

            # First check that the proxy can reach the internet.
            connectivity = curl_test(
                port,
                CONNECTIVITY_URL,
                CONNECT_TIMEOUT,
            )

            if connectivity is None:
                return None

            # Then measure download speed.
            speed = curl_test(
                port,
                DOWNLOAD_URL,
                DOWNLOAD_TIMEOUT,
            )

            if speed is None:
                return None

            # curl reports bytes per second.
            speed_mbps = speed * 8 / 1_000_000

            if speed_mbps < MIN_SPEED:
                return None

            return {
                "uri": normalize_uri(uri),
                "speed_mbps": speed_mbps,
            }

    except Exception as exc:
        log(
            f"[ERROR] Server test failed: "
            f"{type(exc).__name__}"
        )

        return None

    finally:
        if process is not None:

            process.terminate()

            try:
                process.wait(timeout=3)

            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


# ============================================================
# Test and sort
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

            result = future.result()

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


def format_server_uri(uri, speed_mbps):
    """
    Add measured download speed in MB/s after the country flag
    in the server name.

    Example:
        🇩🇪 Germany
        ->
        🇩🇪 10.65 MB/s Germany
    """

    parts = urlsplit(uri)

    # Convert Mbps to MB/s.
    speed_mb = speed_mbps / 8

    speed_label = f"{speed_mb:.2f} MB/s"

    name = unquote(parts.fragment)

    # Remove previously added speed labels to prevent duplication.
    name = re.sub(
        r"\s*\|\s*\d+(?:\.\d+)?\s*MB/s",
        "",
        name,
        flags=re.IGNORECASE,
    )

    name = re.sub(
        r"^\s*\d+(?:\.\d+)?\s*MB/s\s*",
        "",
        name,
        flags=re.IGNORECASE,
    )

    # Insert speed immediately after the country flag.
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
        # If no country flag exists, prepend the speed.
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

    # Sort fastest first.
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
        f"valid servers to {OUTPUT_FILE}"
    )


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
    log(f"Invalid servers: {len(servers) - len(valid_servers)}")
    log("====================================")


if __name__ == "__main__":
    main()
