
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
    parse_qsl,
    unquote,
    urlsplit,
    urlunsplit,
)


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
    / now.strftime("%Y")
    / now.strftime("%m")
    / f"{now.strftime('%d')}.txt"
)

SING_BOX = shutil.which("sing-box") or "sing-box"
CURL = shutil.which("curl") or "curl"

CONNECTIVITY_URL = (
    "https://www.cloudflare.com/cdn-cgi/trace"
)

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
            log(f"[SOURCE ERROR] Failed: {url}")
            return ""

        log(f"[SOURCE OK] {url}")

        return result.stdout.strip()

    except subprocess.TimeoutExpired:

        log(f"[SOURCE ERROR] Timeout: {url}")
        return ""

    except Exception as exc:

        log(
            f"[SOURCE ERROR] {url}: "
            f"{type(exc).__name__}: {exc}"
        )

        return ""


# ============================================================
# Hysteria2 URI normalization
# ============================================================

HY2_PATTERN = re.compile(
    r"(?:hy2|hysteria2)://[^\s\"'<>]+",
    re.IGNORECASE,
)

COUNTRY_FLAG_PATTERN = re.compile(
    r"[\U0001F1E6-\U0001F1FF]{2}"
)


def normalize_uri(uri):

    uri = uri.strip()

    uri = uri.rstrip("),.;]}>'\"")

    if uri.lower().startswith("hy2://"):
        uri = "hysteria2://" + uri[6:]

    return uri


# ============================================================
# Canonical duplicate detection
# ============================================================

def canonical_hysteria2_key(uri):
    """
    Generate a canonical identity for a Hysteria2 URI.

    Ignores:
      - Country names
      - Country flags
      - Display names
      - Speed labels
      - URI fragments
      - Query parameter ordering

    Preserves:
      - Hostname
      - Port
      - Password
      - Connection path
      - SNI
      - TLS settings
      - ALPN
      - Obfuscation settings
      - Other connection parameters
    """

    uri = normalize_uri(uri)

    parsed = urlsplit(uri)

    if parsed.scheme.lower() != "hysteria2":
        raise ValueError("Unsupported URI scheme")

    hostname = parsed.hostname

    if not hostname:
        raise ValueError("Missing hostname")

    port = parsed.port

    if port is None:
        raise ValueError("Missing port")

    hostname = hostname.lower().rstrip(".")

    password = unquote(
        parsed.username or ""
    )

    aliases = {
        "sni": "server_name",
        "servername": "server_name",
        "server_name": "server_name",
        "allowinsecure": "insecure",
        "insecure": "insecure",
        "obfs_password": "obfs-password",
        "obfs-password": "obfs-password",
    }

    query_items = parse_qsl(
        parsed.query,
        keep_blank_values=True,
    )

    normalized_query = []

    for key, value in query_items:

        normalized_key = key.lower()

        normalized_key = aliases.get(
            normalized_key,
            normalized_key,
        )

        if normalized_key == "insecure":

            value = value.lower()

            if value in ("true", "yes"):
                value = "1"

            elif value in ("false", "no"):
                value = "0"

        if normalized_key == "alpn":

            value = ",".join(
                sorted(
                    item.strip()
                    for item in value.split(",")
                    if item.strip()
                )
            )

        normalized_query.append(
            (
                normalized_key,
                value,
            )
        )

    normalized_query.sort()

    return (
        "hysteria2",
        hostname,
        port,
        password,
        parsed.path,
        tuple(normalized_query),
    )


def deduplicate_hysteria2_uris(uris):

    unique_uris = []
    seen = set()

    duplicates = 0
    invalid = 0

    for uri in uris:

        try:

            key = canonical_hysteria2_key(uri)

        except (
            ValueError,
            TypeError,
        ) as exc:

            invalid += 1

            log(
                f"[DEDUP SKIP] "
                f"{type(exc).__name__}: {exc}"
            )

            continue

        if key in seen:

            duplicates += 1
            continue

        seen.add(key)

        unique_uris.append(
            normalize_uri(uri)
        )

    log(
        f"[DEDUP] Input: {len(uris)} | "
        f"Unique: {len(unique_uris)} | "
        f"Duplicates: {duplicates} | "
        f"Invalid: {invalid}"
    )

    return unique_uris


# ============================================================
# Recursive URI extraction
# ============================================================

def extract_uris(
    text,
    max_decode_depth=5,
):

    if not text:
        return []

    found = []

    for match in HY2_PATTERN.findall(text):

        found.append(
            normalize_uri(match)
        )

    if max_decode_depth <= 0:

        return deduplicate_hysteria2_uris(
            found
        )

    compact = re.sub(
        r"\s+",
        "",
        text,
    )

    if not compact:

        return deduplicate_hysteria2_uris(
            found
        )

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

        if (
            decoded
            and decoded.strip() != text.strip()
        ):

            decoded_uris = extract_uris(
                decoded,
                max_decode_depth - 1,
            )

            found.extend(decoded_uris)

    except Exception:
        pass

    return deduplicate_hysteria2_uris(
        found
    )


# ============================================================
# Collect servers
# ============================================================

def collect_servers():

    sources = load_sources()

    log(f"[INFO] Sources: {len(sources)}")

    all_servers = []

    for source in sources:

        content = download_source(source)

        servers = extract_uris(content)

        log(
            f"[INFO] Extracted {len(servers)} "
            f"Hysteria2 servers from source"
        )

        all_servers.extend(servers)

    # Global duplicate detection across all sources.
    unique_servers = (
        deduplicate_hysteria2_uris(
            all_servers
        )
    )

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
        raise ValueError("Missing hostname")

    if parsed.port is None:
        raise ValueError("Missing port")

    password = unquote(
        parsed.username or ""
    )

    if not password:
        raise ValueError("Missing password")

    query = parse_qs(
        parsed.query,
        keep_blank_values=True,
    )

    def get_param(
        *names,
        default=None,
    ):

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

        sock.bind(
            (
                "127.0.0.1",
                0,
            )
        )

        return sock.getsockname()[1]


# ============================================================
# sing-box configuration
# ============================================================

def create_singbox_config(
    outbound,
    port,
):

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

def wait_for_proxy(
    port,
    process,
):

    deadline = (
        time.monotonic()
        + STARTUP_TIMEOUT
    )

    while time.monotonic() < deadline:

        if process.poll() is not None:
            return False

        try:

            with socket.create_connection(
                (
                    "127.0.0.1",
                    port,
                ),
                timeout=0.5,
            ):

                return True

        except OSError:

            time.sleep(0.2)

    return False


# ============================================================
# Curl testing
# ============================================================

def curl_test(
    port,
    url,
    timeout,
):

    proxy = (
        f"socks5h://127.0.0.1:{port}"
    )

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

        speed_bytes_per_second = float(
            parts[1]
        )

        if (
            status_code < 200
            or status_code >= 400
        ):

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
                Path(temp_dir)
                / "config.json"
            )

            config_file.write_text(
                json.dumps(
                    config,
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            if not validate_singbox_config(
                config_file
            ):

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

            if not wait_for_proxy(
                port,
                process,
            ):

                return None

            connectivity = curl_test(
                port,
                CONNECTIVITY_URL,
                CONNECT_TIMEOUT,
            )

            if connectivity is None:
                return None

            speed_bytes_per_second = curl_test(
                port,
                DOWNLOAD_URL,
                DOWNLOAD_TIMEOUT,
            )

            if speed_bytes_per_second is None:
                return None

            speed_mbps = (
                speed_bytes_per_second
                * 8
                / 1_000_000
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
            executor.submit(
                test_server,
                server,
            ): server
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

    valid_servers.sort(
        key=lambda item: item["speed_mbps"],
        reverse=True,
    )

    return valid_servers


# ============================================================
# Format URI name: country flag + speed only
# ============================================================

def format_server_uri(
    uri,
    speed_mbps,
):
    """
    Output format:

        hysteria2://...#🇩🇪 12.50 MB/s

    All extra names and descriptions are removed.
    """

    uri = normalize_uri(uri)

    parts = urlsplit(uri)

    original_name = unquote(
        parts.fragment
    ).strip()

    # Extract the first country flag.
    flag_match = COUNTRY_FLAG_PATTERN.search(
        original_name
    )

    if flag_match:

        country_flag = flag_match.group(0)

    else:

        country_flag = "🌐"

    # Convert Mbps to MB/s.
    speed_mb = max(
        0.0,
        speed_mbps / 8,
    )

    speed_label = (
        f"{speed_mb:.2f} MB/s"
    )

    # Clean name: ONLY flag and speed.
    clean_name = (
        f"{country_flag} {speed_label}"
    )

    # Preserve connection parameters.
    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path,
            parts.query,
            clean_name,
        )
    )


# ============================================================
# Atomic file writing
# ============================================================

def write_file_atomic(
    file_path,
    content,
):
    """
    Write a file using a temporary file and atomic replacement.
    """

    file_path = Path(file_path)

    file_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_file = None

    try:

        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            dir=file_path.parent,
            prefix=f".{file_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as file:

            temp_file = Path(file.name)

            file.write(content)

            file.flush()

            os.fsync(file.fileno())

        os.replace(
            temp_file,
            file_path,
        )

        temp_file = None

    finally:

        if (
            temp_file is not None
            and temp_file.exists()
        ):

            try:

                temp_file.unlink()

            except OSError:

                pass


# ============================================================
# Save results
# ============================================================

def save_results(valid_servers):
    """
    Save unique servers, sorted by speed.

    Latest:
        results/hysteria2.txt

    Backup:
        results/YYYY/MM/DD.txt
    """

    # Sort fastest first.
    valid_servers.sort(
        key=lambda item: item["speed_mbps"],
        reverse=True,
    )

    # Final deduplication before saving.
    # Since the list is sorted, the fastest duplicate
    # configuration is preserved.
    unique_servers = []

    seen = set()

    for server in valid_servers:

        uri = server["uri"]

        try:

            key = canonical_hysteria2_key(
                uri
            )

        except (
            ValueError,
            TypeError,
        ):

            key = normalize_uri(uri)

        if key in seen:
            continue

        seen.add(key)

        unique_servers.append(server)

    # Format output.
    output_lines = [

        format_server_uri(
            server["uri"],
            server["speed_mbps"],
        )

        for server in unique_servers
    ]

    content = "\n".join(output_lines)

    if content:
        content += "\n"

    # Ensure directories exist.
    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    BACKUP_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Save latest output.
    write_file_atomic(
        OUTPUT_FILE,
        content,
    )

    # Save daily backup.
    write_file_atomic(
        BACKUP_FILE,
        content,
    )

    log(
        f"[INFO] Saved {len(unique_servers)} "
        f"unique valid servers"
    )

    log(
        f"[INFO] Latest results: {OUTPUT_FILE}"
    )

    log(
        f"[INFO] Daily backup: {BACKUP_FILE}"
    )

    log(
        f"[INFO] Output size: "
        f"{len(content.encode('utf-8')):,} bytes"
    )


# ============================================================
# Main
# ============================================================

def main():

    if shutil.which(SING_BOX) is None:

        log(
            "[ERROR] sing-box executable not found"
        )

        sys.exit(1)

    if shutil.which(CURL) is None:

        log(
            "[ERROR] curl executable not found"
        )

        sys.exit(1)

    if not SOURCES_FILE.exists():

        log(
            f"[ERROR] Sources file not found: "
            f"{SOURCES_FILE}"
        )

        sys.exit(1)

    # Collect and deduplicate servers.
    servers = collect_servers()

    if not servers:

        log(
            "[WARNING] No Hysteria2 servers found"
        )

        save_results([])

        return

    # Test unique servers.
    valid_servers = test_servers(
        servers
    )

    # Save clean output and backup.
    save_results(
        valid_servers
    )

    log("")

    log(
        "===================================="
    )

    log(
        "Hysteria2 testing completed"
    )

    log(
        f"Total unique servers: {len(servers)}"
    )

    log(
        f"Valid servers: {len(valid_servers)}"
    )

    log(
        f"Invalid servers: "
        f"{len(servers) - len(valid_servers)}"
    )

    log(
        "===================================="
    )


if __name__ == "__main__":

    main()
