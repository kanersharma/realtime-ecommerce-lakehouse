"""The stack's host ports: check them before the first run, and move ours if one is taken.

    python scripts/ports.py           # busy port? pick the next free one, write it to .env, print the URLs
    python scripts/ports.py --check   # only report; exit 1 if a port is taken by something else

Docker Compose reads `.env` automatically, so `docker compose up -d --build` then uses those ports.
Never stop another program to free a port: move this stack instead. That's what this does.
Standard library only (Python 3.8+), on Windows, macOS and Linux.
"""
import os
import re
import socket
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = ROOT / ".env"
PORTS = {  # .env variable: (default host port, what it serves)
    "SHOP_PORT": (8000, "Lakeshop store (+ /admin.html)"),
    "DASHBOARD_PORT": (8501, "Dashboard"),
    "FLINK_PORT": (8081, "Flink UI"),
    "REGISTRY_PORT": (8085, "Schema Registry API"),
    "KAFKA_UI_PORT": (8088, "Kafka UI"),
    "TRINO_PORT": (8090, "Trino"),
    "ICEBERG_REST_PORT": (8181, "Iceberg REST catalog"),
    "S3_PORT": (9000, "RustFS S3 API"),
    "S3_CONSOLE_PORT": (9001, "RustFS console"),
    "KAFKA_PORT": (29092, "Kafka (host listener)"),
}


def read_env(path=None):
    path = Path(path or ENV_FILE)
    if not path.exists():
        return {}
    pairs = (line.split("=", 1) for line in path.read_text(encoding="utf-8").splitlines()
             if "=" in line and not line.lstrip().startswith("#"))
    return {k.strip(): re.split(r"\s+#", v, maxsplit=1)[0].strip() for k, v in pairs}  # drop `  # comment`


def host_port(name, env_file=None):
    """Where a service is reachable from this machine: the environment, then .env, then the default."""
    return int(os.environ.get(name) or read_env(env_file).get(name) or PORTS[name][0])


def is_free(port):
    """Nothing listens on it (a connect fails) and it can be bound (Docker publishes on all interfaces)."""
    with socket.socket() as s:
        s.settimeout(0.5)
        if s.connect_ex(("127.0.0.1", port)) == 0:
            return False
    try:
        with socket.socket() as s:
            s.bind(("0.0.0.0", port))
        return True
    except OSError:
        return False


def plan(current, free=None):
    """{name: port}: keep each port if free, else the next free one that no other service uses."""
    free, chosen = free or is_free, {}
    for name, port in current.items():
        avoid = set(chosen.values()) | {p for n, p in current.items() if n != name and n not in chosen}
        while port in avoid or not free(port):
            port += 1
        chosen[name] = port
    return chosen


def write_env(changes, env_file=None):
    """Set these variables in .env, keeping every other line (comments, other settings) as it was."""
    path = Path(env_file or ENV_FILE)
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    todo = dict(changes)
    for i, line in enumerate(lines):
        key = line.split("=", 1)[0].strip()
        if "=" in line and key in todo:
            comment = re.search(r"\s+#.*$", line.split("=", 1)[1])
            lines[i] = f"{key}={todo.pop(key)}{comment.group(0) if comment else ''}"
    lines += [f"{k}={v}" for k, v in todo.items()]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def stack_running():
    try:
        out = subprocess.run(["docker", "compose", "ps", "-q", "--status", "running"], cwd=ROOT,
                             capture_output=True, text=True, timeout=30).stdout
        return bool(out.strip())
    except (OSError, subprocess.SubprocessError):
        return False


def urls(ports):
    p = ports
    return [("Store", f"http://localhost:{p['SHOP_PORT']}"),
            ("Catalog admin", f"http://localhost:{p['SHOP_PORT']}/admin.html"),
            ("Dashboard", f"http://localhost:{p['DASHBOARD_PORT']}"),
            ("Flink UI", f"http://localhost:{p['FLINK_PORT']}"),
            ("Kafka UI", f"http://localhost:{p['KAFKA_UI_PORT']}"),
            ("Trino", f"http://localhost:{p['TRINO_PORT']}"),
            ("Schema Registry", f"http://localhost:{p['REGISTRY_PORT']}/subjects"),
            ("RustFS console", f"http://localhost:{p['S3_CONSOLE_PORT']} (admin / password)")]


def main(argv):
    current = {name: host_port(name) for name in PORTS}
    if stack_running():  # the ports are (most likely) ours: don't move a running stack
        print("The stack is running; it uses:")
    else:
        chosen = plan(current)
        moved = {n: p for n, p in chosen.items() if p != current[n]}
        for name, port in moved.items():
            print(f"port {current[name]} ({PORTS[name][1]}) is in use by another program -> {name}={port}")
        if moved and "--check" in argv:
            print("Run `python scripts/ports.py` to move them (writes .env), then `docker compose up -d --build`.")
            return 1
        if moved:
            write_env(moved)
            print(f"Wrote {ENV_FILE.name}. `docker compose up -d --build` will use these ports.")
        else:
            print("All ports are free.")
        current = chosen
    width = max(len(label) for label, _ in urls(current))
    for label, url in urls(current):
        print(f"  {label:<{width}}  {url}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
