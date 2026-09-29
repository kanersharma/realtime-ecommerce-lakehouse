"""scripts/ports.py: configurable host ports, and moving ours when another program has one."""
import re
import socket

import pytest

import ports
from conftest import ROOT


@pytest.fixture
def env_file(tmp_path, monkeypatch):
    path = tmp_path / ".env"
    monkeypatch.setattr(ports, "ENV_FILE", path)
    for name in ports.PORTS:
        monkeypatch.delenv(name, raising=False)
    return path


def test_the_environment_beats_env_file_beats_the_default(env_file, monkeypatch):
    assert ports.host_port("SHOP_PORT") == 8000
    env_file.write_text("# comment\nSHOP_PORT=18000\n", encoding="utf-8")
    assert ports.host_port("SHOP_PORT") == 18000
    monkeypatch.setenv("SHOP_PORT", "28000")
    assert ports.host_port("SHOP_PORT") == 28000


def test_a_busy_port_moves_to_the_next_free_one_without_taking_another_services():
    current = {"FLINK_PORT": 8081, "REGISTRY_PORT": 8085, "KAFKA_UI_PORT": 8088}
    busy = {8081, 8082, 8085, 8086, 8087}
    chosen = ports.plan(current, free=lambda p: p not in busy)
    assert chosen == {"FLINK_PORT": 8083, "REGISTRY_PORT": 8089, "KAFKA_UI_PORT": 8088}  # 8088 stays Kafka UI's


def test_free_ports_are_kept():
    current = {name: default for name, (default, _) in ports.PORTS.items()}
    assert ports.plan(current, free=lambda p: True) == current


def test_env_file_keeps_everything_else(env_file):
    env_file.write_text("# my settings\nSESSIONS_PER_SEC=5\nSHOP_PORT=8000\n", encoding="utf-8")
    ports.write_env({"SHOP_PORT": 8001, "TRINO_PORT": 8091})
    assert env_file.read_text(encoding="utf-8") == "# my settings\nSESSIONS_PER_SEC=5\nSHOP_PORT=8001\nTRINO_PORT=8091\n"


def test_env_example_style_comments_are_understood_and_kept(env_file):
    """.env.example has inline comments; Docker Compose ignores them, so we must too."""
    env_file.write_text((ROOT / ".env.example").read_text(encoding="utf-8"), encoding="utf-8")
    assert ports.host_port("SHOP_PORT") == 8000 and ports.host_port("KAFKA_PORT") == 29092
    ports.write_env({"SHOP_PORT": 8001})
    assert "SHOP_PORT=8001            # Lakeshop store and /admin.html" in env_file.read_text(encoding="utf-8")
    assert ports.host_port("SHOP_PORT") == 8001


def test_check_reports_and_the_default_mode_moves_our_port(env_file, monkeypatch, capsys):
    monkeypatch.setattr(ports, "stack_running", lambda: False)
    monkeypatch.setattr(ports, "is_free", lambda p: p != 8000)
    assert ports.main(["--check"]) == 1 and not env_file.exists()      # --check changes nothing
    assert "port 8000 (Lakeshop store (+ /admin.html)) is in use" in capsys.readouterr().out
    assert ports.main([]) == 0
    assert env_file.read_text(encoding="utf-8") == "SHOP_PORT=8001\n"
    assert "http://localhost:8001/admin.html" in capsys.readouterr().out


def test_a_running_stack_is_left_alone(env_file, monkeypatch):
    monkeypatch.setattr(ports, "stack_running", lambda: True)
    monkeypatch.setattr(ports, "is_free", lambda p: False)               # its own containers hold them
    assert ports.main([]) == 0 and not env_file.exists()


def test_is_free_sees_a_real_listener():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        s.listen()
        port = s.getsockname()[1]
        assert not ports.is_free(port)


def test_every_published_port_is_configurable_in_compose():
    """No host port may be hard-coded: each one is ${VAR:-default}, with VAR and default from ports.PORTS."""
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    published = re.findall(r'"(\S+?):\d+"', "\n".join(line for line in compose.splitlines()
                                                      if line.strip().startswith("ports:")))
    assert published and all(re.fullmatch(r"\$\{[A-Z0-9_]+:-\d+\}", p) for p in published), published
    used = dict(re.findall(r"\$\{([A-Z0-9_]+_PORT):-(\d+)\}", compose))
    assert {name: int(port) for name, port in used.items()} == {n: d for n, (d, _) in ports.PORTS.items()}
