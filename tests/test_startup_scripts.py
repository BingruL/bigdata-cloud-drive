import os
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_full_startup_scripts_exist_and_are_executable():
    for rel_path in (
        "scripts/start_full.sh",
        "scripts/status_full.sh",
        "scripts/stop_full.sh",
    ):
        path = ROOT / rel_path
        assert path.exists(), f"{rel_path} should exist"
        assert os.access(path, os.X_OK), f"{rel_path} should be executable"


def test_full_startup_scripts_are_valid_bash():
    for rel_path in (
        "scripts/start_full.sh",
        "scripts/status_full.sh",
        "scripts/stop_full.sh",
    ):
        path = ROOT / rel_path
        result = subprocess.run(
            ["bash", "-n", str(path)],
            cwd=ROOT,
            text=True,
            capture_output=True,
        )
        assert result.returncode == 0, result.stderr


def test_start_full_script_launches_complete_runtime_stack():
    content = (ROOT / "scripts/start_full.sh").read_text()

    assert "start-dfs.sh" in content
    assert "start-hbase.sh" in content
    assert "hbase thrift start" in content
    assert "docker start cloud-drive-kafka" in content
    assert "apache/kafka:3.7.2" in content
    assert "KAFKA_ENABLED=1" in content
    assert "python run.py" in content
    assert "backend.workers.log_consumer" in content
    assert "spark-submit" in content
    assert "spark-sql-kafka-0-10_2.12:3.5.0" in content


def test_stop_full_script_does_not_stop_hdfs_or_hbase_by_default():
    content = (ROOT / "scripts/stop_full.sh").read_text()

    assert "stop-dfs.sh" not in content
    assert "stop-hbase.sh" not in content
    assert "docker stop cloud-drive-kafka" in content


def test_makefile_exposes_short_startup_commands():
    content = (ROOT / "Makefile").read_text()

    assert "start-full:" in content
    assert "status-full:" in content
    assert "stop-full:" in content
