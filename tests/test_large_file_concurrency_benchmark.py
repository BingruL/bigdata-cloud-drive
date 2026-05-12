import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "large_file_concurrency_benchmark",
    ROOT / "scripts" / "large_file_concurrency_benchmark.py",
)
large_bench = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(large_bench)


def test_parse_size_supports_large_file_units():
    assert large_bench.parse_size("1GB") == 1024 ** 3
    assert large_bench.parse_size("1.5GB") == int(1.5 * 1024 ** 3)
    assert large_bench.parse_size("64MB") == 64 * 1024 ** 2


def test_summarize_reports_error_rate_and_latency():
    results = [
        large_bench.TaskResult("upload", "u1", "a.bin", bytes=100, seconds=2.0, ok=True),
        large_bench.TaskResult("upload", "u2", "b.bin", bytes=100, seconds=4.0, ok=True),
        large_bench.TaskResult("upload", "u3", "c.bin", bytes=100, seconds=1.0, ok=False),
    ]

    summary = large_bench.summarize("upload", results)

    assert summary["total"] == 3
    assert summary["success"] == 2
    assert summary["errors"] == 1
    assert summary["error_rate_pct"] == 33.33
    assert summary["bytes"] == 200
