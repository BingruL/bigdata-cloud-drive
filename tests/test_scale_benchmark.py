import importlib.util
from pathlib import Path
import random


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "scale_benchmark", ROOT / "scripts" / "scale_benchmark.py"
)
scale_benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(scale_benchmark)


def test_parse_size_supports_common_units():
    assert scale_benchmark.parse_size("1GB") == 1024 ** 3
    assert scale_benchmark.parse_size("1.5MB") == int(1.5 * 1024 ** 2)
    assert scale_benchmark.parse_size("2048") == 2048


def test_distribute_sizes_preserves_total():
    rng = random.Random(7)
    sizes = scale_benchmark.distribute_sizes(1024 * 1024 * 1024, 1000, rng)

    assert len(sizes) == 1000
    assert sum(sizes) == 1024 * 1024 * 1024
    assert all(size > 0 for size in sizes)


def test_file_meta_row_contains_benchmark_columns():
    rng = random.Random(9)
    file_id, row = scale_benchmark.file_meta_row(
        "demo", 3, "alice", 4096, rng, 1710000000000, "scale_group_demo"
    )

    assert file_id == "scale_demo_00000003"
    assert row[b"meta:owner"] == b"alice"
    assert row[b"meta:size"] == b"4096"
    assert row[b"meta:benchmark_label"] == b"demo"
    assert b"meta:tags" in row
