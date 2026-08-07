from __future__ import annotations

from typing import Optional

from db_benchmark.benchmark import (
    BenchmarkConfig,
    BenchmarkSummary,
    CancelCallback,
    LogCallback,
    _check_gaussdb_connectivity,
    _run_gaussdb_benchmark,
)


def run(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> BenchmarkSummary:
    return _run_gaussdb_benchmark(config, log_callback=log_callback, cancel_callback=cancel_callback)


def check_connectivity(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    _check_gaussdb_connectivity(config, log_callback=log_callback)
