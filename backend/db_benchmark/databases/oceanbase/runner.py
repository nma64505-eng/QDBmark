from __future__ import annotations

from typing import Optional

from db_benchmark.benchmark import BenchmarkConfig, BenchmarkSummary, CancelCallback, LogCallback
from db_benchmark.benchmark import _check_oceanbase_connectivity, _run_oceanbase_benchmark


def run(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None, cancel_callback: Optional[CancelCallback] = None) -> BenchmarkSummary:
    return _run_oceanbase_benchmark(config, log_callback=log_callback, cancel_callback=cancel_callback)


def check_connectivity(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    _check_oceanbase_connectivity(config, log_callback=log_callback)
