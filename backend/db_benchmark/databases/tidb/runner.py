from __future__ import annotations

from typing import Optional

from db_benchmark.benchmark import BenchmarkConfig, BenchmarkSummary, CancelCallback, LogCallback
from db_benchmark.benchmark import (
    _check_mysql_connectivity,
    _run_mysql_benchmark,
    _run_tidb_tiup_tpcc_benchmark,
)


def run(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None, cancel_callback: Optional[CancelCallback] = None) -> BenchmarkSummary:
    if config.tidb_engine == "tiup_tpcc":
        return _run_tidb_tiup_tpcc_benchmark(config, log_callback=log_callback, cancel_callback=cancel_callback)
    return _run_mysql_benchmark(config, log_callback=log_callback, cancel_callback=cancel_callback)


def check_connectivity(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    _check_mysql_connectivity(config, log_callback=log_callback)
