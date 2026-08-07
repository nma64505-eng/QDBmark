from __future__ import annotations

from typing import Optional

from db_benchmark.benchmark import (
    BenchmarkConfig,
    BenchmarkSummary,
    CancelCallback,
    LogCallback,
    _check_rocketmq_connectivity,
    _run_rocketmq_benchmark,
)


def check_connectivity(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    _check_rocketmq_connectivity(config, log_callback=log_callback)


def run(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> BenchmarkSummary:
    return _run_rocketmq_benchmark(config, log_callback=log_callback, cancel_callback=cancel_callback)
