from __future__ import annotations

from typing import Any, Dict, Optional

from db_benchmark.benchmark import (
    BenchmarkConfig,
    BenchmarkSummary,
    BenchmarkValidationError,
    CancelCallback,
    LogCallback,
    _emit_log,
    _fio_display_target,
    _mongo_display_target,
    _elasticsearch_target_hosts,
    _gaussdb_target_hosts,
    _rocketmq_namesrv,
    _rabbitmq_broker,
    cleanup_benchmark_data as legacy_cleanup_benchmark_data,
    collect_instance_info as legacy_collect_instance_info,
    collect_runtime_sample as legacy_collect_runtime_sample,
    format_runtime_metrics as legacy_format_runtime_metrics,
)

from .dm import runner as dm
from .clickhouse import runner as clickhouse
from .elasticsearch import runner as elasticsearch
from .fio import runner as fio
from .gaussdb import runner as gaussdb
from .kafka import runner as kafka
from .kingbase import runner as kingbase
from .mongodb import runner as mongodb
from .mysql import runner as mysql
from .oceanbase import runner as oceanbase
from .oracle import runner as oracle
from .postgres import runner as postgres
from .redis import runner as redis
from .rocketmq import runner as rocketmq
from .rabbitmq import runner as rabbitmq
from .sqlserver import runner as sqlserver
from .tidb import runner as tidb
from .vastbase import runner as vastbase


RUNNERS = {
    "MySQL": mysql,
    "TiDB": tidb,
    "MongoDB": mongodb,
    "ElasticSearch": elasticsearch,
    "ClickHouse": clickhouse,
    "Redis": redis,
    "Oracle": oracle,
    "SQL Server": sqlserver,
    "DM": dm,
    "OceanBase": oceanbase,
    "Kafka": kafka,
    "RocketMQ": rocketmq,
    "RabbitMQ": rabbitmq,
    "KingBase": kingbase,
    "PostgreSQL": postgres,
    "GaussDB": gaussdb,
    "OpenGauss": postgres,
    "Vastbase": vastbase,
    "FIO": fio,
}


def _runner_for(config: BenchmarkConfig):
    runner = RUNNERS.get(config.db_type)
    if runner is None:
        raise BenchmarkValidationError(
            f"当前版本仅支持 MySQL / TiDB / SQL Server / Oracle / PostgreSQL / GaussDB / OpenGauss / DM / OceanBase / KingBase / Vastbase / MongoDB / Redis / ElasticSearch / ClickHouse / Kafka / RocketMQ / RabbitMQ / FIO 压测执行，已选择 {config.db_type}。"
        )
    return runner


def run_benchmark(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> BenchmarkSummary:
    config.validate()
    return _runner_for(config).run(config, log_callback=log_callback, cancel_callback=cancel_callback)


def run_connectivity_check(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    config.validate()
    connectivity_target = (
        _fio_display_target(config)
        if config.db_type == "FIO"
        else _mongo_display_target(config)
        if config.db_type == "MongoDB"
        else _elasticsearch_target_hosts(config)
        if config.db_type == "ElasticSearch"
        else f"{config.kafka_bootstrap_servers or config.host + ':' + str(config.port)}/{config.database}"
        if config.db_type == "Kafka"
        else f"{_rocketmq_namesrv(config)}/{config.database}"
        if config.db_type == "RocketMQ"
        else f"{_rabbitmq_broker(config)}/{config.database}"
        if config.db_type == "RabbitMQ"
        else f"{config.host}:{config.port}/{config.database or 'default'}"
        if config.db_type == "ClickHouse"
        else _gaussdb_target_hosts(config) + f"/{config.database}"
        if config.db_type == "GaussDB"
        else f"{config.host}:{config.port}/{config.database}"
    )
    _emit_log(log_callback, f"已接收连通性测试: {config.db_type} / {connectivity_target}")
    _runner_for(config).check_connectivity(config, log_callback=log_callback)


def cleanup_benchmark_data(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> None:
    legacy_cleanup_benchmark_data(config, log_callback=log_callback, cancel_callback=cancel_callback)


def collect_instance_info(config: BenchmarkConfig) -> Optional[Dict[str, Any]]:
    return legacy_collect_instance_info(config)


def collect_runtime_sample(config: BenchmarkConfig) -> Optional[Dict[str, Any]]:
    return legacy_collect_runtime_sample(config)


def format_runtime_metrics(config: BenchmarkConfig, current: Optional[Dict[str, Any]], previous: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    return legacy_format_runtime_metrics(config, current, previous)
