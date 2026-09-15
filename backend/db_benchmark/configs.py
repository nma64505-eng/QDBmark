from __future__ import annotations

import shlex
from typing import Any, Dict, List

from db_benchmark.benchmark import BenchmarkConfig, BenchmarkValidationError


DM_TUNING_EXTRA_OPTIONS = {"dm-apply-tuning", "dm_apply_tuning"}
OCEANBASE_APPLY_TUNING_OPTIONS = {"ob-apply-tuning", "ob_apply_tuning"}
OCEANBASE_WAIT_MAJOR_FREEZE_OPTIONS = {"ob-wait-major-freeze", "ob_wait_major_freeze"}
OCEANBASE_GATHER_STATS_OPTIONS = {"ob-gather-stats", "ob_gather_stats"}
OCEANBASE_BOOLEAN_EXTRA_OPTIONS = (
    OCEANBASE_APPLY_TUNING_OPTIONS
    | OCEANBASE_WAIT_MAJOR_FREEZE_OPTIONS
    | OCEANBASE_GATHER_STATS_OPTIONS
)

ORACLE_ONLY_EXTRA_OPTIONS = {
    "tpcc-user",
    "tpcc-pass",
    "tpcc_user",
    "tpcc_pass",
    "oracle-common-config",
    "oracle_common_config",
    "oracle-shutdown-strict",
    "oracle_shutdown_strict",
    "build-vu",
    "build_vu",
    "warehouses",
    "connect",
    "tpcc-tablespace-size-gb",
    "tpcc_tablespace_size_gb",
}


class MySQLConfig(BenchmarkConfig):
    pass


class TiDBConfig(BenchmarkConfig):
    pass


class PostgreSQLConfig(BenchmarkConfig):
    pass


class GaussDBConfig(BenchmarkConfig):
    pass


class VastbaseConfig(BenchmarkConfig):
    pass


class OpenGaussConfig(BenchmarkConfig):
    pass


class OracleConfig(BenchmarkConfig):
    pass


class SQLServerConfig(BenchmarkConfig):
    pass


class DMConfig(BenchmarkConfig):
    pass


class OceanBaseConfig(BenchmarkConfig):
    pass


class KingBaseConfig(BenchmarkConfig):
    pass


class RedisConfig(BenchmarkConfig):
    pass


class KafkaConfig(BenchmarkConfig):
    pass


class RocketMQConfig(BenchmarkConfig):
    pass


class RabbitMQConfig(BenchmarkConfig):
    pass


class MongoDBConfig(BenchmarkConfig):
    pass


class ElasticSearchConfig(BenchmarkConfig):
    pass


class ClickHouseConfig(BenchmarkConfig):
    pass


class FIOConfig(BenchmarkConfig):
    pass


def parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _split_cli_option_name(token: str) -> tuple[str, bool]:
    option = token[2:] if token.startswith("--") else token
    if "=" in option:
        return option.split("=", 1)[0], True
    return option, False


def cli_option_value(tokens: List[str], option_names: set[str]) -> str | None:
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if not token.startswith("--"):
            index += 1
            continue
        option_name, has_inline_value = _split_cli_option_name(token)
        if option_name in option_names:
            if has_inline_value:
                return token.split("=", 1)[1]
            if index + 1 < len(tokens) and not tokens[index + 1].startswith("--"):
                return tokens[index + 1]
            return "true"
        index += 1
    return None


def cli_option_enabled(tokens: List[str], option_names: set[str], default: bool = False) -> bool:
    value = cli_option_value(tokens, option_names)
    if value is None:
        return default
    return parse_bool(value, True)


def strip_cli_options(tokens: List[str], option_names: set[str]) -> List[str]:
    filtered: List[str] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token.startswith("--"):
            option_name, has_inline_value = _split_cli_option_name(token)
            if option_name in option_names:
                if not has_inline_value and index + 1 < len(tokens) and not tokens[index + 1].startswith("--"):
                    index += 2
                else:
                    index += 1
                continue
        filtered.append(token)
        index += 1
    return filtered


def strip_extra_options(raw: str, option_names: set[str]) -> str:
    tokens = shlex.split(raw) if raw else []
    tokens = strip_cli_options(tokens, option_names)
    return shlex.join(tokens) if tokens else ""



def _oceanbase_tenant_from_user(user: str) -> str:
    user = str(user or "").strip()
    if "@" not in user:
        return ""
    tenant = user.split("@", 1)[1]
    tenant = tenant.split("#", 1)[0]
    tenant = tenant.split(":", 1)[0]
    return tenant.strip()


def _oceanbase_base_user(user: str) -> str:
    user = str(user or "").strip()
    if not user:
        return "root"
    if "@" in user:
        return user.split("@", 1)[0].strip() or "root"
    return user


def _oceanbase_tenant_from_payload(payload: Dict[str, Any]) -> str:
    tenant = str(payload.get("ob_tenant_name") or payload.get("tenant_name") or "").strip()
    if tenant:
        return tenant
    return _oceanbase_tenant_from_user(str(payload.get("db_user", "")))


def _oceanbase_effective_user(payload: Dict[str, Any], tenant: str) -> str:
    base_user = _oceanbase_base_user(str(payload.get("db_user", "")))
    return f"{base_user}@{tenant}" if tenant else base_user

def _parse_threads(payload: Dict[str, Any]) -> List[int]:
    concurrency_raw = str(payload.get("concurrency_values", "")).strip()
    if not concurrency_raw:
        raise BenchmarkValidationError("请至少填写一个线程值，例如 1,5,10。")

    concurrency_values: List[int] = []
    for item in concurrency_raw.split(","):
        value = item.strip()
        if not value:
            continue
        try:
            concurrency_values.append(int(value))
        except ValueError as exc:
            raise BenchmarkValidationError("线程值必须是整数，多个值用英文逗号分隔。") from exc
    return concurrency_values


def _payload_or_cli_enabled(payload: Dict[str, Any], field_name: str, option_names: set[str], tokens: List[str]) -> bool:
    dashed_field_name = field_name.replace("_", "-")
    enabled = parse_bool(payload.get(field_name), False)
    if not enabled and field_name not in payload and dashed_field_name not in payload:
        enabled = cli_option_enabled(tokens, option_names, False)
    return enabled


def _normalize_extra_options(db_type: str, payload: Dict[str, Any]) -> str:
    raw = str(payload.get("extra_options", "")).strip()
    extra_options_parts = shlex.split(raw) if raw else []
    dm_apply_tuning = _payload_or_cli_enabled(payload, "dm_apply_tuning", DM_TUNING_EXTRA_OPTIONS, extra_options_parts)
    ob_apply_tuning = _payload_or_cli_enabled(payload, "ob_apply_tuning", OCEANBASE_APPLY_TUNING_OPTIONS, extra_options_parts)
    ob_wait_major_freeze = _payload_or_cli_enabled(payload, "ob_wait_major_freeze", OCEANBASE_WAIT_MAJOR_FREEZE_OPTIONS, extra_options_parts)
    ob_gather_stats = _payload_or_cli_enabled(payload, "ob_gather_stats", OCEANBASE_GATHER_STATS_OPTIONS, extra_options_parts)

    if db_type == "Oracle":
        extra_options_parts = strip_cli_options(extra_options_parts, {"tpcc-user", "tpcc-pass", "tpcc_user", "tpcc_pass"})
    else:
        extra_options_parts = strip_cli_options(extra_options_parts, ORACLE_ONLY_EXTRA_OPTIONS)

    extra_options_parts = strip_cli_options(extra_options_parts, DM_TUNING_EXTRA_OPTIONS)
    extra_options_parts = strip_cli_options(extra_options_parts, OCEANBASE_BOOLEAN_EXTRA_OPTIONS)
    if db_type == "DM" and dm_apply_tuning:
        extra_options_parts.extend(["--dm-apply-tuning", "true"])
    if db_type == "OceanBase":
        if ob_apply_tuning:
            extra_options_parts.extend(["--ob-apply-tuning", "true"])
        if ob_wait_major_freeze:
            extra_options_parts.extend(["--ob-wait-major-freeze", "true"])
        if ob_gather_stats:
            extra_options_parts.extend(["--ob-gather-stats", "true"])
    return shlex.join(extra_options_parts) if extra_options_parts else ""


def build_config_from_payload(payload: Dict[str, Any]) -> BenchmarkConfig:
    db_type = str(payload.get("db_type", "MySQL")).strip() or "MySQL"
    concurrency_values = _parse_threads(payload)
    normalized_extra_options = _normalize_extra_options(db_type, payload)
    oceanbase_tenant_name = _oceanbase_tenant_from_payload(payload) if db_type == "OceanBase" else ""
    effective_db_user = _oceanbase_effective_user(payload, oceanbase_tenant_name) if db_type == "OceanBase" else str(payload.get("db_user", "")).strip()
    tidb_engine = str(payload.get("tidb_engine", "sysbench")).strip() or "sysbench"

    tidb_defaults = db_type == "TiDB"
    tidb_tpcc_defaults = tidb_defaults and tidb_engine == "tiup_tpcc"
    clickhouse_defaults = db_type == "ClickHouse"
    kafka_defaults = db_type == "Kafka"
    rocketmq_defaults = db_type == "RocketMQ"
    rabbitmq_defaults = db_type == "RabbitMQ"
    message_queue_defaults = kafka_defaults or rocketmq_defaults or rabbitmq_defaults
    default_report_interval = 1 if (tidb_defaults or clickhouse_defaults or message_queue_defaults) else 2
    default_table_count = 12 if kafka_defaults else (0 if (rocketmq_defaults or rabbitmq_defaults) else (1 if clickhouse_defaults else (10 if tidb_tpcc_defaults else 250)))
    default_table_size = 1024 if message_queue_defaults else (1000000 if clickhouse_defaults else (200 if tidb_tpcc_defaults else (260000 if tidb_defaults else 10000)))
    default_workload = "producer" if message_queue_defaults else ("count" if clickhouse_defaults else ("tprocc" if tidb_tpcc_defaults else "oltp_read_only"))
    default_operation_count = 1000000 if message_queue_defaults else 100000

    config_class = {
        "MySQL": MySQLConfig,
        "TiDB": TiDBConfig,
        "PostgreSQL": PostgreSQLConfig,
        "GaussDB": GaussDBConfig,
        "OpenGauss": OpenGaussConfig,
        "Vastbase": VastbaseConfig,
        "Oracle": OracleConfig,
        "SQL Server": SQLServerConfig,
        "DM": DMConfig,
        "OceanBase": OceanBaseConfig,
        "KingBase": KingBaseConfig,
        "Redis": RedisConfig,
        "Kafka": KafkaConfig,
        "RocketMQ": RocketMQConfig,
        "RabbitMQ": RabbitMQConfig,
        "MongoDB": MongoDBConfig,
        "ElasticSearch": ElasticSearchConfig,
        "ClickHouse": ClickHouseConfig,
        "FIO": FIOConfig,
    }.get(db_type, BenchmarkConfig)

    return config_class(
        db_type=db_type,
        host=str(payload.get("db_host", "")).strip(),
        port=int(payload.get("db_port", 5672 if db_type == "RabbitMQ" else (9876 if db_type == "RocketMQ" else (9092 if db_type == "Kafka" else 3306))) or (5672 if db_type == "RabbitMQ" else (9876 if db_type == "RocketMQ" else (9092 if db_type == "Kafka" else 3306)))),
        user=("default" if db_type == "ClickHouse" and not effective_db_user else effective_db_user),
        password=str(payload.get("db_password", "")),
        database=(str(payload.get("db_name", "")).strip() or ("default" if db_type == "ClickHouse" else ("qdbmark-perf-test" if db_type in {"Kafka", "RocketMQ", "RabbitMQ"} else ""))),
        workload=str(payload.get("workload", default_workload)).strip(),
        threads_values=concurrency_values,
        mongo_topology=str(payload.get("mongo_topology", "standalone")).strip() or "standalone",
        mongo_hosts=str(payload.get("mongo_hosts", "")).strip(),
        mongo_replica_set=str(payload.get("mongo_replica_set", "")).strip(),
        mongo_read_preference=str(payload.get("mongo_read_preference", "primary")).strip() or "primary",
        mongo_shard_key=str(payload.get("mongo_shard_key", "_id")).strip() or "_id",
        auth_database=str(payload.get("auth_database", "")).strip() or str(payload.get("db_name", "")).strip(),
        collection_name=str(payload.get("collection_name", "")).strip() or "test",
        operation_count=int(payload.get("operation_count", default_operation_count) or default_operation_count),
        redis_data_size=int(payload.get("redis_data_size", 128) or 128),
        kafka_bootstrap_servers=str(payload.get("kafka_bootstrap_servers", "")).strip(),
        kafka_acks=str(payload.get("kafka_acks", "1")).strip() or "1",
        kafka_throughput=int(payload.get("kafka_throughput", -1) or -1),
        kafka_replication_factor=int(payload.get("kafka_replication_factor", 1) or 1),
        es_pipeline=str(payload.get("es_pipeline", "benchmark-only")).strip() or "benchmark-only",
        es_challenge=str(payload.get("es_challenge", "")).strip(),
        es_ingest_percentage=float(payload.get("es_ingest_percentage", 100) or 100),
        es_track_params=str(payload.get("es_track_params", "")).strip(),
        gaussdb_architecture=str(payload.get("gaussdb_architecture", "centralized")).strip().lower() or "centralized",
        gaussdb_cn_hosts=str(payload.get("gaussdb_cn_hosts", "")).strip(),
        gaussdb_conn_params=str(payload.get("gaussdb_conn_params", "prepareThreshold=1&batchMode=on&fetchsize=10")).strip() or "prepareThreshold=1&batchMode=on&fetchsize=10",
        oceanbase_tenant_name=oceanbase_tenant_name,
        oracle_engine=str(payload.get("oracle_engine", "hammerdb")).strip() or "hammerdb",
        postgres_engine=str(payload.get("postgres_engine", "sysbench")).strip() or "sysbench",
        tidb_engine=tidb_engine,
        postgres_schema=str(payload.get("postgres_schema", "")).strip(),
        pgbench_scale=int(payload.get("pgbench_scale", payload.get("table_size", 200) or 200) or 200),
        pgbench_jobs=int(payload.get("pgbench_jobs", 16) or 16),
        pgbench_fillfactor=int(payload.get("pgbench_fillfactor", 80) or 80),
        pgbench_latency_limit=int(payload.get("pgbench_latency_limit", 10) or 10),
        fio_target_path=str(payload.get("fio_target_path", "")).strip() or "/tmp/fio-benchmark/testfile",
        fio_ssh_enabled=parse_bool(payload.get("fio_ssh_enabled"), False),
        fio_ssh_host=str(payload.get("fio_ssh_host", "")).strip(),
        fio_ssh_port=int(payload.get("fio_ssh_port", 22) or 22),
        fio_ssh_user=str(payload.get("fio_ssh_user", "")).strip(),
        fio_ssh_password=str(payload.get("fio_ssh_password", "")),
        fio_block_size=str(payload.get("fio_block_size", "")).strip() or "4k",
        fio_iodepth=int(payload.get("fio_iodepth", 32) or 32),
        fio_size_mb=int(payload.get("fio_size_mb", 1024) or 1024),
        fio_direct=parse_bool(payload.get("fio_direct"), True),
        fio_read_percent=int(payload.get("fio_read_percent", 50) or 50),
        prometheus_url=str(payload.get("prometheus_url", "")).strip(),
        prometheus_instance=str(payload.get("prometheus_instance", "")).strip(),
        prometheus_namespace=str(payload.get("prometheus_namespace", "")).strip() or "qfusion-admin",
        prometheus_pod=str(payload.get("prometheus_pod", "")).strip(),
        prometheus_container=str(payload.get("prometheus_container", "")).strip(),
        current_primary=str(payload.get("current_primary", "") or payload.get("prometheus_container", "")).strip(),
        duration_seconds=int(payload.get("duration_seconds", 60) or 60),
        warmup_seconds=int(payload.get("warmup_seconds", 300) or 0),
        report_interval=int(payload.get("report_interval", default_report_interval) or default_report_interval),
        table_count=int(payload.get("table_count", default_table_count) or default_table_count),
        table_size=int(payload.get("table_size", default_table_size) or default_table_size),
        extra_options=normalized_extra_options,
        ssl_enabled=parse_bool(payload.get("ssl_enabled"), False),
        ssl_verify=parse_bool(payload.get("ssl_verify"), False),
        ssl_ca_file=str(payload.get("ssl_ca_file", "")).strip(),
        auto_prepare=parse_bool(payload.get("auto_prepare"), True),
        auto_cleanup=parse_bool(payload.get("auto_cleanup"), False),
        report_title=str(payload.get("report_title", "")).strip() or "数据库性能测试报告",
        artifact_dir=str(payload.get("artifact_dir", "")).strip(),
        task_id=str(payload.get("qdbmark_job_id", "")).strip(),
    )
