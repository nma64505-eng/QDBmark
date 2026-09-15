from __future__ import annotations

import csv
import json
import os
import ipaddress
import re
import signal
import shlex
import socket
import ssl
import shutil
import subprocess
import tempfile
import urllib.error
import urllib.request
from queue import Empty, Queue
from threading import Lock, Thread
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from time import monotonic, sleep
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import quote, quote_plus, urlencode
from zoneinfo import ZoneInfo

from db_benchmark.prosql import (
    _prometheus_container_runtime_stats,
    _prometheus_database_runtime_sample,
    _prometheus_mysql_runtime_sample,
    _prometheus_postgresql_container_metrics,
)


ALLOWED_WORKLOADS = {
    "oltp_read_only",
    "oltp_read_write",
    "oltp_point_select",
    "oltp_update_index",
}
MONGO_ALLOWED_WORKLOADS = {
    "workloada",
    "workloadb",
    "workloadc",
    "workloadf",
}
MONGO_ALLOWED_TOPOLOGIES = {
    "standalone",
    "replica_set",
    "sharded",
}
MONGO_ALLOWED_READ_PREFERENCES = {
    "primary",
    "primaryPreferred",
    "secondary",
    "secondaryPreferred",
    "nearest",
}
REDIS_ALLOWED_WORKLOADS = {
    "set",
}
KAFKA_ALLOWED_WORKLOADS = {
    "producer",
    "consumer",
}
KAFKA_DEFAULT_TOPIC = "qdbmark-perf-test"
KAFKA_DEFAULT_PARTITIONS = 12
KAFKA_DEFAULT_RECORDS = 1000000
KAFKA_DEFAULT_RECORD_SIZE = 1024
KAFKA_MAX_CONCURRENCY = 256
ROCKETMQ_ALLOWED_WORKLOADS = {
    "producer",
    "consumer",
}
ROCKETMQ_DEFAULT_TOPIC = "qdbmark-perf-test"
ROCKETMQ_DEFAULT_MESSAGE_SIZE = 1024
ROCKETMQ_DEFAULT_MESSAGES = 1000000
ROCKETMQ_MAX_CONCURRENCY = 512
RABBITMQ_ALLOWED_WORKLOADS = {
    "producer",
    "consumer",
    "mixed",
}
RABBITMQ_DEFAULT_QUEUE = "qdbmark-perf-test"
RABBITMQ_DEFAULT_MESSAGE_SIZE = 1024
RABBITMQ_DEFAULT_MESSAGES = 1000000
RABBITMQ_MAX_CONCURRENCY = 512
CLICKHOUSE_ALLOWED_WORKLOADS = {
    "count",
    "filter",
    "aggregation",
    "group_by",
    "top_n",
}
CLICKHOUSE_DEFAULT_TABLE = "qdbmark_ck_events"
ELASTICSEARCH_ALLOWED_TRACKS = {
    "geonames",
    "http_logs",
    "nyc_taxis",
    "eventdata",
    "pmc",
    "so",
}
ELASTICSEARCH_ALLOWED_PIPELINES = {
    "benchmark-only",
}
ORACLE_ALLOWED_WORKLOADS = {
    "tprocc",
    "soe",
}
ORACLE_ALLOWED_ENGINES = {
    "hammerdb",
    "swingbench",
}
SQLSERVER_ALLOWED_WORKLOADS = {
    "tprocc",
}
DM_ALLOWED_WORKLOADS = {
    "tprocc",
}
OCEANBASE_ALLOWED_WORKLOADS = {
    "tprocc",
}
KINGBASE_ALLOWED_WORKLOADS = {
    "tprocc",
}
GAUSSDB_ALLOWED_WORKLOADS = {
    "tprocc",
}
GAUSSDB_ALLOWED_ARCHITECTURES = {
    "centralized",
    "distributed",
}
POSTGRES_DB_TYPES = {
    "PostgreSQL",
    "GaussDB",
    "OpenGauss",
    "Vastbase",
}
POSTGRES_ALLOWED_ENGINES = {
    "sysbench",
    "pgbench",
}
TIDB_ALLOWED_ENGINES = {
    "sysbench",
    "tiup_tpcc",
}
TIDB_TPCC_WORKLOADS = {
    "tpcc",
    "tpc-c",
    "tprocc",
}
POSTGRES_PGBENCH_WORKLOADS = {
    "tpcb_like",
}
MYSQL_SYSTEM_DATABASES = {
    "information_schema",
    "mysql",
    "performance_schema",
    "sys",
}
SUPPORTED_DB_TYPES = {
    "MySQL",
    "TiDB",
    "FIO",
    "SQL Server",
    "Oracle",
    "Redis",
    "MongoDB",
    "PostgreSQL",
    "GaussDB",
    "DM",
    "Vastbase",
    "OceanBase",
    "RabbitMQ",
    "OpenGauss",
    "RocketMQ",
    "KingBase",
    "Kafka",
    "ElasticSearch",
    "ClickHouse",
}
BASE_DIR = Path(__file__).resolve().parent
VENDORED_SYSBENCH_BIN = BASE_DIR / "vendor" / "sysbench" / "bin" / "sysbench"
VENDORED_SYSBENCH_SHARE = BASE_DIR / "vendor" / "sysbench" / "share" / "sysbench"
VENDORED_YCSB_DIR = BASE_DIR / "vendor" / "ycsb"
VENDORED_YCSB_BIN = VENDORED_YCSB_DIR / "bin" / "ycsb.sh"
DM_TUNING_INTEGER_PARAMETERS: Dict[str, int] = {
    "MAX_OS_MEMORY": 90,
    "MEMORY_POOL": 500,
    "BUFFER": 60000,
    "BUFFER_POOLS": 197,
    "FAST_POOL_PAGES": 99999,
    "FAST_ROLL_PAGES": 3000,
    "DICT_BUF_SIZE": 50,
    "RECYCLE": 8,
    "VM_POOL_SIZE": 512,
    "VM_POOL_TARGET": 32768,
    "SESS_POOL_SIZE": 1024,
    "SESS_POOL_TARGET": 32768,
    "N_MEM_POOLS": 100,
    "TRX_MODE": 1,
    "ENABLE_RQ_TO_NONREF_SPL": 3,
    "SORT_BUF_SIZE": 10,
    "SORT_BLK_SIZE": 1,
    "SORT_BUF_GLOBAL_SIZE": 100000,
    "SORT_FLAG": 1,
    "ADAPTIVE_NPLN_FLAG": 0,
    "HASH_PLL_OPT_FLAG": 2,
    "REFED_EXISTS_OPT_FLAG": 0,
    "PARTIAL_JOIN_EVALUATION_FLAG": 1,
    "USE_FK_REMOVE_TABLES_FLAG": 0,
    "SUBQ_EXP_CVT_FLAG": 0,
    "REFED_SUBQ_CROSS_FLAG": 0,
    "VIEW_FILTER_MERGING": 2,
    "UPD_DEL_OPT": 2,
    "LIKE_OPT_FLAG": 7,
    "GROUP_OPT_FLAG": 0,
    "HAGR_DISTINCT_OPT_FLAG": 0,
    "CASE_WHEN_CVT_IFUN": 5,
    "MAX_SESSIONS": 10000,
    "MAX_SESSION_STATEMENT": 20000,
    "FAST_LOGIN": 1,
    "BDTA_SIZE": 1000,
    "JOIN_HASH_SIZE": 5000,
    "CACHE_POOL_SIZE": 100,
    "PHC_MODE_ENFORCE": 11,
    "ENABLE_IN_VALUE_LIST_OPT": 1,
    "ENHANCED_BEXP_TRANS_GEN": 3,
    "ENABLE_SPACELIMIT_CHECK": 0,
    "FAST_RELEASE_SLOCK": 0,
    "SESS_CHECK_INTERVAL": 30,
    "NOWAIT_WHEN_UNIQUE_CONFLICT": 1,
    "MSG_COMPRESS_TYPE": 2,
    "COMM_VALIDATE": 0,
    "COMM_TRACE": 0,
    "OPTIMIZER_AGGR_GROUPBY_ELIM": 1,
    "FIRST_ROWS": 16,
    "BTR_SPLIT_MODE": 1,
    "DECIMAL_FIX_STORAGE": 1,
    "ENABLE_HUGE_SECIND": 0,
    "RS_PRE_FETCH": 0,
    "ENABLE_HASH_JOIN": 0,
    "ENABLE_MONITOR": 0,
    "MONITOR_TIME": 0,
    "ENABLE_FREQROOTS": 1,
    "CKPT_RLOG_SIZE": 0,
    "CKPT_DIRTY_PAGES": 0,
    "CKPT_INTERVAL": 600,
    "CKPT_FLUSH_PAGES": 1000,
    "CKPT_WAIT_PAGES": 512,
    "FORCE_FLUSH_PAGES": 0,
    "UNDO_EXTENT_NUM": 2,
    "PARALLEL_PURGE_FLAG": 1,
    "PURGE_WAIT_TIME": 0,
    "PSEG_RECV": 1,
    "WORKER_THREADS": 64,
    "TASK_THREADS": 16,
    "WORK_THRD_STACK_SIZE": 1024,
    "FAST_RW_LOCK": 2,
    "WORKER_CPU_PERCENT": 100,
    "DIRECT_IO": 1,
    "IO_THR_GROUPS": 32,
    "VM_MEM_HEAP": 1,
    "FAST_COMMIT": 99,
    "TRX_VIEW_MODE": 1,
    "PURGE_DEL_OPT": 2,
    "HASH_ACTIVE_TRX_VIEW": 0,
    "MAX_CONCURRENT_TRX": 0,
    "CONCURRENT_TRX_MODE": 0,
    "CONCURRENT_DELAY": 12,
    "RLOG_BUF_SIZE": 512,
    "RLOG_POOL_SIZE": 128,
    "RLOG_PARALLEL_ENABLE": 1,
    "RLOG_SAFE_SPACE": 128,
    "RLOG_CHECK_SPACE": 1,
    "REDO_PWR_OPT": 1,
    "RLOG_RESERVE_SIZE": 4096,
    "RLOG_RESERVE_THRESHOLD": 0,
}
DM_TUNING_DOUBLE_PARAMETERS: Dict[str, float] = {
    "CKPT_FLUSH_RATE": 5.0,
    "UNDO_RETENTION": 0.2,
}
LogCallback = Callable[[str], None]
CancelCallback = Callable[[], bool]
DOCKER_SOCKET_PATH = Path("/var/run/docker.sock")
DOCKER_CONTAINER_CACHE: Dict[str, Optional[Dict[str, Any]]] = {}
MYSQL_DATASET_CACHE: Dict[str, Dict[str, Any]] = {}
MYSQL_DATASET_CACHE_LOCK = Lock()
BEIJING_TZ = ZoneInfo("Asia/Shanghai")
ORACLE_CLIENT_INIT_ATTEMPTED = False
ORACLE_CLIENT_MODE = "thin"


def _beijing_now() -> datetime:
    return datetime.now(BEIJING_TZ)


class BenchmarkValidationError(ValueError):
    pass


class BenchmarkCancelledError(BenchmarkValidationError):
    pass


def _format_bytes(value: float) -> str:
    if value <= 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.2f} {unit}" if unit != "B" else f"{int(size)} {unit}"
        size /= 1024.0
    return f"{value:.2f} B"


def _format_mib(value: float) -> str:
    if value <= 0:
        return "0 MB"
    return f"{float(value):.2f} MB"


def _format_pct(numerator: float, denominator: float) -> str:
    if denominator <= 0:
        return "-"
    percent = _round_percent((numerator / denominator) * 100)
    return f"{percent}%" if percent is not None else "-"


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _clamp_percent(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    return max(0.0, min(numeric, 100.0))


def _round_percent(value: Any) -> Optional[float]:
    numeric = _clamp_percent(value)
    return round(numeric, 2) if numeric is not None else None


def _validate_ipv4_address(value: str, label: str = "数据库地址") -> None:
    try:
        parsed = ipaddress.ip_address(str(value).strip())
    except ValueError as exc:
        raise BenchmarkValidationError(f"{label}必须填写有效 IPv4 地址，不支持域名或容器名。") from exc
    if parsed.version != 4:
        raise BenchmarkValidationError(f"{label}必须填写有效 IPv4 地址。")


def _decode_chunked_http_body(payload: bytes) -> bytes:
    position = 0
    chunks: List[bytes] = []
    while position < len(payload):
        line_end = payload.find(b"\r\n", position)
        if line_end < 0:
            break
        chunk_header = payload[position:line_end].split(b";", 1)[0].strip()
        if not chunk_header:
            break
        chunk_size = int(chunk_header, 16)
        position = line_end + 2
        if chunk_size == 0:
            break
        chunks.append(payload[position:position + chunk_size])
        position += chunk_size + 2
    return b"".join(chunks)


def _read_http_json_from_socket(client: socket.socket, path: str, host_header: str) -> Any:
    request = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host_header}\r\n"
        "User-Agent: qfusion-db-benchmark\r\n"
        "Connection: close\r\n\r\n"
    )
    client.sendall(request.encode("utf-8"))
    response = bytearray()
    while True:
        chunk = client.recv(65536)
        if not chunk:
            break
        response.extend(chunk)

    header_bytes, _, body = bytes(response).partition(b"\r\n\r\n")
    header_lines = header_bytes.decode("utf-8", errors="replace").split("\r\n")
    if not header_lines:
        return None
    status_line = header_lines[0].split()
    if len(status_line) < 2:
        return None
    status_code = _safe_int(status_line[1])
    if status_code >= 400:
        return None

    headers: Dict[str, str] = {}
    for line in header_lines[1:]:
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        headers[key.strip().lower()] = value.strip().lower()

    if headers.get("transfer-encoding") == "chunked":
        body = _decode_chunked_http_body(body)
    if not body:
        return None
    try:
        return json.loads(body.decode("utf-8", errors="replace"))
    except Exception:
        return None


def _docker_http_get_json(path: str) -> Any:
    if not DOCKER_SOCKET_PATH.exists():
        return None

    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.settimeout(2.5)
    try:
        client.connect(str(DOCKER_SOCKET_PATH))
        return _read_http_json_from_socket(client, path, "docker")
    except Exception:
        return None
    finally:
        client.close()


def _docker_http_get_json_remote(host: str, path: str, port: int = 2375) -> Any:
    target_host = str(host or "").strip()
    if not target_host:
        return None
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.settimeout(2.5)
    try:
        client.connect((target_host, port))
        return _read_http_json_from_socket(client, path, target_host)
    except Exception:
        return None
    finally:
        client.close()


def _docker_inspect_json(container_id: str, daemon_host: str = "") -> Any:
    path = f"/containers/{container_id}/json"
    if daemon_host:
        return _docker_http_get_json_remote(daemon_host, path)
    return _docker_http_get_json(path)


def _is_local_host(host: str) -> bool:
    candidate = str(host or "").strip().lower()
    return candidate in {"", "localhost", "127.0.0.1", "::1", "host.docker.internal"}


def _docker_daemon_candidates(*values: Any) -> List[str]:
    hosts: List[str] = []
    seen = set()
    for value in values:
        raw = str(value or "").strip()
        if not raw:
            continue
        for item in raw.split(","):
            candidate = item.strip()
            if not candidate:
                continue
            if "://" in candidate:
                candidate = candidate.split("://", 1)[1]
            if "@" in candidate:
                candidate = candidate.rsplit("@", 1)[-1]
            candidate = candidate.split("/", 1)[0].strip()
            if candidate.startswith("[") and "]" in candidate:
                candidate = candidate[1:candidate.index("]")]
            elif ":" in candidate:
                candidate = candidate.rsplit(":", 1)[0]
            normalized = candidate.strip().lower()
            if normalized and normalized not in seen and not _is_local_host(normalized):
                seen.add(normalized)
                hosts.append(normalized)
    return hosts


def _container_candidates(*values: Any) -> List[str]:
    candidates: List[str] = []
    seen: set[str] = set()
    for value in values:
        raw = str(value or "").strip()
        if not raw:
            continue
        for item in raw.split(","):
            candidate = item.strip()
            if not candidate:
                continue
            if "://" in candidate:
                candidate = candidate.split("://", 1)[1]
            if "@" in candidate:
                candidate = candidate.rsplit("@", 1)[-1]
            candidate = candidate.split("/", 1)[0].strip()
            if not candidate:
                continue
            variants = {candidate.lower()}
            if ":" in candidate and not candidate.startswith("["):
                variants.add(candidate.rsplit(":", 1)[0].lower())
            for variant in variants:
                normalized = variant.strip()
                if normalized and normalized not in seen:
                    seen.add(normalized)
                    candidates.append(normalized)
    return candidates


def _find_docker_container(candidates: List[str], daemon_hosts: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
    if not candidates:
        return None

    wanted = set(candidates)
    daemon_sources = [("local", None)] + [("remote", host) for host in (daemon_hosts or [])]

    for source_type, daemon_host in daemon_sources:
        cache_key = f"{source_type}:{daemon_host or 'local'}|{'|'.join(sorted(candidates))}"
        if cache_key in DOCKER_CONTAINER_CACHE:
            cached = DOCKER_CONTAINER_CACHE[cache_key]
            if cached:
                return cached
            continue

        containers = (
            _docker_http_get_json("/containers/json?all=1")
            if source_type == "local"
            else _docker_http_get_json_remote(str(daemon_host), "/containers/json?all=1")
        )
        if not isinstance(containers, list):
            DOCKER_CONTAINER_CACHE[cache_key] = None
            continue

        matched: Optional[Dict[str, Any]] = None
        for container in containers:
            if not isinstance(container, dict):
                continue
            names = {str(name).lstrip("/").lower() for name in container.get("Names", [])}
            labels_map = container.get("Labels") or {}
            labels = {str(value).lower() for value in labels_map.values()}
            if isinstance(labels_map, dict):
                hostname = str(labels_map.get("com.docker.compose.service", "")).strip().lower()
                if hostname:
                    labels.add(hostname)
            networks = container.get("NetworkSettings", {}).get("Networks", {}) if isinstance(container.get("NetworkSettings"), dict) else {}
            aliases = set()
            if isinstance(networks, dict):
                for network in networks.values():
                    if not isinstance(network, dict):
                        continue
                    aliases.update(str(alias).lower() for alias in network.get("Aliases", []) or [])
                    aliases.update(str(alias).lower() for alias in network.get("DNSNames", []) or [])
                    ip_address = str(network.get("IPAddress", "")).strip().lower()
                    if ip_address:
                        aliases.add(ip_address)
            container_id = str(container.get("Id", "")).lower()
            short_id = container_id[:12]
            values = names | labels | aliases
            if short_id:
                values.add(short_id)
            inspect_payload = _docker_inspect_json(container_id, str(daemon_host or "")) if container_id else None
            if isinstance(inspect_payload, dict):
                inspect_hostname = str((inspect_payload.get("Config") or {}).get("Hostname", "")).strip().lower()
                if inspect_hostname:
                    values.add(inspect_hostname)
            if any(candidate in values or (container_id and container_id.startswith(candidate)) for candidate in wanted):
                matched = {
                    "id": str(container.get("Id", "")),
                    "name": next(iter(names), str(container.get("Id", ""))[:12]),
                    "daemon_host": daemon_host or "",
                    "source_type": source_type,
                }
                break

        DOCKER_CONTAINER_CACHE[cache_key] = matched
        if matched:
            return matched

    return None


def _docker_container_runtime_stats(*candidates: Any, docker_hosts: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
    container = _find_docker_container(_container_candidates(*candidates), daemon_hosts=docker_hosts)
    if not container:
        return None

    stats = (
        _docker_http_get_json(f"/containers/{container['id']}/stats?stream=false")
        if container.get("source_type") == "local"
        else _docker_http_get_json_remote(str(container.get("daemon_host", "")), f"/containers/{container['id']}/stats?stream=false")
    )
    if not isinstance(stats, dict):
        return None

    cpu_stats = stats.get("cpu_stats", {}) if isinstance(stats.get("cpu_stats"), dict) else {}
    precpu_stats = stats.get("precpu_stats", {}) if isinstance(stats.get("precpu_stats"), dict) else {}
    memory_stats = stats.get("memory_stats", {}) if isinstance(stats.get("memory_stats"), dict) else {}
    memory_detail = memory_stats.get("stats", {}) if isinstance(memory_stats.get("stats"), dict) else {}

    cpu_delta = _safe_float(cpu_stats.get("cpu_usage", {}).get("total_usage")) - _safe_float(
        precpu_stats.get("cpu_usage", {}).get("total_usage")
    )
    system_delta = _safe_float(cpu_stats.get("system_cpu_usage")) - _safe_float(precpu_stats.get("system_cpu_usage"))
    online_cpus = _safe_int(cpu_stats.get("online_cpus")) or len(cpu_stats.get("cpu_usage", {}).get("percpu_usage") or []) or 1
    cpu_percent = round((cpu_delta / system_delta) * online_cpus * 100.0, 2) if cpu_delta > 0 and system_delta > 0 else None
    cpu_percent = _round_percent(cpu_percent)

    memory_usage = _safe_float(memory_stats.get("usage"))
    cache_bytes = _safe_float(memory_detail.get("inactive_file")) or _safe_float(memory_detail.get("cache"))
    if memory_usage > 0 and cache_bytes > 0 and memory_usage > cache_bytes:
        memory_usage -= cache_bytes
    memory_limit = _safe_float(memory_stats.get("limit"))
    blkio_stats = stats.get("blkio_stats", {}) if isinstance(stats.get("blkio_stats"), dict) else {}
    io_serviced = blkio_stats.get("io_serviced_recursive") or []
    io_ops_total = 0.0
    if isinstance(io_serviced, list):
        for item in io_serviced:
            if isinstance(item, dict):
                io_ops_total += _safe_float(item.get("value"))

    return {
        "container_name": str(container.get("name", "-")),
        "cpu_percent": _round_percent(cpu_percent),
        "memory_usage_bytes": memory_usage,
        "memory_limit_bytes": memory_limit,
        "io_ops_total": io_ops_total if io_ops_total > 0 else None,
        "daemon_host": str(container.get("daemon_host", "")),
    }


def _resolve_container_runtime_stats(config: BenchmarkConfig, *candidates: Any) -> Optional[Dict[str, Any]]:
    if config.prometheus_url:
        prometheus_stats = _prometheus_container_runtime_stats(
            config.prometheus_url,
            config.prometheus_container,
            *candidates,
        )
        if prometheus_stats:
            return prometheus_stats

    return _docker_container_runtime_stats(
        config.prometheus_container,
        *candidates,
        docker_hosts=_docker_daemon_candidates(config.host),
    )


@dataclass
class BenchmarkConfig:
    db_type: str
    host: str
    port: int
    user: str
    password: str
    database: str
    workload: str
    threads_values: List[int]
    mongo_topology: str = "standalone"
    mongo_hosts: str = ""
    mongo_replica_set: str = ""
    mongo_read_preference: str = "primary"
    mongo_shard_key: str = "_id"
    auth_database: str = ""
    collection_name: str = "test"
    operation_count: int = 100000
    redis_data_size: int = 128
    kafka_bootstrap_servers: str = ""
    kafka_acks: str = "1"
    kafka_throughput: int = -1
    kafka_replication_factor: int = 1
    es_pipeline: str = "benchmark-only"
    es_challenge: str = ""
    es_ingest_percentage: float = 100.0
    es_track_params: str = ""
    gaussdb_architecture: str = "centralized"
    gaussdb_cn_hosts: str = ""
    gaussdb_conn_params: str = "prepareThreshold=1&batchMode=on&fetchsize=10"
    oceanbase_tenant_name: str = ""
    oracle_engine: str = "hammerdb"
    postgres_engine: str = "sysbench"
    tidb_engine: str = "sysbench"
    postgres_schema: str = ""
    pgbench_scale: int = 200
    pgbench_jobs: int = 16
    pgbench_fillfactor: int = 80
    pgbench_latency_limit: int = 10
    fio_target_path: str = "/app/data/artifacts/fio/testfile"
    fio_ssh_enabled: bool = False
    fio_ssh_host: str = ""
    fio_ssh_port: int = 22
    fio_ssh_user: str = ""
    fio_ssh_password: str = ""
    fio_block_size: str = "4k"
    fio_iodepth: int = 32
    fio_size_mb: int = 1024
    fio_direct: bool = True
    fio_read_percent: int = 50
    prometheus_url: str = ""
    prometheus_instance: str = ""
    prometheus_namespace: str = ""
    prometheus_pod: str = ""
    prometheus_container: str = ""
    current_primary: str = ""
    duration_seconds: int = 60
    warmup_seconds: int = 300
    report_interval: int = 2
    table_count: int = 250
    table_size: int = 10000
    extra_options: str = ""
    ssl_enabled: bool = False
    ssl_verify: bool = False
    ssl_ca_file: str = ""
    auto_prepare: bool = True
    auto_cleanup: bool = False
    report_title: str = "数据库性能测试报告"
    artifact_dir: str = ""
    task_id: str = ""

    def validate(self) -> None:
        if self.db_type not in SUPPORTED_DB_TYPES:
            raise BenchmarkValidationError("数据库类型不在当前可选列表中。")
        if self.db_type != "FIO" and (self.port <= 0 or self.port > 65535):
            raise BenchmarkValidationError("端口必须填写 1-65535 之间的有效端口。")
        if not self.threads_values:
            raise BenchmarkValidationError("至少需要一个线程值。")
        if any(value <= 0 for value in self.threads_values):
            raise BenchmarkValidationError("线程值必须大于 0。")
        if self.warmup_seconds < 0:
            raise BenchmarkValidationError("预热时长不能小于 0 秒。")
        if self.db_type == "MySQL":
            if not self.host:
                raise BenchmarkValidationError("数据库地址不能为空。")
            _validate_ipv4_address(self.host)
            if not self.database:
                raise BenchmarkValidationError("数据库名称不能为空。")
            self._validate_mysql()
            return
        if self.db_type == "TiDB":
            if not self.host:
                raise BenchmarkValidationError("TiDB 地址不能为空。")
            _validate_ipv4_address(self.host, "TiDB 地址")
            if not self.database:
                raise BenchmarkValidationError("TiDB 数据库名称不能为空。")
            self._validate_tidb()
            return
        if self.db_type == "FIO":
            self._validate_fio()
            return
        if self.db_type == "Redis":
            if not self.host:
                raise BenchmarkValidationError("Redis 地址不能为空。")
            _validate_ipv4_address(self.host, "Redis 地址")
            self._validate_redis()
            return
        if self.db_type == "ElasticSearch":
            if not self.host:
                raise BenchmarkValidationError("ElasticSearch 地址不能为空。")
            self._validate_elasticsearch()
            return
        if self.db_type == "ClickHouse":
            if not self.host:
                raise BenchmarkValidationError("ClickHouse 地址不能为空。")
            _validate_ipv4_address(self.host, "ClickHouse 地址")
            if not self.database:
                self.database = "default"
            if not self.user:
                self.user = "default"
            self._validate_clickhouse()
            return
        if self.db_type == "Kafka":
            if not self.host and not self.kafka_bootstrap_servers:
                raise BenchmarkValidationError("Kafka Bootstrap Servers 不能为空。")
            if self.host and "," not in self.host and ":" not in self.host:
                _validate_ipv4_address(self.host, "Kafka 地址")
            if not self.database:
                raise BenchmarkValidationError("Kafka Topic 不能为空。")
            self._validate_kafka()
            return
        if self.db_type == "RocketMQ":
            if not self.host:
                raise BenchmarkValidationError("RocketMQ NameServer 地址不能为空。")
            if "," not in self.host and ";" not in self.host and ":" not in self.host:
                _validate_ipv4_address(self.host, "RocketMQ NameServer 地址")
            if not self.database:
                raise BenchmarkValidationError("RocketMQ Topic 不能为空。")
            self._validate_rocketmq()
            return
        if self.db_type == "RabbitMQ":
            if not self.host:
                raise BenchmarkValidationError("RabbitMQ Broker 地址不能为空。")
            if "," not in self.host and ":" not in self.host:
                _validate_ipv4_address(self.host, "RabbitMQ Broker 地址")
            if not self.database:
                raise BenchmarkValidationError("RabbitMQ Queue 不能为空。")
            self._validate_rabbitmq()
            return
        if self.db_type == "Oracle":
            if not self.host:
                raise BenchmarkValidationError("Oracle 地址不能为空。")
            _validate_ipv4_address(self.host, "Oracle 地址")
            if not self.user:
                raise BenchmarkValidationError("Oracle system 用户名不能为空。")
            if self.user.strip().lower() != "system":
                raise BenchmarkValidationError("Oracle HammerDB 当前强制使用 system 用户执行连接、建模和 AWR 授权，请将用户名填写为 system。")
            if not self.password:
                raise BenchmarkValidationError("Oracle system 密码不能为空。")
            if not self.database:
                raise BenchmarkValidationError("Oracle service name 不能为空。")
            self._validate_oracle()
            return
        if self.db_type == "SQL Server":
            if not self.host:
                raise BenchmarkValidationError("SQL Server 地址不能为空。")
            _validate_ipv4_address(self.host, "SQL Server 地址")
            if not self.user:
                raise BenchmarkValidationError("SQL Server 用户名不能为空。")
            if not self.password:
                raise BenchmarkValidationError("SQL Server 密码不能为空。")
            if not self.database:
                raise BenchmarkValidationError("SQL Server 数据库名不能为空。")
            self._validate_sqlserver()
            return
        if self.db_type == "DM":
            if not self.host:
                raise BenchmarkValidationError("DM 地址不能为空。")
            _validate_ipv4_address(self.host, "DM 地址")
            if not self.user:
                raise BenchmarkValidationError("DM 用户名不能为空。")
            if not self.password:
                raise BenchmarkValidationError("DM 密码不能为空。")
            self._validate_dm()
            return
        if self.db_type == "OceanBase":
            if not self.host:
                raise BenchmarkValidationError("OceanBase 地址不能为空。")
            _validate_ipv4_address(self.host, "OceanBase 地址")
            if not self.oceanbase_tenant_name:
                raise BenchmarkValidationError("OceanBase 租户名不能为空。")
            if not _oceanbase_safe_identifier(self.oceanbase_tenant_name):
                raise BenchmarkValidationError("OceanBase 租户名只能包含字母、数字、下划线、点、美元符号和中横线。")
            if self.oceanbase_tenant_name.lower() == "sys":
                raise BenchmarkValidationError("OceanBase 性能测试不能使用 sys 租户，请填写业务租户名。")
            if not self.user:
                raise BenchmarkValidationError("OceanBase 连接用户不能为空。")
            if not self.password:
                raise BenchmarkValidationError("OceanBase 密码不能为空。")
            if not self.database:
                raise BenchmarkValidationError("OceanBase 数据库名不能为空。")
            self._validate_oceanbase()
            return
        if self.db_type == "KingBase":
            if not self.host:
                raise BenchmarkValidationError("KingBase 地址不能为空。")
            _validate_ipv4_address(self.host, "KingBase 地址")
            if not self.user:
                raise BenchmarkValidationError("KingBase 用户名不能为空。")
            if not self.password:
                raise BenchmarkValidationError("KingBase 密码不能为空。")
            if not self.database:
                raise BenchmarkValidationError("KingBase 数据库名不能为空。")
            self._validate_kingbase()
            return
        if self.db_type == "GaussDB":
            if not self.host:
                raise BenchmarkValidationError("GaussDB 地址不能为空。")
            if not self.user:
                raise BenchmarkValidationError("GaussDB 用户名不能为空。")
            if not self.password:
                raise BenchmarkValidationError("GaussDB 密码不能为空。")
            if not self.database:
                raise BenchmarkValidationError("GaussDB 数据库名不能为空。")
            self._validate_gaussdb()
            return
        if self.db_type == "MongoDB":
            if self.mongo_topology == "standalone" and not self.host:
                raise BenchmarkValidationError("数据库地址不能为空。")
            if self.mongo_topology == "standalone":
                _validate_ipv4_address(self.host)
            if not self.database:
                raise BenchmarkValidationError("数据库名称不能为空。")
            self._validate_mongodb()
            return
        if self.db_type in POSTGRES_DB_TYPES:
            if not self.host:
                raise BenchmarkValidationError("数据库地址不能为空。")
            _validate_ipv4_address(self.host)
            if not self.database:
                raise BenchmarkValidationError("数据库名称不能为空。")
            self._validate_postgresql()
            return
        raise BenchmarkValidationError(
            f"当前版本仅支持 MySQL / TiDB / SQL Server / Oracle / PostgreSQL / GaussDB / OpenGauss / DM / OceanBase / KingBase / Vastbase / MongoDB / Redis / ElasticSearch / ClickHouse / Kafka / RocketMQ / RabbitMQ / FIO 压测执行，已选择 {self.db_type}。"
        )

    def _validate_mysql(self) -> None:
        if not self.user:
            raise BenchmarkValidationError("数据库用户名不能为空。")
        if self.workload not in ALLOWED_WORKLOADS:
            raise BenchmarkValidationError(f"当前 {self.db_type} 仅支持标准 sysbench OLTP 模型。")
        if self.duration_seconds <= 0:
            raise BenchmarkValidationError("测试时长必须大于 0 秒。")
        if self.report_interval <= 0:
            raise BenchmarkValidationError("报告间隔必须大于 0 秒。")
        if self.table_count <= 0:
            raise BenchmarkValidationError("表数量必须大于 0。")
        if self.table_size <= 0:
            raise BenchmarkValidationError("单表行数必须大于 0。")

    def _validate_tidb(self) -> None:
        if not self.user:
            raise BenchmarkValidationError("TiDB 用户名不能为空。")
        if self.tidb_engine not in TIDB_ALLOWED_ENGINES:
            raise BenchmarkValidationError("TiDB 压测引擎仅支持 sysbench 或 tiup bench TPC-C。")
        if self.tidb_engine == "sysbench":
            self._validate_mysql()
            return
        if self.workload.strip().lower() not in TIDB_TPCC_WORKLOADS:
            raise BenchmarkValidationError("TiDB tiup bench 当前仅支持 TPC-C 模型。")
        if self.duration_seconds <= 0:
            raise BenchmarkValidationError("测试时长必须大于 0 秒。")
        if self.table_count <= 0:
            raise BenchmarkValidationError("TiDB TPC-C 造数并发必须大于 0。")
        if self.table_size <= 0:
            raise BenchmarkValidationError("TiDB TPC-C 仓库数必须大于 0。")

    def _validate_redis(self) -> None:
        if self.workload not in REDIS_ALLOWED_WORKLOADS:
            raise BenchmarkValidationError("当前 Redis 仅支持 redis-benchmark set 模型。")
        if self.operation_count <= 0:
            raise BenchmarkValidationError("Redis 操作总数必须大于 0。")
        if self.table_size <= 0:
            raise BenchmarkValidationError("Redis 随机 key 空间必须大于 0。")
        if self.redis_data_size <= 0:
            raise BenchmarkValidationError("Redis value 大小必须大于 0。")

    def _validate_elasticsearch(self) -> None:
        if not self.workload:
            raise BenchmarkValidationError("ElasticSearch Rally track 不能为空。")
        if not re.match(r"^[A-Za-z0-9_.-]+$", self.workload):
            raise BenchmarkValidationError("ElasticSearch Rally track 仅支持字母、数字、点、下划线和中划线。")
        if self.es_pipeline not in ELASTICSEARCH_ALLOWED_PIPELINES:
            raise BenchmarkValidationError("ElasticSearch 当前仅支持 Rally benchmark-only pipeline。")
        if self.es_challenge and not re.match(r"^[A-Za-z0-9_.:-]+$", self.es_challenge):
            raise BenchmarkValidationError("ElasticSearch challenge 仅支持字母、数字、点、冒号、下划线和中划线。")
        if not 0 < self.es_ingest_percentage <= 100:
            raise BenchmarkValidationError("ElasticSearch 数据灌入比例必须大于 0 且不超过 100%。")
        if self.table_size <= 0:
            raise BenchmarkValidationError("ElasticSearch bulk size 必须大于 0。")
        if self.report_interval <= 0:
            raise BenchmarkValidationError("ElasticSearch 报告间隔必须大于 0 秒。")
        if "'" in self.user or "'" in self.password:
            raise BenchmarkValidationError("ElasticSearch 用户名和密码暂不支持单引号。")

    def _validate_clickhouse(self) -> None:
        if self.workload not in CLICKHOUSE_ALLOWED_WORKLOADS:
            raise BenchmarkValidationError("当前 ClickHouse 仅支持 count / filter / aggregation / group_by / top_n 查询模型。")
        if self.duration_seconds <= 0:
            raise BenchmarkValidationError("ClickHouse 测试时长必须大于 0 秒。")
        if self.report_interval <= 0:
            raise BenchmarkValidationError("ClickHouse 报告间隔必须大于 0 秒。")
        if self.table_count <= 0:
            raise BenchmarkValidationError("ClickHouse 表数量必须大于 0。")
        if self.table_size <= 0:
            raise BenchmarkValidationError("ClickHouse 造数行数必须大于 0。")

    def _validate_kafka(self) -> None:
        if self.workload not in KAFKA_ALLOWED_WORKLOADS:
            raise BenchmarkValidationError("Kafka 压测模型仅支持 producer / consumer。")
        if not re.match(r"^[A-Za-z0-9._-]+$", self.database):
            raise BenchmarkValidationError("Kafka Topic 只能包含字母、数字、点、下划线和中划线。")
        if self.operation_count <= 0:
            raise BenchmarkValidationError("Kafka 消息总数必须大于 0。")
        if self.table_size <= 0:
            raise BenchmarkValidationError("Kafka 消息大小必须大于 0 bytes。")
        if self.table_count <= 0:
            raise BenchmarkValidationError("Kafka Topic 分区数必须大于 0。")
        if self.report_interval <= 0:
            raise BenchmarkValidationError("Kafka 报告间隔必须大于 0 秒。")
        if max(self.threads_values) > KAFKA_MAX_CONCURRENCY:
            raise BenchmarkValidationError(f"Kafka 并发客户端数不能超过 {KAFKA_MAX_CONCURRENCY}。")
        if str(self.kafka_acks or "1") not in {"0", "1", "all", "-1"}:
            raise BenchmarkValidationError("Kafka acks 只能填写 0 / 1 / all。")
        if self.kafka_throughput < -1 or self.kafka_throughput == 0:
            raise BenchmarkValidationError("Kafka 吞吐限制必须为 -1 或大于 0。")
        if self.kafka_replication_factor <= 0:
            raise BenchmarkValidationError("Kafka Topic 副本数必须大于 0。")

    def _validate_rocketmq(self) -> None:
        if self.workload not in ROCKETMQ_ALLOWED_WORKLOADS:
            raise BenchmarkValidationError("RocketMQ 压测模型仅支持 producer / consumer。")
        if not re.match(r"^[A-Za-z0-9._%-]+$", self.database):
            raise BenchmarkValidationError("RocketMQ Topic 只能包含字母、数字、点、下划线、中划线和百分号。")
        if self.operation_count <= 0:
            raise BenchmarkValidationError("RocketMQ 消息总数必须大于 0。")
        if self.table_size <= 0:
            raise BenchmarkValidationError("RocketMQ 消息大小必须大于 0 bytes。")
        if self.report_interval <= 0:
            raise BenchmarkValidationError("RocketMQ 报告间隔必须大于 0 秒。")
        if self.duration_seconds <= 0:
            raise BenchmarkValidationError("RocketMQ consumer 测试时长必须大于 0 秒。")
        if max(self.threads_values) > ROCKETMQ_MAX_CONCURRENCY:
            raise BenchmarkValidationError(f"RocketMQ 并发线程数不能超过 {ROCKETMQ_MAX_CONCURRENCY}。")

    def _validate_rabbitmq(self) -> None:
        if self.workload not in RABBITMQ_ALLOWED_WORKLOADS:
            raise BenchmarkValidationError("RabbitMQ 压测模型仅支持 producer / consumer / mixed。")
        if not self.user:
            raise BenchmarkValidationError("RabbitMQ 用户名不能为空。")
        if not self.password:
            raise BenchmarkValidationError("RabbitMQ 密码不能为空。")
        if not re.match(r"^[A-Za-z0-9._%:@/-]+$", self.database):
            raise BenchmarkValidationError("RabbitMQ Queue 只能包含字母、数字、点、下划线、中划线、斜线、百分号、冒号和 @。")
        if self.operation_count <= 0:
            raise BenchmarkValidationError("RabbitMQ 消息总数必须大于 0。")
        if self.table_size <= 0:
            raise BenchmarkValidationError("RabbitMQ 消息大小必须大于 0 bytes。")
        if self.report_interval <= 0:
            raise BenchmarkValidationError("RabbitMQ 报告间隔必须大于 0 秒。")
        if self.duration_seconds <= 0:
            raise BenchmarkValidationError("RabbitMQ 测试时长必须大于 0 秒。")
        if max(self.threads_values) > RABBITMQ_MAX_CONCURRENCY:
            raise BenchmarkValidationError(f"RabbitMQ 并发线程数不能超过 {RABBITMQ_MAX_CONCURRENCY}。")

    def _validate_oracle(self) -> None:
        if self.oracle_engine not in ORACLE_ALLOWED_ENGINES:
            raise BenchmarkValidationError("Oracle 压测引擎仅支持 hammerdb / swingbench。")
        expected_workload = "soe" if self.oracle_engine == "swingbench" else "tprocc"
        if self.workload != expected_workload:
            raise BenchmarkValidationError(f"当前 Oracle {self.oracle_engine} 模式仅支持 {expected_workload}。")
        if self.duration_seconds <= 0:
            raise BenchmarkValidationError("Oracle 测试时长必须大于 0 秒。")
        if self.report_interval <= 0:
            raise BenchmarkValidationError("Oracle 报告间隔必须大于 0 秒。")
        if self.table_size <= 0:
            raise BenchmarkValidationError("Oracle 仓库数必须大于 0。")

    def _validate_sqlserver(self) -> None:
        if self.workload not in SQLSERVER_ALLOWED_WORKLOADS:
            raise BenchmarkValidationError("当前 SQL Server 仅支持 HammerDB TPC-C / tprocc。")
        if self.duration_seconds <= 0:
            raise BenchmarkValidationError("SQL Server 测试时长必须大于 0 秒。")
        if self.table_size <= 0:
            raise BenchmarkValidationError("SQL Server 仓库数必须大于 0。")

    def _validate_dm(self) -> None:
        if self.workload not in DM_ALLOWED_WORKLOADS:
            raise BenchmarkValidationError("当前 DM 仅支持 BenchmarkSQL TPC-C / tprocc。")
        if self.duration_seconds <= 0:
            raise BenchmarkValidationError("DM 测试时长必须大于 0 秒。")
        if self.table_size <= 0:
            raise BenchmarkValidationError("DM 仓库数必须大于 0。")
        if self.table_count <= 0:
            raise BenchmarkValidationError("DM 造数并发必须大于 0。")

    def _validate_oceanbase(self) -> None:
        if self.workload not in OCEANBASE_ALLOWED_WORKLOADS:
            raise BenchmarkValidationError("当前 OceanBase 仅支持 BenchmarkSQL TPC-C / tprocc。")
        if self.duration_seconds <= 0:
            raise BenchmarkValidationError("OceanBase 测试时长必须大于 0 秒。")
        if self.table_size <= 0:
            raise BenchmarkValidationError("OceanBase 仓库数必须大于 0。")
        if self.table_count <= 0:
            raise BenchmarkValidationError("OceanBase 造数并发必须大于 0。")

    def _validate_kingbase(self) -> None:
        if self.workload not in KINGBASE_ALLOWED_WORKLOADS:
            raise BenchmarkValidationError("当前 KingBase 仅支持 BenchmarkSQL TPC-C / tprocc。")
        if self.duration_seconds <= 0:
            raise BenchmarkValidationError("KingBase 测试时长必须大于 0 秒。")
        if self.table_size <= 0:
            raise BenchmarkValidationError("KingBase 仓库数必须大于 0。")
        if self.table_count <= 0:
            raise BenchmarkValidationError("KingBase 造数并发必须大于 0。")

    def _validate_gaussdb(self) -> None:
        architecture = str(self.gaussdb_architecture or "centralized").strip().lower()
        if architecture not in GAUSSDB_ALLOWED_ARCHITECTURES:
            raise BenchmarkValidationError("GaussDB 架构仅支持 centralized / distributed。")
        if self.workload not in GAUSSDB_ALLOWED_WORKLOADS:
            raise BenchmarkValidationError("当前 GaussDB 仅支持 BenchmarkSQL TPC-C / tprocc。")
        if self.duration_seconds <= 0:
            raise BenchmarkValidationError("GaussDB 测试时长必须大于 0 秒。")
        if self.table_size <= 0:
            raise BenchmarkValidationError("GaussDB 仓库数必须大于 0。")
        if self.table_count <= 0:
            raise BenchmarkValidationError("GaussDB 造数并发必须大于 0。")
        if architecture == "distributed" and not str(self.gaussdb_cn_hosts or "").strip():
            raise BenchmarkValidationError("GaussDB 分布式模式必须填写 CN 节点列表。")
        _gaussdb_host_entries(self)

    def _validate_mongodb(self) -> None:
        if self.mongo_topology not in MONGO_ALLOWED_TOPOLOGIES:
            raise BenchmarkValidationError("MongoDB 拓扑类型仅支持 standalone / replica_set / sharded。")
        if self.workload not in MONGO_ALLOWED_WORKLOADS:
            raise BenchmarkValidationError("当前 MongoDB 仅支持 YCSB workloada/workloadb/workloadc/workloadf。")
        if self.mongo_topology == "standalone":
            if not self.host:
                raise BenchmarkValidationError("MongoDB 单机模式下数据库地址不能为空。")
        else:
            if not self.mongo_hosts:
                raise BenchmarkValidationError("MongoDB 副本集或分片集群模式下，节点列表不能为空。")
        if self.mongo_topology == "replica_set" and not self.mongo_replica_set:
            raise BenchmarkValidationError("MongoDB 副本集模式下必须填写 replicaSet 名称。")
        if self.mongo_read_preference not in MONGO_ALLOWED_READ_PREFERENCES:
            raise BenchmarkValidationError("MongoDB 读偏好仅支持 primary / primaryPreferred / secondary / secondaryPreferred / nearest。")
        if not self.collection_name:
            raise BenchmarkValidationError("MongoDB 集合名称不能为空。")
        if not self.mongo_shard_key:
            raise BenchmarkValidationError("MongoDB 分片键字段不能为空。")
        if self.table_size <= 0:
            raise BenchmarkValidationError("MongoDB 记录总数必须大于 0。")
        if self.operation_count <= 0:
            raise BenchmarkValidationError("MongoDB 操作总数必须大于 0。")

    def _validate_postgresql(self) -> None:
        if not self.user:
            raise BenchmarkValidationError(f"{self.db_type} 用户名不能为空。")
        if self.db_type == "Vastbase" and not self.postgres_schema.strip():
            raise BenchmarkValidationError("Vastbase Schema 不能为空。")
        if self.postgres_engine not in POSTGRES_ALLOWED_ENGINES:
            raise BenchmarkValidationError(f"{self.db_type} 压测引擎仅支持 sysbench / pgbench。")
        if self.db_type == "OpenGauss" and self.postgres_engine != "sysbench":
            raise BenchmarkValidationError(f"当前 {self.db_type} 按手册仅封装 sysbench(pgsql) 测试链路。")
        if self.duration_seconds <= 0:
            raise BenchmarkValidationError(f"{self.db_type} 测试时长必须大于 0 秒。")
        if self.report_interval <= 0:
            raise BenchmarkValidationError(f"{self.db_type} 报告间隔必须大于 0 秒。")
        if self.postgres_engine == "sysbench":
            if self.workload not in ALLOWED_WORKLOADS:
                raise BenchmarkValidationError(f"当前 {self.db_type} sysbench 仅支持标准 OLTP 模型。")
            if self.table_count <= 0:
                raise BenchmarkValidationError(f"{self.db_type} 表数量必须大于 0。")
            if self.table_size <= 0:
                raise BenchmarkValidationError(f"{self.db_type} 单表行数必须大于 0。")
            return
        if self.workload not in POSTGRES_PGBENCH_WORKLOADS:
            raise BenchmarkValidationError(f"当前 {self.db_type} pgbench 仅支持内置 tpcb_like 模式。")
        if self.pgbench_scale <= 0:
            raise BenchmarkValidationError("pgbench scale 必须大于 0。")
        if self.pgbench_jobs <= 0:
            raise BenchmarkValidationError("pgbench jobs 必须大于 0。")
        if self.pgbench_fillfactor <= 0 or self.pgbench_fillfactor > 100:
            raise BenchmarkValidationError("pgbench fillfactor 必须在 1-100 之间。")
        if self.pgbench_latency_limit <= 0:
            raise BenchmarkValidationError("pgbench 延迟门限必须大于 0。")

    def _validate_fio(self) -> None:
        if not self.fio_target_path:
            raise BenchmarkValidationError("FIO 测试文件路径不能为空。")
        if self.fio_ssh_enabled:
            if not self.fio_ssh_host:
                raise BenchmarkValidationError("启用远程 FIO 后，SSH 节点地址不能为空。")
            if not self.fio_ssh_user:
                raise BenchmarkValidationError("启用远程 FIO 后，SSH 用户不能为空。")
            if self.fio_ssh_port <= 0:
                raise BenchmarkValidationError("SSH 端口必须大于 0。")
        else:
            _validate_local_fio_target_path(Path(self.fio_target_path).expanduser())
        if self.workload not in {"read", "write", "randread", "randwrite", "randrw"}:
            raise BenchmarkValidationError("当前 FIO 模式仅支持 read/write/randread/randwrite/randrw。")
        if self.duration_seconds <= 0:
            raise BenchmarkValidationError("FIO 测试时长必须大于 0 秒。")
        if self.fio_iodepth <= 0:
            raise BenchmarkValidationError("FIO iodepth 必须大于 0。")
        if self.fio_size_mb <= 0:
            raise BenchmarkValidationError("FIO 测试文件大小必须大于 0 MB。")
        if self.fio_read_percent < 0 or self.fio_read_percent > 100:
            raise BenchmarkValidationError("FIO 混合读比例必须在 0-100 之间。")


@dataclass
class ConcurrencyResult:
    concurrency: int
    elapsed_seconds: float
    total_transactions: int
    total_queries: int
    total_events: int
    read_queries: int
    write_queries: int
    other_queries: int
    failed_requests: int
    qps: float
    tps: float
    avg_latency_ms: float
    p95_latency_ms: float
    min_latency_ms: float
    max_latency_ms: float
    success_rate: float
    baseline_label: str
    sample_errors: List[str]
    raw_output: str = field(repr=False, default="")

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload.pop("raw_output", None)
        return payload

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "ConcurrencyResult":
        return cls(
            concurrency=int(payload["concurrency"]),
            elapsed_seconds=float(payload["elapsed_seconds"]),
            total_transactions=int(payload["total_transactions"]),
            total_queries=int(payload["total_queries"]),
            total_events=int(payload["total_events"]),
            read_queries=int(payload["read_queries"]),
            write_queries=int(payload["write_queries"]),
            other_queries=int(payload["other_queries"]),
            failed_requests=int(payload["failed_requests"]),
            qps=float(payload["qps"]),
            tps=float(payload["tps"]),
            avg_latency_ms=float(payload["avg_latency_ms"]),
            p95_latency_ms=float(payload["p95_latency_ms"]),
            min_latency_ms=float(payload["min_latency_ms"]),
            max_latency_ms=float(payload["max_latency_ms"]),
            success_rate=float(payload["success_rate"]),
            baseline_label=str(payload["baseline_label"]),
            sample_errors=list(payload.get("sample_errors", [])),
        )


@dataclass
class BenchmarkSummary:
    report_title: str
    executed_at: str
    target: Dict[str, Any]
    parameters: Dict[str, Any]
    results: List[ConcurrencyResult]
    recommended_baseline: Optional[ConcurrencyResult]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "report_title": self.report_title,
            "executed_at": self.executed_at,
            "target": self.target,
            "parameters": self.parameters,
            "results": [result.to_dict() for result in self.results],
            "recommended_baseline": self.recommended_baseline.to_dict()
            if self.recommended_baseline
            else None,
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "BenchmarkSummary":
        results = [ConcurrencyResult.from_dict(item) for item in payload.get("results", [])]
        baseline_payload = payload.get("recommended_baseline")
        return cls(
            report_title=str(payload["report_title"]),
            executed_at=str(payload["executed_at"]),
            target=dict(payload.get("target", {})),
            parameters=dict(payload.get("parameters", {})),
            results=results,
            recommended_baseline=ConcurrencyResult.from_dict(baseline_payload) if baseline_payload else None,
        )


def _classify_baseline(success_rate: float, p95_latency_ms: float) -> str:
    if success_rate < 95:
        return "失败率偏高"
    if success_rate < 99:
        return "可观察"
    if p95_latency_ms <= 100:
        return "推荐基线"
    if p95_latency_ms <= 300:
        return "稳定"
    if p95_latency_ms <= 800:
        return "高负载"
    return "待优化"


def _find_sysbench() -> str:
    configured_raw = os.environ.get("SYSBENCH_BIN", "").strip()
    if configured_raw:
        configured = Path(configured_raw)
        if configured.exists():
            return str(configured)
    if VENDORED_SYSBENCH_BIN.exists():
        return str(VENDORED_SYSBENCH_BIN)
    sysbench_path = shutil.which("sysbench")
    if not sysbench_path:
        raise BenchmarkValidationError(
            "未找到 sysbench。请优先使用 Docker，或先执行 scripts/vendor_sysbench.sh 将 sysbench 内置到项目目录。"
        )
    return sysbench_path


def _find_tiup() -> str:
    configured_raw = os.environ.get("TIUP_BIN", "").strip()
    candidates: List[Path] = []
    if configured_raw:
        candidates.append(Path(configured_raw))
    candidates.extend(
        [
            Path("/usr/local/bin/tiup"),
            Path("/root/.tiup/bin/tiup"),
            Path("/app/vendor/tiup/bin/tiup"),
        ]
    )
    for candidate in candidates:
        if candidate.exists() and os.access(candidate, os.X_OK):
            return str(candidate)
    tiup_path = shutil.which("tiup")
    if not tiup_path:
        raise BenchmarkValidationError("未找到 tiup。请重新构建镜像，确保已内置 TiUP bench 组件。")
    return tiup_path


MYSQL_AUTH_PLUGIN_NAME = "caching_sha2_password.so"
MYSQL_PLUGIN_DIR_ENV_KEYS = ("MYSQL_PLUGIN_DIR", "LIBMYSQL_PLUGIN_DIR", "MARIADB_PLUGIN_DIR")
MYSQL_PLUGIN_DIR_CANDIDATES = (
    "/usr/lib64/mysql/plugin",
    "/usr/lib/mysql/plugin",
    "/usr/lib/x86_64-linux-gnu/mariadb19/plugin",
    "/usr/lib/x86_64-linux-gnu/mariadb18/plugin",
    "/usr/lib/x86_64-linux-gnu/mariadb/plugin",
    "/usr/lib64/mariadb/plugin",
    "/app/vendor/mysql/plugin",
)
_MYSQL_PLUGIN_DIR_LOGGED: Dict[str, bool] = {}


def _mysql_auth_plugin_missing_message() -> str:
    return (
        "当前运行环境缺少 MySQL 8/9 默认认证插件 caching_sha2_password.so，"
        "sysbench 无法连接使用 caching_sha2_password 认证的用户。"
        "请使用已更新的 QDBmark Docker 镜像，或在运行环境中安装 MariaDB-common/MariaDB-compat，"
        "并确认 /usr/lib64/mysql/plugin/caching_sha2_password.so 存在。"
    )


def _mysql_client_plugin_dirs() -> List[Path]:
    raw_candidates: List[str] = []
    for env_key in MYSQL_PLUGIN_DIR_ENV_KEYS:
        raw_value = os.environ.get(env_key, "").strip()
        if raw_value:
            raw_candidates.append(raw_value)
    raw_candidates.extend(MYSQL_PLUGIN_DIR_CANDIDATES)

    plugin_dirs: List[Path] = []
    seen: set[str] = set()
    for raw_candidate in raw_candidates:
        candidate = Path(raw_candidate)
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        if (candidate / MYSQL_AUTH_PLUGIN_NAME).exists():
            plugin_dirs.append(candidate)
    return plugin_dirs


def _mysql_sysbench_env(log_callback: Optional[LogCallback] = None) -> Dict[str, str]:
    plugin_dirs = _mysql_client_plugin_dirs()
    if not plugin_dirs:
        return {}

    plugin_dir = str(plugin_dirs[0])
    if not _MYSQL_PLUGIN_DIR_LOGGED.get(plugin_dir):
        _emit_log(log_callback, f"MySQL 8/9 认证插件目录: {plugin_dir}")
        _MYSQL_PLUGIN_DIR_LOGGED[plugin_dir] = True
    return {env_key: plugin_dir for env_key in MYSQL_PLUGIN_DIR_ENV_KEYS}


def _is_mysql_auth_plugin_load_error(output: str) -> bool:
    normalized = output.lower()
    return "caching_sha2_password" in normalized and "cannot be loaded" in normalized


def _find_redis_benchmark() -> str:
    redis_benchmark_path = shutil.which("redis-benchmark")
    if not redis_benchmark_path:
        raise BenchmarkValidationError("未找到 redis-benchmark。请优先使用 Docker，或在当前运行环境中安装 redis-tools。")
    return redis_benchmark_path


def _find_redis_cli() -> str:
    redis_cli_path = shutil.which("redis-cli")
    if not redis_cli_path:
        raise BenchmarkValidationError("未找到 redis-cli。请优先使用 Docker，或在当前运行环境中安装 redis-tools。")
    return redis_cli_path


def _find_hammerdbcli() -> str:
    configured_raw = os.environ.get("HAMMERDB_CLI", "").strip()
    if configured_raw:
        configured = Path(configured_raw)
        if configured.exists():
            return str(configured)
    preferred = Path("/opt/HammerDB-4.0/hammerdbcli")
    if preferred.exists():
        return str(preferred)
    for pattern in ("/opt/HammerDB*/hammerdbcli", "/opt/hammerdb*/hammerdbcli"):
        for candidate in sorted(Path("/").glob(pattern.lstrip("/")), reverse=True):
            if candidate.exists():
                return str(candidate)
    hammerdb_path = shutil.which("hammerdbcli")
    if hammerdb_path:
        return hammerdb_path
    raise BenchmarkValidationError("未找到 hammerdbcli。请使用包含 HammerDB 的镜像，或设置 HAMMERDB_CLI 指向 hammerdbcli。")


def _find_swingbench_home() -> Path:
    configured_raw = os.environ.get("SWINGBENCH_HOME", "").strip()
    candidates: List[Path] = []
    if configured_raw:
        candidates.append(Path(configured_raw))
    candidates.extend([Path("/opt/swingbench"), Path("/opt/swingbench261076"), Path("/app/vendor/swingbench")])
    for candidate in candidates:
        if (candidate / "bin" / "charbench").exists() and (candidate / "bin" / "oewizard").exists():
            return candidate
    raise BenchmarkValidationError("未找到 Swingbench。请设置 SWINGBENCH_HOME，或将 swingbench 解压到 /opt/swingbench。")


def _find_swingbench_command(name: str) -> str:
    command = _find_swingbench_home() / "bin" / name
    if not command.exists():
        raise BenchmarkValidationError(f"未找到 Swingbench 命令: {command}")
    return str(command)


def _find_ycsb() -> str:
    configured_home = os.environ.get("YCSB_HOME", "").strip()
    if configured_home:
        candidate = Path(configured_home) / "bin" / "ycsb.sh"
        if candidate.exists():
            return str(candidate)
    if VENDORED_YCSB_BIN.exists():
        return str(VENDORED_YCSB_BIN)
    ycsb_path = shutil.which("ycsb.sh") or shutil.which("ycsb")
    if not ycsb_path:
        raise BenchmarkValidationError("未找到 YCSB。请优先使用 Docker 方式运行 MongoDB 压测。")
    return ycsb_path


def _find_pgbench() -> str:
    for candidate in Path("/usr/lib/postgresql").glob("*/bin/pgbench"):
        if candidate.exists():
            return str(candidate)
    pgbench_path = shutil.which("pgbench")
    if not pgbench_path:
        raise BenchmarkValidationError("未找到 pgbench。请优先使用 Docker，或在当前运行环境中安装 PostgreSQL client。")
    return pgbench_path


_PGBENCH_HELP_CACHE: Dict[str, str] = {}


def _pgbench_help_text(pgbench_path: str) -> str:
    cached = _PGBENCH_HELP_CACHE.get(pgbench_path)
    if cached is not None:
        return cached
    try:
        result = subprocess.run(
            [pgbench_path, "--help"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=10,
            check=False,
        )
        help_text = result.stdout or ""
    except Exception:
        help_text = ""
    _PGBENCH_HELP_CACHE[pgbench_path] = help_text
    return help_text


def _pgbench_supports_option(pgbench_path: str, option: str) -> bool:
    return option in _pgbench_help_text(pgbench_path)


def _find_vacuumdb() -> str:
    vacuumdb_path = shutil.which("vacuumdb")
    if not vacuumdb_path:
        raise BenchmarkValidationError("未找到 vacuumdb。请优先使用 Docker，或在当前运行环境中安装 PostgreSQL client。")
    return vacuumdb_path


def _vacuumdb_supports_jobs(vacuumdb_path: str) -> bool:
    try:
        result = subprocess.run(
            [vacuumdb_path, "--help"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=5,
            check=False,
        )
    except Exception:
        return False
    return "-j," in result.stdout or "--jobs" in result.stdout


def _find_fio() -> str:
    configured_raw = os.environ.get("FIO_BIN", "").strip()
    if configured_raw:
        configured = Path(configured_raw)
        if configured.exists():
            return str(configured)
    fio_path = shutil.which("fio")
    if not fio_path:
        raise BenchmarkValidationError("未找到 fio。请优先使用 Docker，或在当前运行环境中安装 fio。")
    return fio_path


LOCAL_FIO_SAFE_ROOTS = (Path("/app/data"),)


def _is_path_under(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except (ValueError, FileNotFoundError):
        return False


def _validate_local_fio_target_path(target_path: Path) -> None:
    if not target_path.is_absolute():
        raise BenchmarkValidationError("本地 FIO 测试文件路径必须填写绝对路径。")
    if target_path.exists() and target_path.is_dir():
        raise BenchmarkValidationError("FIO 测试文件路径必须是具体文件路径，不能只填写目录。")
    parent = target_path.parent
    if not parent.exists():
        raise BenchmarkValidationError(
            "本地 FIO 目标目录不存在。为避免误写容器根目录，请先使用 /app/data/artifacts/fio 等已挂载目录，"
            "或启用 SSH 远程执行并填写宿主机测试文件路径。"
        )
    if not any(_is_path_under(parent, root) for root in LOCAL_FIO_SAFE_ROOTS):
        raise BenchmarkValidationError(
            "本地 FIO 目标路径必须位于 /app/data 下的持久化挂载目录。"
            "需要压测宿主机或存储目录时，请启用 SSH 远程执行。"
        )
    if not os.access(parent, os.W_OK):
        raise BenchmarkValidationError(f"本地 FIO 目标目录不可写: {parent}")


def _find_sshpass() -> Optional[str]:
    return shutil.which("sshpass")


def _build_ssh_command(
    config: BenchmarkConfig,
    remote_command: str,
    connect_timeout: int = 5,
) -> List[str]:
    if not config.fio_ssh_enabled:
        raise BenchmarkValidationError("当前并未启用远程 FIO SSH 执行。")

    command: List[str] = []
    if config.fio_ssh_password:
        sshpass_bin = _find_sshpass()
        if not sshpass_bin:
            raise BenchmarkValidationError("当前运行环境未安装 sshpass，无法使用密码方式执行远程 FIO。")
        command.extend([sshpass_bin, "-p", config.fio_ssh_password])

    command.extend(
        [
            "ssh",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            f"ConnectTimeout={connect_timeout}",
            "-o",
            "BatchMode=yes" if not config.fio_ssh_password else "BatchMode=no",
            "-p",
            str(config.fio_ssh_port),
            f"{config.fio_ssh_user}@{config.fio_ssh_host}",
            remote_command,
        ]
    )
    return command


def _stream_logged_lines(output: str, log_callback: Optional[LogCallback], log_prefix: str = "") -> None:
    for line in output.splitlines():
        if line.strip():
            _emit_log(log_callback, f"{log_prefix}{line}")


def _run_remote_command(
    config: BenchmarkConfig,
    remote_command: str,
    timeout_seconds: int,
    allow_failure: bool = False,
    log_callback: Optional[LogCallback] = None,
    log_prefix: str = "",
    log_label: str = "远程命令",
    cancel_callback: Optional[CancelCallback] = None,
) -> str:
    _check_cancel(cancel_callback)
    if shutil.which("ssh"):
        return _run_command(
            _build_ssh_command(config, remote_command),
            timeout_seconds=timeout_seconds,
            allow_failure=allow_failure,
            log_callback=log_callback,
            log_prefix=log_prefix,
            log_label=log_label,
            secrets=[config.fio_ssh_password],
            cancel_callback=cancel_callback,
        )

    try:
        import paramiko
    except Exception as exc:
        raise BenchmarkValidationError("当前运行环境既没有 ssh 客户端，也没有安装 paramiko，无法执行远程 FIO。") from exc

    _emit_log(
        log_callback,
        f"{log_label}启动: paramiko connect -> {config.fio_ssh_user}@{config.fio_ssh_host}:{config.fio_ssh_port}",
    )

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    output = ""
    try:
        client.connect(
            hostname=config.fio_ssh_host,
            port=config.fio_ssh_port,
            username=config.fio_ssh_user,
            password=config.fio_ssh_password or None,
            timeout=min(timeout_seconds, 15),
            banner_timeout=min(timeout_seconds, 15),
            auth_timeout=min(timeout_seconds, 15),
            look_for_keys=not bool(config.fio_ssh_password),
            allow_agent=not bool(config.fio_ssh_password),
        )
        stdin, stdout, stderr = client.exec_command(remote_command, timeout=timeout_seconds, get_pty=True)
        _check_cancel(cancel_callback)
        if stdin:
            stdin.close()
        exit_code = stdout.channel.recv_exit_status()
        output = (stdout.read() + stderr.read()).decode("utf-8", errors="replace").strip()
        _stream_logged_lines(output, log_callback=log_callback, log_prefix=log_prefix)
        if exit_code != 0 and not allow_failure:
            raise BenchmarkValidationError(output or f"{log_label}执行失败。")
        if exit_code == 0:
            _emit_log(log_callback, f"{log_label}完成。")
        else:
            _emit_log(log_callback, f"{log_label}返回非 0，已按允许失败继续。")
        return output
    except BenchmarkValidationError:
        raise
    except Exception as exc:
        raise BenchmarkValidationError(str(exc)) from exc
    finally:
        client.close()


def _redis_command_base(config: BenchmarkConfig, executable: str) -> List[str]:
    command = [
        executable,
        "-h",
        config.host,
        "-p",
        str(config.port),
    ]
    if config.user:
        command.extend(["--user", config.user])
    if config.password:
        command.extend(["-a", config.password])
    return command


def _redis_cli_command(config: BenchmarkConfig, *args: str) -> List[str]:
    return _redis_command_base(config, _find_redis_cli()) + list(args)


def _redis_benchmark_command(config: BenchmarkConfig, concurrency: int) -> List[str]:
    command = _redis_command_base(config, _find_redis_benchmark()) + [
        "-t",
        config.workload,
        "-c",
        str(concurrency),
        "-d",
        str(config.redis_data_size),
        "-n",
        str(config.operation_count),
        "-r",
        str(config.table_size),
    ]
    return command


def _parse_redis_info(output: str) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for line in output.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        result[key.strip()] = value.strip()
    return result


def _check_redis_connectivity(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    _emit_log(log_callback, "开始检查 Redis 连接性...")
    masked_password = "******" if config.password else ""
    _emit_log(
        log_callback,
        f"连接检测启动: redis-cli ping -> host={config.host} port={config.port} user={config.user or '-'} password={masked_password}",
    )
    output = _run_command(
        _redis_cli_command(config, "PING"),
        timeout_seconds=30,
        log_callback=log_callback,
        log_prefix="[redis-cli] ",
        log_label="Redis 连接检测",
        secrets=[config.password],
    )
    if "PONG" not in output.upper():
        raise BenchmarkValidationError(f"Redis PING 未返回 PONG: {output[:400]}")
    _emit_log(log_callback, "Redis 连接检测通过: PONG")


def _run_redis_cleanup(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> None:
    db_index = str(config.database or "0").strip() or "0"
    if not db_index.isdigit():
        raise BenchmarkValidationError("Redis 删除测试数据需要 DB Index 为数字。")
    _emit_log(log_callback, f"开始清理 Redis DB {db_index} 测试数据...")
    _run_command(
        _redis_command_base(config, _find_redis_cli()) + ["-n", db_index, "FLUSHDB"],
        timeout_seconds=120,
        log_callback=log_callback,
        log_prefix="[redis-cleanup] ",
        log_label="Redis FLUSHDB",
        secrets=[config.password],
        cancel_callback=cancel_callback,
    )
    _emit_log(log_callback, f"Redis DB {db_index} 已执行 FLUSHDB。")


def _build_redis_instance_info(config: BenchmarkConfig) -> Dict[str, Any]:
    output = _run_command(
        _redis_cli_command(config, "INFO", "server"),
        timeout_seconds=30,
        allow_failure=True,
        log_label="redis-info-server",
        secrets=[config.password],
    )
    info = _parse_redis_info(output)
    container_stats = _resolve_container_runtime_stats(config, config.prometheus_instance, config.host)
    cards = [
        {"label": "实例类型", "value": "Redis"},
        {"label": "实例地址", "value": f"{config.host}:{config.port}"},
        {"label": "版本", "value": info.get("redis_version", "-")},
        {"label": "运行模式", "value": info.get("redis_mode", "-")},
        {"label": "进程ID", "value": info.get("process_id", "-")},
    ]
    if container_stats:
        cards.extend(
            [
                {"label": "容器名称", "value": container_stats.get("container_name", "-")},
                {"label": "容器内存上限", "value": _format_bytes(_safe_float(container_stats.get("memory_limit_bytes")))},
            ]
        )
    return {"captured_at": _beijing_now().strftime("%Y-%m-%d %H:%M:%S"), "cards": cards}


def _build_redis_runtime_sample(config: BenchmarkConfig) -> Dict[str, Any]:
    output = _run_command(
        _redis_cli_command(config, "INFO"),
        timeout_seconds=30,
        allow_failure=True,
        log_label="redis-info",
        secrets=[config.password],
    )
    info = _parse_redis_info(output)
    prometheus_stats = _prometheus_database_runtime_sample(config)
    container_stats = prometheus_stats or _resolve_container_runtime_stats(config, config.prometheus_instance, config.host)
    return {
        "captured_at": monotonic(),
        "metrics_source": (container_stats or {}).get("metrics_source", "docker" if container_stats else "database"),
        "connected_clients": _safe_int(info.get("connected_clients")),
        "ops_per_sec": _safe_float((container_stats or {}).get("prometheus_qps"), None) or _safe_int(info.get("instantaneous_ops_per_sec")),
        "total_commands_processed": _safe_int(info.get("total_commands_processed")),
        "used_memory_bytes": _safe_int(info.get("used_memory")),
        "maxmemory_bytes": _safe_int(info.get("maxmemory")),
        "container_name": (container_stats or {}).get("container_name", ""),
        "container_cpu_percent": (container_stats or {}).get("container_cpu_percent", (container_stats or {}).get("cpu_percent")),
        "container_cpu_limit_cores": (container_stats or {}).get("container_cpu_limit_cores"),
        "container_memory_percent": (container_stats or {}).get("container_memory_percent"),
        "container_memory_usage_bytes": (container_stats or {}).get("memory_usage_bytes"),
        "container_memory_limit_bytes": (container_stats or {}).get("container_memory_limit_bytes", (container_stats or {}).get("memory_limit_bytes")),
        "container_io_ops_total": (container_stats or {}).get("io_ops_total"),
        "container_io_ops_rate": (container_stats or {}).get("container_io_ops_rate", (container_stats or {}).get("io_ops_rate")),
        "prometheus_qps": (container_stats or {}).get("prometheus_qps"),
        "redis_key_count": (container_stats or {}).get("redis_key_count"),
    }


def _format_redis_runtime_metrics(current: Dict[str, Any], previous: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    elapsed = max(_safe_float(current.get("captured_at")) - _safe_float((previous or {}).get("captured_at")), 0.0)
    qps = _safe_float(current.get("ops_per_sec"), None)
    if (qps is None or qps <= 0) and previous and elapsed > 0:
        qps = max(_safe_int(current.get("total_commands_processed")) - _safe_int(previous.get("total_commands_processed")), 0) / elapsed
    memory_pct = _round_percent(current.get("container_memory_percent")) if current.get("container_memory_percent") is not None else None
    if memory_pct is None:
        memory_usage = _safe_float(current.get("used_memory_bytes")) or _safe_float(current.get("container_memory_usage_bytes"))
        memory_limit = _safe_float(current.get("maxmemory_bytes")) or _safe_float(current.get("container_memory_limit_bytes"))
        memory_pct = round(min(memory_usage / memory_limit * 100, 100), 2) if memory_usage > 0 and memory_limit > 0 else None
    container_cpu_percent = _round_percent(current.get("container_cpu_percent")) if current.get("container_cpu_percent") is not None else None
    container_iops = _safe_float(current.get("container_io_ops_rate"), None)
    if container_iops is None and previous and elapsed > 0:
        current_io_ops = _safe_float(current.get("container_io_ops_total"), -1.0)
        previous_io_ops = _safe_float(previous.get("container_io_ops_total"), -1.0)
        if current_io_ops >= 0 and previous_io_ops >= 0:
            container_iops = round(max(current_io_ops - previous_io_ops, 0.0) / elapsed, 2)
    return {
        "captured_at": _beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
        "cards": [
            {"label": "当前连接数", "value": str(current.get("connected_clients", 0))},
            {"label": "实时 QPS", "value": f"{round(qps, 2)} /s" if qps is not None else "-"},
            {"label": "内存使用率", "value": f"{memory_pct}%" if memory_pct is not None else "-"},
            {"label": "容器 CPU", "value": f"{container_cpu_percent}%" if container_cpu_percent is not None else "-"},
            {"label": "容器 IOPS", "value": f"{container_iops} /s" if container_iops is not None else "-"},
            {"label": "Key 数", "value": str(current.get("redis_key_count")) if current.get("redis_key_count") is not None else "-"},
        ],
        "series": [
            {"key": "queries_rate", "label": "实时 QPS", "chart": "qps", "chart_label": "QPS", "color": "#2f80ed", "value": round(qps, 2) if qps is not None else None, "display": f"{round(qps, 2)} /s" if qps is not None else "-", "unit": "/s", "format": "ops"},
            {"key": "redis_memory_pct", "label": "内存使用率", "chart": "memory_usage", "chart_label": "内存使用率", "color": "#27ae60", "value": memory_pct, "display": f"{memory_pct}%" if memory_pct is not None else "-", "unit": "%", "format": "percent"},
            {"key": "container_cpu_pct", "label": "容器 CPU", "chart": "cpu_usage", "chart_label": "容器 CPU", "color": "#f2994a", "value": container_cpu_percent, "display": f"{container_cpu_percent}%" if container_cpu_percent is not None else "-", "unit": "%", "format": "percent"},
            {"key": "container_iops", "label": "容器 IOPS", "chart": "iops", "chart_label": "IOPS", "color": "#9b51e0", "value": container_iops, "display": f"{container_iops} /s" if container_iops is not None else "-", "unit": "/s", "format": "iops"},
            {"key": "redis_key_count", "label": "Key 数", "chart": "keys", "chart_label": "Key 数", "color": "#5b8def", "value": _safe_float(current.get("redis_key_count"), None), "display": str(current.get("redis_key_count")) if current.get("redis_key_count") is not None else "-", "unit": "", "format": "number"},
        ],
    }


def _parse_redis_benchmark_output(config: BenchmarkConfig, concurrency: int, output: str) -> ConcurrencyResult:
    qps_match = re.search(r"throughput summary:\s*([0-9.]+)\s*requests per second", output, re.IGNORECASE)
    if not qps_match:
        qps_match = re.search(r"([0-9.]+)\s*requests per second", output, re.IGNORECASE)
    if not qps_match:
        raise BenchmarkValidationError(f"未能解析 redis-benchmark 输出，请检查目标 Redis 连接。\n{output[:800]}")
    qps = round(float(qps_match.group(1)), 2)
    avg_match = re.search(r"avg\s+([0-9.]+)\s*ms", output, re.IGNORECASE)
    p95_match = re.search(r"p95\s+([0-9.]+)\s*ms", output, re.IGNORECASE)
    avg_latency = round(float(avg_match.group(1)), 2) if avg_match else 0.0
    p95_latency = round(float(p95_match.group(1)), 2) if p95_match else avg_latency
    total_requests = config.operation_count
    elapsed_seconds = round(total_requests / qps, 2) if qps > 0 else 0
    return ConcurrencyResult(
        concurrency=concurrency,
        elapsed_seconds=elapsed_seconds,
        total_transactions=total_requests,
        total_queries=total_requests,
        total_events=total_requests,
        read_queries=0,
        write_queries=0,
        other_queries=0,
        failed_requests=0,
        qps=qps,
        tps=qps,
        avg_latency_ms=avg_latency,
        p95_latency_ms=p95_latency,
        min_latency_ms=0,
        max_latency_ms=0,
        success_rate=100,
        baseline_label=_classify_baseline(100, p95_latency),
        sample_errors=_extract_sample_errors(output),
        raw_output=output,
    )


def _parse_cli_options(raw: str) -> Dict[str, str]:
    options: Dict[str, str] = {}
    tokens = shlex.split(raw or "")
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token.startswith("--"):
            key = token[2:].replace("-", "_")
            value = "true"
            if index + 1 < len(tokens) and not tokens[index + 1].startswith("--"):
                value = tokens[index + 1]
                index += 1
            options[key] = value
        index += 1
    return options


def _option_enabled(options: Dict[str, str], key: str, default: bool = True) -> bool:
    value = options.get(key)
    if value is None:
        return default
    return str(value).strip().lower() not in {"0", "false", "no", "off", "disabled"}


def _oracle_easy_connect(config: BenchmarkConfig) -> str:
    return f"//{config.host}:{config.port}/{config.database}"


def _tcl_braced(value: Any) -> str:
    text = str(value)
    text = text.replace("\\", "\\\\").replace("}", "\\}")
    return "{" + text + "}"


def _find_oracle_client_lib_dir() -> Optional[str]:
    candidates: List[Path] = []
    env_lib_dir = os.environ.get("ORACLE_CLIENT_LIB_DIR")
    if env_lib_dir:
        candidates.append(Path(env_lib_dir))

    oracle_root = Path("/opt/oracle")
    if oracle_root.exists():
        candidates.append(oracle_root / "instantclient")
        candidates.extend(sorted(oracle_root.glob("instantclient*")))

    for candidate in candidates:
        if not candidate.exists() or not candidate.is_dir():
            continue
        if next(candidate.glob("libclntsh.so*"), None):
            return str(candidate)
    return None


def _init_oracle_client(log_callback: Optional[LogCallback] = None) -> str:
    import oracledb

    global ORACLE_CLIENT_INIT_ATTEMPTED, ORACLE_CLIENT_MODE
    if ORACLE_CLIENT_INIT_ATTEMPTED:
        return ORACLE_CLIENT_MODE

    ORACLE_CLIENT_INIT_ATTEMPTED = True
    lib_dir = _find_oracle_client_lib_dir()
    if not lib_dir:
        ORACLE_CLIENT_MODE = "thin"
        _emit_log(log_callback, "未检测到 Oracle Instant Client，继续使用 thin 模式连接。")
        return ORACLE_CLIENT_MODE

    try:
        oracledb.init_oracle_client(lib_dir=lib_dir)
        ORACLE_CLIENT_MODE = "thick"
        _emit_log(log_callback, f"Oracle Instant Client 已加载: {lib_dir}，切换为 thick 模式连接。")
    except Exception as exc:
        ORACLE_CLIENT_MODE = "thick" if not oracledb.is_thin_mode() else "thin"
        _emit_log(log_callback, f"Oracle Instant Client 初始化失败，将继续使用 {ORACLE_CLIENT_MODE} 模式: {exc}")
    return ORACLE_CLIENT_MODE


def _check_oracle_connectivity(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    import oracledb

    _emit_log(log_callback, "开始检查 Oracle 连接性...")
    mode = _init_oracle_client(log_callback=log_callback)
    masked_password = "******" if config.password else ""
    _emit_log(
        log_callback,
        f"连接检测启动: oracledb {mode} connect -> host={config.host} port={config.port} service={config.database} user={config.user} password={masked_password}",
    )
    try:
        with oracledb.connect(user=config.user, password=config.password, dsn=_oracle_easy_connect(config)) as connection:
            with connection.cursor() as cursor:
                cursor.execute("select banner from v$version where rownum = 1")
                row = cursor.fetchone()
        _emit_log(log_callback, f"Oracle 连接检测通过: {row[0] if row else 'OK'}")
    except Exception as exc:
        message = str(exc)
        if mode == "thin" and "DPY-4011" in message:
            raise BenchmarkValidationError(
                "Oracle 连接在 thin 模式下被服务端主动断开，通常是数据库启用了 Native Network Encryption 或 Oracle Net checksumming。"
                " 当前镜像需要启用 Oracle Instant Client 后使用 thick 模式连接。"
                f" 原始错误: {message}"
            ) from exc
        raise BenchmarkValidationError(message) from exc


ORACLE_COMMON_TABLESPACE_SQL: tuple[tuple[str, str, str, str], ...] = (
    (
        "UNDO",
        "dba_data_files",
        "UNDOTBS1",
        "alter tablespace undotbs1 add datafile size 10G autoextend on maxsize 30G",
    ),
    (
        "TEMP",
        "dba_temp_files",
        "TEMP",
        "alter tablespace temp add tempfile size 10G autoextend on maxsize 30G",
    ),
)

ORACLE_COMMON_SYSTEM_PARAMETERS: tuple[tuple[str, str, str], ...] = (
    ("result_cache_max_size", "0", "alter system set result_cache_max_size=0 scope=spfile"),
    ("processes", "10000", "alter system set processes=10000 scope=spfile"),
    ("db_files", "2000", "alter system set db_files=2000 scope=spfile"),
    ("standby_file_management", "AUTO", "alter system set standby_file_management='AUTO' scope=spfile"),
    ("archive_lag_target", "1800", "alter system set archive_lag_target=1800 scope=spfile"),
    ("control_file_record_keep_time", "30", "alter system set control_file_record_keep_time=30 scope=spfile"),
    ("session_cached_cursors", "300", "alter system set session_cached_cursors=300 scope=spfile"),
    ("open_cursors", "2000", "alter system set open_cursors=2000 scope=spfile"),
    ("remote_login_passwordfile", "EXCLUSIVE", "alter system set remote_login_passwordfile='EXCLUSIVE' scope=spfile"),
    ("resource_manager_plan", "", "alter system set resource_manager_plan='' scope=spfile"),
    ("statistics_level", "TYPICAL", "alter system set statistics_level='TYPICAL' scope=spfile"),
    ("timed_statistics", "TRUE", "alter system set timed_statistics=TRUE scope=spfile"),
    ("undo_management", "AUTO", "alter system set undo_management='AUTO' scope=spfile"),
    ("db_cache_advice", "OFF", "alter system set db_cache_advice='OFF' scope=spfile"),
    ("audit_trail", "NONE", "alter system set audit_trail='NONE' scope=spfile"),
    ("audit_sys_operations", "FALSE", "alter system set audit_sys_operations=false scope=spfile"),
    ("_ash_size", "104857600", 'alter system set "_ash_size"=104857600 scope=spfile'),
    ("_resource_manager_always_off", "TRUE", 'alter system set "_resource_manager_always_off"=TRUE scope=spfile'),
    ("undo_retention", "10", 'alter system set "undo_retention"=10 scope=spfile'),
    ("db_block_checking", "FALSE", "alter system set db_block_checking=FALSE scope=spfile"),
    ("db_block_checksum", "FALSE", "alter system set db_block_checksum=FALSE scope=spfile"),
    ("_db_always_check_system_ts", "FALSE", 'alter system set "_db_always_check_system_ts"=FALSE scope=spfile'),
    ("_use_adaptive_log_file_sync", "FALSE", 'alter system set "_use_adaptive_log_file_sync"=\'FALSE\' scope=spfile'),
    ("_disable_selftune_checkpointing", "TRUE", 'alter system set "_disable_selftune_checkpointing"=TRUE scope=spfile'),
    ("_fast_cursor_reexecute", "TRUE", 'alter system set "_fast_cursor_reexecute"=TRUE scope=spfile'),
    ("_sort_elimination_cost_ratio", "1", 'alter system set "_sort_elimination_cost_ratio"=1 scope=spfile'),
    ("trace_enabled", "FALSE", "alter system set trace_enabled=FALSE scope=spfile"),
    ("pre_page_sga", "FALSE", "alter system set pre_page_sga=FALSE scope=spfile"),
    ("commit_logging", "BATCH", "alter system set commit_logging='BATCH' scope=spfile"),
    ("_lm_res_hash_bucket", "4194250", 'alter system set "_lm_res_hash_bucket"=4194250 scope=spfile'),
    ("_column_tracking_level", "1", 'alter system set "_column_tracking_level"=1 scope=spfile'),
    ("_lm_share_lock_opt", "FALSE", 'alter system set "_lm_share_lock_opt"=false scope=spfile'),
)


def _oracle_normalize_parameter_value(value: Any) -> str:
    normalized = "" if value is None else str(value).strip()
    if len(normalized) >= 2 and normalized[0] == normalized[-1] and normalized[0] in {"'", '"'}:
        normalized = normalized[1:-1].strip()
    lowered = normalized.lower()
    if lowered in {"true", "false"}:
        return lowered
    try:
        number = float(normalized)
        if number.is_integer():
            return str(int(number))
    except (TypeError, ValueError):
        pass
    return normalized.lower()


def _oracle_spfile_parameter_value(cursor: Any, name: str) -> Optional[str]:
    cursor.execute(
        """
        select value
          from v$spparameter
         where lower(name) = lower(:1)
           and isspecified = 'TRUE'
         order by case when sid = '*' then 0 else 1 end
        """,
        [name],
    )
    row = cursor.fetchone()
    return None if not row else row[0]


def _oracle_tablespace_has_target_file(cursor: Any, view_name: str, tablespace_name: str) -> bool:
    cursor.execute(
        f"""
        select count(*)
          from {view_name}
         where tablespace_name = :1
           and autoextensible = 'YES'
           and bytes >= 10 * 1024 * 1024 * 1024
           and maxbytes >= 30 * 1024 * 1024 * 1024
        """,
        [tablespace_name],
    )
    row = cursor.fetchone()
    return bool(row and int(row[0] or 0) > 0)


def _wait_for_oracle_reconnect(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
    timeout_seconds: int = 900,
) -> None:
    import oracledb

    deadline = monotonic() + timeout_seconds
    last_error = ""
    _emit_log(log_callback, "开始等待 Oracle 数据库恢复连接...")
    while monotonic() < deadline:
        _check_cancel(cancel_callback)
        try:
            with oracledb.connect(user=config.user, password=config.password, dsn=_oracle_easy_connect(config)) as connection:
                with connection.cursor() as cursor:
                    try:
                        cursor.execute("select open_mode from v$database")
                        row = cursor.fetchone()
                        open_mode = str(row[0] if row else "").upper()
                        if open_mode and open_mode != "READ WRITE":
                            raise RuntimeError(f"数据库当前 open_mode={open_mode}，等待 READ WRITE")
                    except Exception as view_exc:
                        view_message = str(view_exc)
                        if "open_mode=" in view_message:
                            raise
                        cursor.execute("select 1 from dual")
                        cursor.fetchone()
                    cursor.execute("select count(*) from all_users")
                    cursor.fetchone()
            _emit_log(log_callback, "Oracle 数据库已恢复到可查询状态，继续执行后续压测步骤。")
            return
        except Exception as exc:
            last_error = str(exc)
            sleep(10)
    raise BenchmarkValidationError(f"执行 Oracle 通用配置后等待数据库恢复超时。最后一次连接错误: {last_error}")


def _shutdown_oracle_immediate_and_wait(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> None:
    import oracledb

    options = _parse_cli_options(config.extra_options)
    strict_shutdown = _option_enabled(options, "oracle_shutdown_strict", False)
    _emit_log(log_callback, "Oracle 通用配置已变更，按文档执行 shutdown immediate 并等待实例自动恢复。")
    shutdown_errors: list[str] = []
    shutdown_users = ["sys"]
    if str(config.user or "").strip().lower() != "sys":
        shutdown_users.append(str(config.user or "").strip())

    for shutdown_user in shutdown_users:
        if not shutdown_user:
            continue
        try:
            _emit_log(log_callback, f"尝试使用 {shutdown_user} as sysdba 执行 shutdown immediate...")
            with oracledb.connect(
                user=shutdown_user,
                password=config.password,
                dsn=_oracle_easy_connect(config),
                mode=oracledb.AUTH_MODE_SYSDBA,
            ) as connection:
                connection.shutdown(mode=oracledb.DBSHUTDOWN_IMMEDIATE)
            _emit_log(log_callback, f"Oracle shutdown immediate 已提交: {shutdown_user} as sysdba。")
            _wait_for_oracle_reconnect(config, log_callback=log_callback, cancel_callback=cancel_callback)
            return
        except Exception as exc:
            shutdown_errors.append(f"{shutdown_user} as sysdba: {exc}")

    message = "；".join(shutdown_errors)
    if strict_shutdown:
        raise BenchmarkValidationError(
            "Oracle 通用配置已写入 spfile，但执行 shutdown immediate 失败。"
            " 平台已尝试 sys/<页面密码> as sysdba；请确认 sys 密码与页面密码一致、"
            f"并允许远程 SYSDBA 登录。原始错误: {message}"
        )
    else:
        _emit_log(
            log_callback,
            "Oracle shutdown immediate 未能由平台远程执行，已改为继续后续压测。"
            " 平台已尝试 sys/<页面密码> as sysdba；如果仍失败，请确认 sys 密码与页面密码一致、"
            "remote_login_passwordfile 为 EXCLUSIVE，并允许远程 SYSDBA 登录。"
            " 文档中的 spfile 参数需在数据库节点执行 shutdown immediate 后才会完全生效。"
            f" 原始错误: {message}",
        )
        return


def _apply_oracle_common_configuration(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> None:
    import oracledb

    options = _parse_cli_options(config.extra_options)
    if not _option_enabled(options, "oracle_common_config", True):
        _emit_log(log_callback, "已跳过 Oracle 数据库通用配置: --oracle-common-config false")
        return

    _init_oracle_client(log_callback=log_callback)
    changed = False
    _emit_log(log_callback, "开始执行 Oracle 数据库通用配置，来源: 测试文档 1.3/3.3 数据库通用配置。")
    try:
        with oracledb.connect(user=config.user, password=config.password, dsn=_oracle_easy_connect(config)) as connection:
            with connection.cursor() as cursor:
                for label, view_name, tablespace_name, statement in ORACLE_COMMON_TABLESPACE_SQL:
                    _check_cancel(cancel_callback)
                    if _oracle_tablespace_has_target_file(cursor, view_name, tablespace_name):
                        _emit_log(log_callback, f"Oracle {label} 表空间已存在 10G/30G autoextend 文件，跳过扩容。")
                        continue
                    cursor.execute(statement)
                    changed = True
                    _emit_log(log_callback, f"Oracle {label} 表空间扩容完成: {statement}")

                for name, expected, statement in ORACLE_COMMON_SYSTEM_PARAMETERS:
                    _check_cancel(cancel_callback)
                    current = _oracle_spfile_parameter_value(cursor, name)
                    if current is not None and _oracle_normalize_parameter_value(current) == _oracle_normalize_parameter_value(expected):
                        continue
                    cursor.execute(statement)
                    changed = True
                    _emit_log(log_callback, f"Oracle spfile 参数已设置: {statement}")
    except BenchmarkValidationError:
        raise
    except Exception as exc:
        raise BenchmarkValidationError(
            "执行 Oracle 数据库通用配置失败。请确认页面填写的是当前实例的 system 用户和正确密码，"
            "且该账号具备查询 DBA 视图、扩容表空间和 ALTER SYSTEM 权限。"
            f" 原始错误: {exc}"
        ) from exc

    if changed:
        _shutdown_oracle_immediate_and_wait(config, log_callback=log_callback, cancel_callback=cancel_callback)
    else:
        _emit_log(log_callback, "Oracle 数据库通用配置已满足文档要求，无需重启。")


def _oracle_grant_identifier(identifier: str) -> str:
    value = str(identifier or "").strip()
    if not value:
        raise BenchmarkValidationError("Oracle 授权用户名不能为空。")
    if re.fullmatch(r"[A-Za-z][A-Za-z0-9_$#]*", value):
        return value.upper()
    return '"' + value.replace('"', '""') + '"'


def _oracle_quoted_password(password: str) -> str:
    # Oracle passwords containing punctuation such as "!" must be double-quoted
    # in CREATE/ALTER USER statements, while clients still log in with the raw value.
    return '"' + str(password or "").replace('"', '""') + '"'


def _oracle_connect_for_awr_grant(config: BenchmarkConfig):
    import oracledb

    kwargs: Dict[str, Any] = {
        "user": config.user,
        "password": config.password,
        "dsn": _oracle_easy_connect(config),
    }
    return oracledb.connect(**kwargs)


ORACLE_TPCC_TABLESPACE = "TPCCTAB"


def _ensure_oracle_tpcc_tablespace(
    config: BenchmarkConfig,
    cursor: Any,
    log_callback: Optional[LogCallback] = None,
    tablespace_name: str = ORACLE_TPCC_TABLESPACE,
) -> None:
    tablespace = _oracle_grant_identifier(tablespace_name)
    cursor.execute("select count(*) from dba_tablespaces where tablespace_name = :1", [tablespace_name.upper()])
    row = cursor.fetchone()
    if row and int(row[0] or 0) > 0:
        _emit_log(log_callback, f"Oracle TPCC 表空间已存在，按文档跳过创建: {tablespace}")
        return

    statement = f"create tablespace {tablespace} datafile size 30G"
    cursor.execute(statement)
    _emit_log(log_callback, f"Oracle TPCC 表空间已按文档创建: {statement}")


def _grant_oracle_awr_permissions(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    _init_oracle_client(log_callback=log_callback)
    tpcc_user, _ = _oracle_tpcc_credentials(config)
    grantees: list[str] = [str(config.user or "").strip()]
    tpcc_user = str(tpcc_user or "").strip()
    if tpcc_user and tpcc_user.upper() != str(config.user or "").strip().upper():
        grantees.append(tpcc_user)
    _emit_log(log_callback, "开始自动配置 Oracle AWR 权限: 使用 system 账号执行 DBMS_WORKLOAD_REPOSITORY 授权。")
    try:
        with _oracle_connect_for_awr_grant(config) as connection:
            with connection.cursor() as cursor:
                for index, grantee in enumerate(grantees):
                    required_grant = index == 0
                    grant_target = _oracle_grant_identifier(grantee)
                    try:
                        cursor.execute(f"grant execute on sys.dbms_workload_repository to {grant_target}")
                        _emit_log(log_callback, f"已授予 AWR 执行权限: grant execute on sys.dbms_workload_repository to {grant_target}")
                    except Exception as exc:
                        message = str(exc)
                        if "ORA-01749" in message:
                            _emit_log(log_callback, f"AWR 执行权限已跳过: {grant_target} 不能对自身重复授权。")
                            continue
                        if not required_grant and "ORA-01031" in message:
                            _emit_log(
                                log_callback,
                                f"AWR 执行权限未授予 {grant_target}，但该权限只要求压测控制账号具备，继续执行: {message}",
                            )
                            continue
                        raise BenchmarkValidationError(
                            "自动授予 Oracle AWR 权限失败。请确认页面填写的是当前 PDB 的 system 用户和正确密码，"
                            "或在目标 PDB 中手工执行: grant execute on sys.dbms_workload_repository to "
                            f"{grant_target}。原始错误: {message}"
                        ) from exc
                    try:
                        cursor.execute(f"grant select_catalog_role to {grant_target}")
                        _emit_log(log_callback, f"已授予目录查询角色: grant select_catalog_role to {grant_target}")
                    except Exception as exc:
                        _emit_log(log_callback, f"目录查询角色自动授权未完成，继续执行: {exc}")
    except BenchmarkValidationError:
        raise
    except Exception as exc:
        raise BenchmarkValidationError(
            "自动配置 Oracle AWR 权限失败。请确认页面填写的是当前 PDB 的 system 用户和正确密码，"
            f"并且连接的是当前 Service Name 对应的 PDB。原始错误: {exc}"
        ) from exc


def _ensure_oracle_tpcc_user(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    password_override: Optional[str] = None,
) -> None:
    import oracledb

    _init_oracle_client(log_callback=log_callback)
    tpcc_user, tpcc_password = _oracle_tpcc_credentials(config)
    if password_override is not None:
        tpcc_password = password_override
    tpcc_user = str(tpcc_user or "").strip()
    if not tpcc_user:
        raise BenchmarkValidationError("Oracle TPCC 用户不能为空。")
    if not tpcc_password:
        raise BenchmarkValidationError("Oracle TPCC 密码不能为空。")

    grant_target = _oracle_grant_identifier(tpcc_user)
    quoted_password = _oracle_quoted_password(tpcc_password)
    _emit_log(log_callback, f"开始内置配置 Oracle TPCC 用户: {grant_target}")
    try:
        with _oracle_connect_for_awr_grant(config) as connection:
            with connection.cursor() as cursor:
                _ensure_oracle_tpcc_tablespace(config, cursor, log_callback=log_callback)
                cursor.execute("select count(*) from all_users where username = :1", [tpcc_user.upper()])
                row = cursor.fetchone()
                exists = bool(row and int(row[0] or 0) > 0)
                if exists:
                    cursor.execute(
                        f"alter user {grant_target} identified by {quoted_password} "
                        f"default tablespace {ORACLE_TPCC_TABLESPACE} temporary tablespace temp account unlock"
                    )
                    _emit_log(log_callback, f"已重置并解锁 Oracle TPCC 用户: {grant_target}")
                else:
                    cursor.execute(
                        f"create user {grant_target} identified by {quoted_password} "
                        f"default tablespace {ORACLE_TPCC_TABLESPACE} temporary tablespace temp"
                    )
                    _emit_log(log_callback, f"已创建 Oracle TPCC 用户: {grant_target}")
                for statement in (
                    f"grant connect,resource,create view to {grant_target}",
                    f"alter user {grant_target} quota unlimited on {ORACLE_TPCC_TABLESPACE}",
                ):
                    cursor.execute(statement)
                _emit_log(log_callback, f"Oracle TPCC 用户权限配置完成: {grant_target}")
    except Exception as exc:
        message = str(exc)
        if "ORA-00959" in message:
            raise BenchmarkValidationError(
                "自动配置 Oracle TPCC 用户失败: 目标 PDB 中无法创建或使用 TPCCTAB/TEMP 表空间。"
                " 请确认 Oracle 实例可创建数据文件，或先在数据库中创建 TPCCTAB 表空间后重试。"
                f" 原始错误: {message}"
            ) from exc
        raise BenchmarkValidationError(
            "自动配置 Oracle TPCC 用户失败。请确认页面填写的是当前 PDB 的 system 用户和正确密码，"
            f"并具备 create user/grant/quota 权限。原始错误: {message}"
        ) from exc


def _ensure_oracle_hammerdb_tablespace(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    try:
        with _oracle_connect_for_awr_grant(config) as connection:
            with connection.cursor() as cursor:
                _ensure_oracle_tpcc_tablespace(config, cursor, log_callback=log_callback)
    except Exception as exc:
        raise BenchmarkValidationError(
            "Oracle HammerDB 表空间创建失败。文档要求先执行: create tablespace tpcctab datafile size 30G。"
            f" 原始错误: {exc}"
        ) from exc


def _hammerdb_oracle_build_vu(concurrency: int, warehouses: str, requested_build_vu: Optional[str] = None) -> str:
    try:
        requested = int(float(str(requested_build_vu).strip())) if requested_build_vu is not None else 16
    except (TypeError, ValueError):
        requested = 16
    return str(max(requested, 1))


def _hammerdb_oracle_script(
    config: BenchmarkConfig,
    concurrency: int,
    build_schema: bool = False,
    tpcc_password_override: Optional[str] = None,
) -> str:
    options = _parse_cli_options(config.extra_options)
    warehouses = options.get("warehouses", str(config.table_size))
    build_vu = _hammerdb_oracle_build_vu(concurrency, warehouses, options.get("build_vu"))
    connect = options.get("connect", _oracle_easy_connect(config))
    rampup_minutes = str(max(int(config.warmup_seconds / 60), 0))
    duration_minutes = str(max(int(config.duration_seconds / 60), 1))
    if build_schema:
        script_lines = [
            "dbset db ora",
            "dbset bm TPC-C",
            f"diset connection system_user {config.user}",
            f"diset connection system_password {config.password}",
            f"diset connection instance {connect}",
            "diset connection rac 0",
            f"diset tpcc count_ware {warehouses}",
            f"diset tpcc num_vu {build_vu}",
            "diset tpcc partition true",
            "diset tpcc allwarehouse true",
            "vuset logtotemp 1",
        ]
        script_lines.extend(["buildschema", "waittocomplete", "quit"])
        return "\n".join(script_lines) + "\n"
    script_lines = [
        "dbset db ora",
        "dbset bm TPC-C",
        f"diset connection system_user {config.user}",
        f"diset connection system_password {config.password}",
        f"diset connection instance {connect}",
        "diset tpcc total_iterations 99999999999",
        "diset tpcc ora_driver timed",
        f"diset tpcc rampup {rampup_minutes}",
        f"diset tpcc duration {duration_minutes}",
        "loadscript",
        f"vuset vu {concurrency}",
        "vuset delay 5",
        "vuset repeat 5",
        "vuset logtotemp 1",
        "vucreate",
        "vurun",
        "waittocomplete",
        "vudestroy",
        "quit",
    ]
    return "\n".join(script_lines) + "\n"


def _sqlserver_build_vu(warehouses: str, requested_build_vu: Optional[str] = None) -> str:
    try:
        warehouse_count = max(int(float(str(warehouses).strip())), 1)
    except (TypeError, ValueError):
        warehouse_count = 1
    try:
        requested = int(float(str(requested_build_vu).strip())) if requested_build_vu is not None else 16
    except (TypeError, ValueError):
        requested = 16
    return str(max(min(requested, warehouse_count), 1))


def _sqlserver_hammerdb_base_script_lines(config: BenchmarkConfig) -> List[str]:
    options = _parse_cli_options(config.extra_options)
    database = options.get("mssqls_dbase") or options.get("database") or config.database
    odbc_driver = options.get("odbc_driver", "ODBC Driver 17 for SQL Server")
    return [
        "dbset db mssqls",
        "dbset bm tpc-c",
        f"diset connection mssqls_linux_server {_tcl_braced(config.host)}",
        "diset connection mssqls_tcp true",
        f"diset connection mssqls_port {config.port}",
        f"diset connection mssqls_linux_odbc {_tcl_braced(odbc_driver)}",
        "diset connection mssqls_linux_authent sql",
        f"diset connection mssqls_uid {_tcl_braced(config.user)}",
        f"diset connection mssqls_pass {_tcl_braced(config.password)}",
        f"diset tpcc mssqls_dbase {_tcl_braced(database)}",
    ]


def _hammerdb_sqlserver_script(config: BenchmarkConfig, concurrency: int, build_schema: bool = False) -> str:
    options = _parse_cli_options(config.extra_options)
    warehouses = options.get("warehouses", str(config.table_size))
    build_vu = _sqlserver_build_vu(warehouses, options.get("build_vu"))
    total_iterations = options.get("total_iterations", "1000000")
    rampup_minutes = str(max(int(config.warmup_seconds / 60), 0))
    duration_minutes = str(max(int(config.duration_seconds / 60), 1))
    script_lines = _sqlserver_hammerdb_base_script_lines(config) + [
        f"diset tpcc mssqls_count_ware {warehouses}",
        f"diset tpcc mssqls_num_vu {build_vu}",
        "vuset logtotemp 1",
    ]
    if build_schema:
        script_lines.extend(["buildschema", "waittocomplete", "quit"])
        return "\n".join(script_lines) + "\n"
    script_lines.extend(
        [
            f"diset tpcc mssqls_total_iterations {total_iterations}",
            "diset tpcc mssqls_driver timed",
            f"diset tpcc mssqls_rampup {rampup_minutes}",
            f"diset tpcc mssqls_duration {duration_minutes}",
            "loadscript",
            f"vuset vu {concurrency}",
            "vuset delay 500",
            "vuset repeat 500",
            "vuset iterations 1",
            "vuset logtotemp 1",
            "vuset showoutput 1",
            "vuset unique 1",
            "vucreate",
            "vurun",
            "waittocomplete",
            "vudestroy",
            "quit",
        ]
    )
    return "\n".join(script_lines) + "\n"


def _hammerdb_sqlserver_deleteschema_script(config: BenchmarkConfig) -> str:
    script_lines = _sqlserver_hammerdb_base_script_lines(config) + [
        "deleteschema",
        "waittocomplete",
        "quit",
    ]
    return "\n".join(script_lines) + "\n"


def _hammerdb_oracle_driver_config_script(config: BenchmarkConfig, output_path: str) -> str:
    options = _parse_cli_options(config.extra_options)
    warehouses = options.get("warehouses", str(config.table_size))
    build_vu = options.get("build_vu", "16")
    connect = options.get("connect", _oracle_easy_connect(config))
    rampup_minutes = str(max(int(config.warmup_seconds / 60), 0))
    duration_minutes = str(max(int(config.duration_seconds / 60), 1))
    script_lines = [
        "dbset db ora",
        "dbset bm TPC-C",
        f"diset connection system_user {config.user}",
        f"diset connection system_password {config.password}",
        f"diset connection instance {connect}",
        "diset connection rac 0",
        f"diset tpcc count_ware {warehouses}",
        f"diset tpcc num_vu {build_vu}",
        "diset tpcc partition true",
        "diset tpcc allwarehouse true",
        "diset tpcc total_iterations 99999999999",
        "diset tpcc ora_driver timed",
        f"diset tpcc rampup {rampup_minutes}",
        f"diset tpcc duration {duration_minutes}",
        "loadscript",
        f"savescript {output_path}",
        "quit",
    ]
    return "\n".join(script_lines) + "\n"


def _patch_hammerdb_oracle_driver_without_awr(driver_path: str) -> None:
    path = Path(driver_path)
    script = path.read_text()
    script = script.replace(
        '''            } else {
                set sql1 "BEGIN dbms_workload_repository.create_snapshot(); END;"
                oraparse $curn1 $sql1
            }
''',
        '''            } else {
                set sql1 "select 1 from dual"
            }
''',
    )
    start = script.find('if { $timesten } {')
    start = script.find('if { $timesten } {', start + 1)
    end = script.find('if { $mode eq "Primary" }', start)
    if start == -1 or end == -1:
        raise BenchmarkValidationError("未能定位 HammerDB Oracle timed driver 的 AWR 代码块。")
    replacement = r'''puts "Rampup complete, Taking start Transaction Count."
            set start_trans 0
            set sql4 "select sum(d_next_o_id) from district"
            set start_nopm [ standsql $curn2 $sql4 ]
            puts "Timing test period of $duration in minutes"
            set testtime 0
            set durmin $duration
            set duration [ expr $duration*60000 ]
            while {$testtime != $duration} {
                if { [ tsv::get application abort ] } { break } else { after 6000 }
                set testtime [ expr $testtime+6000 ]
                if { ![ expr {$testtime % 60000} ] } {
                    puts -nonewline  "[ expr $testtime / 60000 ]  ...,"
                }
            }
            if { [ tsv::get application abort ] } { break }
            puts "Test complete, Taking end Transaction Count."
            set end_nopm [ standsql $curn2 $sql4 ]
            set nopm [ expr {($end_nopm - $start_nopm)/$durmin} ]
            set tpm $nopm
            puts "[ expr $totalvirtualusers - 1 ] Active Virtual Users configured"
            puts [ testresult $nopm $tpm Oracle ]
            '''
    script = script[:start] + replacement + script[end:]
    path.write_text(script)


def _run_hammerdb_oracle_runload(
    config: BenchmarkConfig,
    concurrency: int,
    timeout_seconds: int,
    log_callback: Optional[LogCallback],
    cancel_callback: Optional[CancelCallback],
) -> str:
    return _run_hammerdb_script(
        _hammerdb_oracle_script(config, concurrency, build_schema=False),
        timeout_seconds=timeout_seconds,
        log_callback=log_callback,
        log_label=f"{concurrency}并发Oracle HammerDB测试",
        secrets=[config.password],
        cancel_callback=cancel_callback,
        artifact_dir=config.artifact_dir,
    )


def _safe_artifact_name(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip())
    return safe.strip("._") or "artifact"


def _hammerdb_artifact_dir(artifact_dir: str) -> Optional[Path]:
    if not artifact_dir:
        return None
    path = Path(artifact_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _persist_hammerdb_temp_log(artifact_dir: Optional[Path], log_label: str) -> Optional[Path]:
    if artifact_dir is None:
        return None
    candidates = sorted(
        Path("/tmp").glob("hammerdb*.log"),
        key=lambda item: item.stat().st_mtime if item.exists() else 0,
        reverse=True,
    )
    if not candidates:
        return None
    source = candidates[0]
    target = artifact_dir / f"{_safe_artifact_name(log_label)}.hammerdb.log"
    shutil.copyfile(source, target)
    return target


def _run_hammerdb_script(
    script: str,
    timeout_seconds: int,
    log_callback: Optional[LogCallback],
    log_label: str,
    secrets: Optional[List[str]] = None,
    cancel_callback: Optional[CancelCallback] = None,
    ignored_ora_codes: Optional[set[str]] = None,
    artifact_dir: str = "",
) -> str:
    hammerdbcli = _find_hammerdbcli()
    hammerdb_home = str(Path(hammerdbcli).resolve().parent)
    _check_cancel(cancel_callback)
    artifacts = _hammerdb_artifact_dir(artifact_dir)
    script_prefix = f"{_safe_artifact_name(log_label)}_"
    temp_kwargs: Dict[str, Any] = {
        "mode": "w",
        "suffix": ".tcl",
        "prefix": script_prefix,
        "delete": False,
    }
    if artifacts is not None:
        temp_kwargs["dir"] = str(artifacts)
    with tempfile.NamedTemporaryFile(**temp_kwargs) as script_file:
        script_file.write(script)
        script_path = script_file.name
    _emit_log(log_callback, f"{log_label}启动: {hammerdbcli} auto {script_path}")
    try:
        process_env = os.environ.copy()
        if artifacts is not None:
            process_env["TMPDIR"] = str(artifacts)
        process_env.setdefault("ORACLE_HOME", "/opt/oracle/instantclient")
        process_env.setdefault("TNS_ADMIN", "/opt/oracle/instantclient/network/admin")
        process_env["LD_LIBRARY_PATH"] = ":".join(
            filter(
                None,
                [
                    "/opt/oracle/instantclient",
                    "/opt/oracle/instantclient/lib",
                    process_env.get("LD_LIBRARY_PATH", ""),
                ],
            )
        )
        process = subprocess.Popen(
            [hammerdbcli, "auto", script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=hammerdb_home,
            env=process_env,
        )
        lines: List[str] = []
        deadline = monotonic() + timeout_seconds
        assert process.stdout is not None
        while True:
            if cancel_callback and cancel_callback():
                process.kill()
                raise BenchmarkCancelledError("测试已终止。")
            if monotonic() > deadline:
                process.kill()
                raise BenchmarkValidationError(f"{log_label}执行超时，超过 {timeout_seconds} 秒。")
            line = process.stdout.readline()
            if line:
                line = line.rstrip()
                lines.append(line)
                if line.strip():
                    _emit_log(log_callback, "[hammerdb] " + _masked_command([line], secrets or []))
            elif process.poll() is not None:
                break
        output = "\n".join(lines).strip()
        return_code = process.wait()
        temp_log_path = _persist_hammerdb_temp_log(artifacts, log_label)
        if temp_log_path is not None:
            _emit_log(log_callback, f"{log_label}日志已保存: {temp_log_path}")
        stty_cleanup_error = 'invalid command name "stty"' in output
        if stty_cleanup_error:
            output = output.split('invalid command name "stty"', 1)[0].strip()
            _emit_log(log_callback, f"{log_label}结束时出现 HammerDB TTY 清理告警，已忽略。")
        failure_message = _extract_hammerdb_failure(output, ignored_ora_codes=ignored_ora_codes)
        if failure_message:
            raise BenchmarkValidationError(failure_message)
        success_detected = _hammerdb_output_indicates_success(output)
        if return_code != 0 and not success_detected:
            raise BenchmarkValidationError(output or f"{log_label}执行失败。")
        if return_code != 0 and success_detected:
            _emit_log(log_callback, f"{log_label}返回非 0，但已检测到 HammerDB 成功标记，按成功处理。")
        _emit_log(log_callback, f"{log_label}完成。")
        return output
    finally:
        if artifacts is None:
            Path(script_path).unlink(missing_ok=True)


HAMMERDB_FAILURE_PATTERNS = [
    re.compile(r"\bFINISHED FAILED\b", re.IGNORECASE),
    re.compile(r"\bError in Virtual User\b", re.IGNORECASE),
    re.compile(r"\bORA-[0-9]{5}\b", re.IGNORECASE),
    re.compile(r"\bFailed to load Oratcl\b", re.IGNORECASE),
    re.compile(r"Monitor failed to notify ready state", re.IGNORECASE),
]


def _extract_hammerdb_failure(output: str, ignored_ora_codes: Optional[set[str]] = None) -> Optional[str]:
    ignored_ora_codes = {code.upper() for code in (ignored_ora_codes or set())}
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    for line in lines:
        if line.startswith("ORA-"):
            code = line.split(":", 1)[0].upper()
            if code in ignored_ora_codes:
                continue
            return line
    for line in lines:
        if "Error in Virtual User" in line or "Failed to load Oratcl" in line:
            if ignored_ora_codes and any(code in line.upper() for code in ignored_ora_codes):
                continue
            return line
    for line in lines:
        if line.startswith("Error:"):
            return line
    if re.search(r"Monitor failed to notify ready state", output, re.IGNORECASE):
        return "Monitor failed to notify ready state"
    if re.search(r"\bFINISHED FAILED\b", output, re.IGNORECASE):
        return "HammerDB 存在虚拟用户失败，请检查前文日志。"
    return None


def _oracle_tpcc_credentials(config: BenchmarkConfig) -> tuple[str, str]:
    options = _parse_cli_options(config.extra_options)
    return options.get("tpcc_user", "tpcc"), options.get("tpcc_pass", "tpcc")


def _check_oracle_tpcc_connectivity(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    import oracledb

    tpcc_user, tpcc_password = _oracle_tpcc_credentials(config)
    mode = _init_oracle_client(log_callback=log_callback)
    masked_password = "******" if tpcc_password else ""
    _emit_log(
        log_callback,
        f"开始检查 Oracle TPCC 用户连通性: oracledb {mode} connect -> host={config.host} port={config.port} service={config.database} user={tpcc_user} password={masked_password}",
    )
    try:
        with oracledb.connect(user=tpcc_user, password=tpcc_password, dsn=_oracle_easy_connect(config)) as connection:
            with connection.cursor() as cursor:
                cursor.execute("select 1 from dual")
                cursor.fetchone()
        _emit_log(log_callback, f"Oracle TPCC 用户连通性检查通过: user={tpcc_user}")
    except Exception as exc:
        raise BenchmarkValidationError(
            f"Oracle TPCC 用户认证失败，请检查页面中的 TPCC 用户/密码是否与数据库中实际创建的一致。原始错误: {exc}"
        ) from exc


TPCC_TABLES = [
    "ITEM",
    "WAREHOUSE",
    "DISTRICT",
    "CUSTOMER",
    "HISTORY",
    "ORDERS",
    "NEW_ORDER",
    "ORDER_LINE",
    "STOCK",
]


def _oracle_tpcc_expected_warehouses(config: BenchmarkConfig) -> int:
    options = _parse_cli_options(config.extra_options)
    raw = options.get("warehouses", str(config.table_size))
    try:
        return max(int(float(str(raw).strip())), 1)
    except (TypeError, ValueError):
        return max(int(config.table_size or 1), 1)


def _oracle_tpcc_schema_exists(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> bool:
    import oracledb

    tpcc_user, tpcc_password = _oracle_tpcc_credentials(config)
    mode = _init_oracle_client(log_callback=log_callback)
    try:
        with oracledb.connect(user=tpcc_user, password=tpcc_password, dsn=_oracle_easy_connect(config)) as connection:
            with connection.cursor() as cursor:
                binds = ",".join(f":{index + 1}" for index in range(len(TPCC_TABLES)))
                cursor.execute(
                    f"select count(*) from user_tables where table_name in ({binds})",
                    TPCC_TABLES,
                )
                row = cursor.fetchone()
        table_count = int(row[0] or 0) if row else 0
        if table_count > 0:
            _emit_log(log_callback, f"检测到 Oracle TPCC schema 已存在业务表: user={tpcc_user}, tables={table_count}")
            return True
        _emit_log(log_callback, f"Oracle TPCC schema 当前无业务表: user={tpcc_user}")
        return False
    except Exception as exc:
        _emit_log(log_callback, f"Oracle TPCC schema 预检查跳过: oracledb {mode} connect user={tpcc_user} failed: {exc}")
        return False


def _oracle_tpcc_schema_stats(config: BenchmarkConfig) -> Dict[str, Any]:
    import oracledb

    tpcc_user, tpcc_password = _oracle_tpcc_credentials(config)
    with oracledb.connect(user=tpcc_user, password=tpcc_password, dsn=_oracle_easy_connect(config)) as connection:
        with connection.cursor() as cursor:
            binds = ",".join(f":{index + 1}" for index in range(len(TPCC_TABLES)))
            cursor.execute(
                f"""
                select table_name, num_rows
                  from user_tables
                 where table_name in ({binds})
                """,
                TPCC_TABLES,
            )
            estimated_rows = {str(name).upper(): int(rows or 0) for name, rows in cursor.fetchall()}

            direct_counts: Dict[str, int] = {}
            for table_name in ("ITEM", "WAREHOUSE", "DISTRICT"):
                cursor.execute(f"select count(*) from {table_name}")
                row = cursor.fetchone()
                direct_counts[table_name] = int(row[0] or 0) if row else 0

            cursor.execute("select nvl(sum(d_next_o_id), 0) from district")
            row = cursor.fetchone()
            next_order_sum = int(row[0] or 0) if row else 0

            cursor.execute("select nvl(round(sum(bytes) / 1024 / 1024, 2), 0) from user_segments")
            row = cursor.fetchone()
            size_mb = float(row[0] or 0) if row else 0.0

    return {
        "estimated_rows": estimated_rows,
        "direct_counts": direct_counts,
        "next_order_sum": next_order_sum,
        "size_mb": size_mb,
    }


def _oracle_tpcc_next_order_sum(config: BenchmarkConfig) -> int:
    return int(_oracle_tpcc_schema_stats(config).get("next_order_sum") or 0)


def _validate_oracle_tpcc_schema_ready(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    tpcc_user, _ = _oracle_tpcc_credentials(config)
    expected_warehouses = _oracle_tpcc_expected_warehouses(config)
    try:
        stats = _oracle_tpcc_schema_stats(config)
    except Exception as exc:
        raise BenchmarkValidationError(f"Oracle TPCC schema 校验失败: 无法读取 {tpcc_user} 下的 TPCC 对象。原始错误: {exc}") from exc

    estimated_rows: Dict[str, int] = stats["estimated_rows"]
    direct_counts: Dict[str, int] = stats["direct_counts"]
    missing_tables = [table_name for table_name in TPCC_TABLES if table_name not in estimated_rows]
    if missing_tables:
        raise BenchmarkValidationError(
            f"Oracle TPCC schema 不完整: 用户 {tpcc_user} 缺少表 {', '.join(missing_tables)}，"
            "buildschema 未真正完成，不能开始压测。"
        )

    expected_direct_counts = {
        "ITEM": 100000,
        "WAREHOUSE": expected_warehouses,
        "DISTRICT": expected_warehouses * 10,
    }
    mismatched = []
    for table_name, expected_count in expected_direct_counts.items():
        actual_count = int(direct_counts.get(table_name, 0))
        if actual_count != expected_count:
            mismatched.append(f"{table_name}={actual_count}, expected={expected_count}")
    if mismatched:
        raise BenchmarkValidationError(
            "Oracle TPCC schema 数据量不符合 HammerDB TPROC-C 预期: "
            + "; ".join(mismatched)
            + "。请清理旧 schema 后重新 buildschema。"
        )

    if int(stats["next_order_sum"] or 0) <= 0:
        raise BenchmarkValidationError("Oracle TPCC schema 校验失败: DISTRICT.D_NEXT_O_ID 汇总为 0，无法产生有效 NOPM。")

    _emit_log(
        log_callback,
        "Oracle TPCC schema 校验通过: "
        f"user={tpcc_user}, warehouses={direct_counts.get('WAREHOUSE')}, "
        f"districts={direct_counts.get('DISTRICT')}, item={direct_counts.get('ITEM')}, "
        f"next_order_sum={stats['next_order_sum']}, segments={stats['size_mb']}MB",
    )


def _hammerdb_oracle_deleteschema_script(config: BenchmarkConfig) -> str:
    options = _parse_cli_options(config.extra_options)
    tpcc_user = options.get("tpcc_user", "tpcc")
    tpcc_pass = options.get("tpcc_pass", "tpcc")
    connect = options.get("connect", _oracle_easy_connect(config))
    script_lines = [
        "dbset db ora",
        "dbset bm TPC-C",
        f"diset connection system_user {config.user}",
        f"diset connection system_password {config.password}",
        f"diset connection instance {connect}",
        "diset connection rac 0",
        f"diset tpcc tpcc_user {tpcc_user}",
        f"diset tpcc tpcc_pass {tpcc_pass}",
        "deleteschema",
        "quit",
    ]
    return "\n".join(script_lines) + "\n"


HAMMERDB_SUCCESS_PATTERNS = [
    re.compile(r"ALL VIRTUAL USERS COMPLETE", re.IGNORECASE),
    re.compile(r"TEST SCHEMA COMPLETE", re.IGNORECASE),
    re.compile(r"SCHEMA BUILD COMPLETED", re.IGNORECASE),
    re.compile(r"TEST COMPLETE", re.IGNORECASE),
    re.compile(r"Success\s+\.\.\.\s+wrote script", re.IGNORECASE),
]


def _hammerdb_output_indicates_success(output: str) -> bool:
    return any(pattern.search(output) for pattern in HAMMERDB_SUCCESS_PATTERNS)


def _read_hammerdb_log_tail(config: Optional[BenchmarkConfig] = None, limit_bytes: int = 200000) -> str:
    log_candidates: list[Path] = []
    if config and config.artifact_dir:
        artifact_path = Path(config.artifact_dir)
        if artifact_path.exists():
            log_candidates.extend(sorted(artifact_path.glob("*.hammerdb.log"), key=lambda item: item.stat().st_mtime, reverse=True))
    log_candidates.append(Path("/tmp/hammerdb.log"))
    log_path = next((candidate for candidate in log_candidates if candidate.exists()), None)
    if log_path is None:
        return ""
    data = log_path.read_bytes()
    if len(data) > limit_bytes:
        data = data[-limit_bytes:]
    return data.decode(errors="ignore").strip()


def _parse_hammerdb_oracle_output(config: BenchmarkConfig, concurrency: int, output: str) -> ConcurrencyResult:
    parse_source = output
    nopm_values = [float(value) for value in re.findall(r"([0-9]+(?:\.[0-9]+)?)\s+NOPM", parse_source, flags=re.IGNORECASE)]
    tpm_values = [
        float(value)
        for value in re.findall(r"([0-9]+(?:\.[0-9]+)?)\s+(?:Oracle\s+)?TPM", parse_source, flags=re.IGNORECASE)
    ]
    if not nopm_values and not tpm_values:
        hammerdb_log = _read_hammerdb_log_tail(config)
        if hammerdb_log:
            parse_source = hammerdb_log
            nopm_values = [float(value) for value in re.findall(r"([0-9]+(?:\.[0-9]+)?)\s+NOPM", parse_source, flags=re.IGNORECASE)]
            tpm_values = [
                float(value)
                for value in re.findall(r"([0-9]+(?:\.[0-9]+)?)\s+(?:Oracle\s+)?TPM", parse_source, flags=re.IGNORECASE)
            ]
    qps = round(nopm_values[-1], 2) if nopm_values else 0.0
    tps = round(tpm_values[-1], 2) if tpm_values else qps
    if qps <= 0 and tps <= 0:
        raise BenchmarkValidationError(f"未能解析 HammerDB Oracle 输出，请检查执行结果。\n{parse_source[-1200:]}")
    if qps <= 0:
        raise BenchmarkValidationError(
            "Oracle HammerDB 输出 NOPM=0，这不是有效的 TPROC-C 压测结果。"
            " 数据库虽然产生了 TPM/提交回滚统计，但 DISTRICT.D_NEXT_O_ID 没有增长，"
            "通常表示 TPCC 交易没有真正执行成功、schema 数据不完整、连接到错误 PDB/Service，"
            "或旧数据/权限导致 New Order 交易未生效。"
            f"\n{parse_source[-1200:]}"
        )
    return ConcurrencyResult(
        concurrency=concurrency,
        elapsed_seconds=config.duration_seconds + config.warmup_seconds,
        total_transactions=int(tps),
        total_queries=int(qps),
        total_events=int(tps or qps),
        read_queries=0,
        write_queries=0,
        other_queries=0,
        failed_requests=0,
        qps=qps,
        tps=tps,
        avg_latency_ms=0.0,
        p95_latency_ms=0.0,
        min_latency_ms=0.0,
        max_latency_ms=0.0,
        success_rate=100.0,
        baseline_label="推荐基线" if qps > 0 else "待优化",
        sample_errors=_extract_sample_errors(parse_source),
        raw_output=parse_source,
    )


def _find_dm_benchmarksql_home() -> Path:
    configured_raw = os.environ.get("DM_BENCHMARKSQL_HOME", "").strip()
    candidates: List[Path] = []
    if configured_raw:
        candidates.append(Path(configured_raw))
    candidates.extend([Path("/app/vendor/benchmarksqlforDM"), BASE_DIR.parent / "vendor" / "benchmarksqlforDM"])
    for candidate in candidates:
        if (candidate / "tpcc_load.sh").exists() and (candidate / "tpcc_test.sh").exists() and (candidate / "bin" / "disql").exists():
            return candidate
    raise BenchmarkValidationError("未找到 benchmarksqlforDM 工具包。请将 benchmarksqlforDM-x86.tar 解压到 /app/vendor/benchmarksqlforDM。")


def _dm_allowed_partition_warehouses() -> set[int]:
    return {100, 200, 500, 1000, 2000}


def _dm_option(config: BenchmarkConfig, name: str, default: Any) -> str:
    options = _parse_cli_options(config.extra_options)
    return str(options.get(name.replace("_", "-"), options.get(name, default))).strip()


def _dm_table_type(config: BenchmarkConfig) -> str:
    return _dm_option(config, "table_type", "12") or "12"


def _dm_tbs_size(config: BenchmarkConfig) -> str:
    return _dm_option(config, "tbs_size", "10240") or "10240"


def _dm_loadworkers(config: BenchmarkConfig) -> str:
    return _dm_option(config, "loadworkers", str(config.table_count)) or str(config.table_count)


def _dm_benchmark_user(config: BenchmarkConfig) -> str:
    return config.user.strip()


def _dm_benchmark_schema(config: BenchmarkConfig) -> str:
    schema = _dm_option(config, "schema", _dm_benchmark_user(config)) or _dm_benchmark_user(config)
    return schema.strip().upper()


def _dm_run_minutes(config: BenchmarkConfig) -> int:
    return max(int((config.duration_seconds + 59) / 60), 1)


def _dm_patch_var(script: str, name: str, value: Any) -> str:
    return re.sub(rf'^{re.escape(name)}=.*$', f'{name}="{value}"', script, flags=re.MULTILINE)


def _dm_patch_entry_script(path: Path, config: BenchmarkConfig, *, concurrency: int) -> None:
    script = path.read_text(encoding="utf-8", errors="replace")
    table_type = _dm_table_type(config)
    script = _dm_patch_var(script, "TABLE_TYPE", table_type)
    script = _dm_patch_var(script, "TBS_SIZE", _dm_tbs_size(config))
    script = _dm_patch_var(script, "BMS_VER", _dm_option(config, "bms_ver", "5") or "5")
    script = _dm_patch_var(script, "WAREHOUSES", config.table_size)
    script = _dm_patch_var(script, "LOADWORKERS", _dm_loadworkers(config))
    script = _dm_patch_var(script, "RUNMINS", _dm_run_minutes(config))
    script = _dm_patch_var(script, "TERMINALS", concurrency)
    script = _dm_patch_var(script, "TEST_NODE", _dm_option(config, "test_node", "1") or "1")
    script = _dm_patch_var(script, "ACID_FLAG", _dm_option(config, "acid_flag", "0") or "0")
    script = _dm_patch_var(script, "CKPT_FLAG", _dm_option(config, "ckpt_flag", "0") or "0")
    script = _dm_patch_var(script, "PREHEAT_FALG", "1" if config.warmup_seconds > 0 else "0")
    direct_connection = (
        "#platform supplied dm connection\n"
        f"export DM_CONNECT_USER={shlex.quote(_dm_benchmark_user(config))}\n"
        f"export PASSWORD={shlex.quote(config.password)}\n"
        f"export DB_HOST1={shlex.quote(config.host)}\n"
        f"export DB_PORT1={shlex.quote(str(config.port))}\n\n"
    )
    script = re.sub(
        r"#take dm password.*?export DB_PORT1=.*?\n\n",
        direct_connection,
        script,
        flags=re.DOTALL,
    )
    path.write_text(script, encoding="utf-8")


def _dm_patch_tool_script(path: Path) -> None:
    script = path.read_text(encoding="utf-8", errors="replace")
    script = script.replace('ip=sysdba/\\"${PASSWORD}\\"@${DB_HOST1}:${DB_PORT1}', 'ip=${DM_CONNECT_USER}/\\"${PASSWORD}\\"@${DB_HOST1}:${DB_PORT1}')
    path.write_text(script, encoding="utf-8")


def _dm_write_user_cleanup_sql(path: Path, schema: str) -> None:
    tables = [
        "bmsql_config",
        "bmsql_new_order",
        "bmsql_order_line",
        "bmsql_oorder",
        "bmsql_history",
        "bmsql_customer",
        "bmsql_stock",
        "bmsql_item",
        "bmsql_district",
        "bmsql_warehouse",
    ]
    statements = [f"DROP TABLE {schema}.{table};" for table in tables]
    statements.append(f"DROP SEQUENCE {schema}.bmsql_hist_id_seq;")
    path.write_text("\n".join(statements) + "\n", encoding="utf-8")


def _dm_patch_benchmarksql_files(workdir: Path, config: BenchmarkConfig) -> None:
    schema = _dm_benchmark_schema(config)
    benchmark_user = _dm_benchmark_user(config)
    run_dirs = [
        workdir / "01_tpcc" / "tpcc" / "benchmarksql5" / "run",
        workdir / "01_tpcc" / "tpcc" / "benchmarksql5_fk" / "run",
    ]
    for run_dir in run_dirs:
        for props_name in ("props.dm", "preheat.dm", "props.dm1", "props.dm2", "preheat.dm1", "preheat.dm2"):
            props_path = run_dir / props_name
            if not props_path.exists():
                continue
            text = props_path.read_text(encoding="utf-8", errors="replace")
            text = re.sub(r"^user=.*$", f"user={benchmark_user}", text, flags=re.MULTILINE)
            text = re.sub(r"^password=.*$", f"password={config.password}", text, flags=re.MULTILINE)
            text = re.sub(r"^conn=.*$", f"conn=jdbc:dm://{config.host}:{config.port}", text, flags=re.MULTILINE)
            text = re.sub(r"^osCollectorScript=.*$\n?", "", text, flags=re.MULTILINE)
            text = re.sub(r"^osCollectorInterval=.*$\n?", "", text, flags=re.MULTILINE)
            props_path.write_text(text, encoding="utf-8")

    tpcc_dir = workdir / "01_tpcc" / "tpcc"
    for sql_path in tpcc_dir.glob("*.sql"):
        text = sql_path.read_text(encoding="utf-8", errors="replace")
        text = text.replace("BENCHMARKSQL", schema)
        text = re.sub(r"\s+storage\(on\s+[A-Za-z0-9_]+\)", "", text, flags=re.IGNORECASE)
        sql_path.write_text(text, encoding="utf-8")
    _dm_write_user_cleanup_sql(tpcc_dir / "bms5_create_user.sql", schema)

    for tool_script in [workdir / "01_tpcc" / "00_load.sh", workdir / "01_tpcc" / "01_test.sh", workdir / "01_tpcc" / "02_test_fk.sh"]:
        if tool_script.exists():
            _dm_patch_tool_script(tool_script)


def _prepare_dm_workdir(config: BenchmarkConfig, concurrency: int) -> Path:
    source = _find_dm_benchmarksql_home()
    if config.artifact_dir:
        base_dir = Path(config.artifact_dir)
    else:
        base_dir = Path(tempfile.mkdtemp(prefix="dm_benchmark_"))
    base_dir.mkdir(parents=True, exist_ok=True)
    workdir = base_dir / f"benchmarksqlforDM_{concurrency}"
    if workdir.exists():
        shutil.rmtree(workdir)
    shutil.copytree(source, workdir)
    for script_name in ("tpcc_load.sh", "tpcc_test.sh"):
        _dm_patch_entry_script(workdir / script_name, config, concurrency=concurrency)
    _dm_patch_benchmarksql_files(workdir, config)
    for executable in [workdir / "tpcc_load.sh", workdir / "tpcc_test.sh", *(workdir / "01_tpcc").glob("*.sh")]:
        executable.chmod(executable.stat().st_mode | 0o111)
    return workdir


def _run_dm_tool_script(
    config: BenchmarkConfig,
    workdir: Path,
    script_name: str,
    timeout_seconds: int,
    log_label: str,
    log_callback: Optional[LogCallback],
    cancel_callback: Optional[CancelCallback],
) -> str:
    return _run_command(
        ["bash", script_name],
        timeout_seconds=timeout_seconds,
        log_callback=log_callback,
        log_prefix="[dm] ",
        log_label=log_label,
        secrets=[config.password],
        cancel_callback=cancel_callback,
        cwd=str(workdir),
    )


def _read_dm_log_tail(workdir: Path) -> str:
    log_dir = workdir / "log" / "01_tpcc"
    logs = sorted(log_dir.glob("*.log"), key=lambda item: item.stat().st_mtime if item.exists() else 0, reverse=True)
    chunks: List[str] = []
    for log_file in logs[:3]:
        try:
            chunks.append(log_file.read_text(encoding="utf-8", errors="replace")[-12000:])
        except Exception:
            continue
    run_dir = workdir / "01_tpcc" / "tpcc" / "benchmarksql5" / "run"
    result_dirs = _latest_dm_result_dirs(workdir)
    for result_dir in result_dirs[:3]:
        for relative_path in ("data/tx_summary.csv", "data/result.csv", "data/runInfo.csv"):
            result_file = result_dir / relative_path
            if not result_file.exists():
                continue
            try:
                chunks.append(result_file.read_text(encoding="utf-8", errors="replace")[-4000:])
            except Exception:
                continue
    return "\n".join(chunks)


def _latest_dm_result_dirs(workdir: Path) -> List[Path]:
    run_dir = workdir / "01_tpcc" / "tpcc" / "benchmarksql5" / "run"
    return sorted(
        run_dir.glob("dameng_result_*"),
        key=lambda item: item.stat().st_mtime if item.exists() else 0,
        reverse=True,
    )


def _dm_result_duration_minutes(result_dir: Path, max_elapsed_ms: float, config: BenchmarkConfig) -> float:
    if max_elapsed_ms > 0:
        return max_elapsed_ms / 1000 / 60

    run_info = result_dir / "data" / "runInfo.csv"
    if run_info.exists():
        try:
            with run_info.open("r", encoding="utf-8", errors="replace", newline="") as file:
                rows = list(csv.DictReader(file))
            for row in reversed(rows):
                run_mins = float(row.get("runMins") or 0)
                if run_mins > 0:
                    return run_mins
        except Exception:
            pass

    fallback_seconds = config.duration_seconds or 0
    if fallback_seconds > 0:
        return fallback_seconds / 60
    return 0.0


def _parse_dm_result_csv_metrics(workdir: Path, config: BenchmarkConfig) -> tuple[float, float]:
    for result_dir in _latest_dm_result_dirs(workdir):
        result_file = result_dir / "data" / "result.csv"
        if not result_file.exists():
            continue

        total_success = 0
        new_order_success = 0
        max_elapsed_ms = 0.0
        try:
            with result_file.open("r", encoding="utf-8", errors="replace", newline="") as file:
                for row in csv.DictReader(file):
                    try:
                        error_count = int(float(row.get("error") or 0))
                    except ValueError:
                        error_count = 0
                    if error_count:
                        continue

                    total_success += 1
                    if (row.get("ttype") or "").strip().upper() == "NEW_ORDER":
                        new_order_success += 1

                    try:
                        max_elapsed_ms = max(max_elapsed_ms, float(row.get("elapsed") or 0))
                    except ValueError:
                        pass
        except Exception:
            continue

        duration_minutes = _dm_result_duration_minutes(result_dir, max_elapsed_ms, config)
        if duration_minutes <= 0:
            continue

        tpmc = round(new_order_success / duration_minutes, 2) if new_order_success else 0.0
        tpmtotal = round(total_success / duration_minutes, 2) if total_success else 0.0
        if tpmc > 0 or tpmtotal > 0:
            return tpmc, tpmtotal

    return 0.0, 0.0


def _parse_dm_output(config: BenchmarkConfig, concurrency: int, output: str, workdir: Path) -> ConcurrencyResult:
    parse_source = "\n".join(part for part in [output, _read_dm_log_tail(workdir)] if part)
    tpmc_values = [float(value) for value in re.findall(r"Measured\s+tpmC\s*\(NewOrders\)\s*=\s*([0-9]+(?:\.[0-9]+)?)", parse_source, re.IGNORECASE)]
    tpmtotal_values = [float(value) for value in re.findall(r"Measured\s+tpmTOTAL\s*=\s*([0-9]+(?:\.[0-9]+)?)", parse_source, re.IGNORECASE)]
    if not tpmc_values:
        tpmc_values = [float(value) for value in re.findall(r"^tpmC,\s*([0-9]+(?:\.[0-9]+)?)", parse_source, re.MULTILINE | re.IGNORECASE)]
    if not tpmtotal_values:
        tpmtotal_values = [float(value) for value in re.findall(r"^tpmTotal,\s*([0-9]+(?:\.[0-9]+)?)", parse_source, re.MULTILINE | re.IGNORECASE)]
    if not tpmtotal_values:
        tpmtotal_values = [float(value) for value in re.findall(r"Running\s+Average\s+tpmTOTAL:\s*([0-9]+(?:\.[0-9]+)?)", parse_source, re.IGNORECASE)]
    if not tpmc_values or not tpmtotal_values:
        csv_tpmc, csv_tpmtotal = _parse_dm_result_csv_metrics(workdir, config)
        if not tpmc_values and csv_tpmc > 0:
            tpmc_values = [csv_tpmc]
        if not tpmtotal_values and csv_tpmtotal > 0:
            tpmtotal_values = [csv_tpmtotal]
    qps = round(tpmc_values[-1], 2) if tpmc_values else 0.0
    tps = round(tpmtotal_values[-1], 2) if tpmtotal_values else qps
    if qps <= 0 and tps <= 0:
        raise BenchmarkValidationError(f"未能解析 DM BenchmarkSQL 输出，请检查执行结果。\n{parse_source[-1200:]}")
    return ConcurrencyResult(
        concurrency=concurrency,
        elapsed_seconds=config.duration_seconds + config.warmup_seconds,
        total_transactions=int(tps),
        total_queries=int(qps),
        total_events=int(tps or qps),
        read_queries=0,
        write_queries=0,
        other_queries=0,
        failed_requests=0,
        qps=qps,
        tps=tps,
        avg_latency_ms=0.0,
        p95_latency_ms=0.0,
        min_latency_ms=0.0,
        max_latency_ms=0.0,
        success_rate=100.0,
        baseline_label="推荐基线" if qps > 0 else "待优化",
        sample_errors=_extract_sample_errors(parse_source),
        raw_output=parse_source,
    )


def _dm_disql_env(dm_home: Path) -> Dict[str, str]:
    return {
        **os.environ,
        "LD_LIBRARY_PATH": f"{dm_home / 'bin'}:{os.environ.get('LD_LIBRARY_PATH', '')}",
        "PATH": f"{dm_home / 'bin'}:{os.environ.get('PATH', '')}",
    }


def _dm_disql_connect_script(config: BenchmarkConfig, sql: str, *, user: Optional[str] = None, password: Optional[str] = None) -> str:
    connect_user = user or config.user or "SYSDBA"
    connect_password = config.password if password is None else password
    return f'conn {connect_user}/"{connect_password}"@{config.host}:{config.port}\n{sql.rstrip()}\nexit\n'


def _run_dm_disql(
    config: BenchmarkConfig,
    sql: str,
    *,
    user: Optional[str] = None,
    password: Optional[str] = None,
    timeout_seconds: int = 60,
    allow_disconnect: bool = False,
) -> str:
    dm_home = _find_dm_benchmarksql_home()
    disql = dm_home / "bin" / "disql"
    try:
        result = subprocess.run(
            [str(disql), "/nolog"],
            input=_dm_disql_connect_script(config, sql, user=user, password=password),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout_seconds,
            check=False,
            env=_dm_disql_env(dm_home),
        )
    except subprocess.TimeoutExpired as exc:
        raise BenchmarkValidationError("DM disql 执行超时。") from exc
    output = _masked_command([result.stdout or ""], [config.password, password or ""])
    if allow_disconnect:
        return output
    if result.returncode != 0 or re.search(r"fail|error|错误|失败|exception|无效", result.stdout or "", re.IGNORECASE):
        raise BenchmarkValidationError(f"DM disql 执行失败: {output[-1600:]}")
    return output


def _dm_admin_credentials(config: BenchmarkConfig) -> tuple[str, str]:
    options = _parse_cli_options(config.extra_options)
    admin_user = options.get("dm_admin_user", config.user or "SYSDBA")
    admin_password = options.get("dm_admin_password", config.password)
    return admin_user, admin_password


def _dm_fetch_rlog_paths(config: BenchmarkConfig) -> List[str]:
    admin_user, admin_password = _dm_admin_credentials(config)
    try:
        output = _run_dm_disql(
            config,
            "select client_path from v$rlogfile;",
            user=admin_user,
            password=admin_password,
            timeout_seconds=30,
        )
    except BenchmarkValidationError:
        return []
    paths: List[str] = []
    for path in re.findall(r"(/[^\s|]+\.log)", output, flags=re.IGNORECASE):
        if path not in paths:
            paths.append(path)
    return paths


def _resize_dm_redo_logs(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> int:
    options = _parse_cli_options(config.extra_options)
    if not _option_enabled(options, "dm_resize_redo", True):
        _emit_log(log_callback, "已跳过 DM redo 扩容: --dm-resize-redo false")
        return 0
    target_size = int(options.get("dm_redo_size", options.get("dm_redo_size_mb", "30720")) or 30720)
    configured_paths = str(options.get("dm_redo_logfiles", "")).strip()
    if configured_paths:
        rlog_paths = [item.strip() for item in configured_paths.split(",") if item.strip()]
    else:
        rlog_paths = _dm_fetch_rlog_paths(config)
    if not rlog_paths:
        _emit_log(log_callback, "未能通过 V$RLOGFILE 识别 DM redo 日志路径，已跳过 redo 扩容。")
        return 0

    changed = 0
    admin_user, admin_password = _dm_admin_credentials(config)
    for rlog_path in rlog_paths:
        _check_cancel(cancel_callback)
        safe_path = rlog_path.replace("'", "''")
        statement = f"alter database resize logfile '{safe_path}' to {target_size};"
        try:
            _run_dm_disql(config, statement, user=admin_user, password=admin_password, timeout_seconds=120)
            changed += 1
            _emit_log(log_callback, f"DM redo 日志已调整为 {target_size}: {rlog_path}")
        except BenchmarkValidationError as exc:
            _emit_log(log_callback, f"DM redo 日志调整失败，继续处理后续参数: {rlog_path}，原因: {exc}")
    return changed


def _apply_dm_ini_parameters(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> int:
    options = _parse_cli_options(config.extra_options)
    strict = _option_enabled(options, "dm_tuning_strict", False)
    failures: List[str] = []
    changed = 0
    admin_user, admin_password = _dm_admin_credentials(config)
    for name, value in DM_TUNING_INTEGER_PARAMETERS.items():
        _check_cancel(cancel_callback)
        try:
            _run_dm_disql(
                config,
                f"SP_SET_PARA_VALUE(2, '{name}', {int(value)});",
                user=admin_user,
                password=admin_password,
                timeout_seconds=60,
            )
            changed += 1
        except BenchmarkValidationError as exc:
            failures.append(f"{name}: {exc}")
            _emit_log(log_callback, f"DM 参数设置失败，已继续: {name}={value}")
    for name, value in DM_TUNING_DOUBLE_PARAMETERS.items():
        _check_cancel(cancel_callback)
        try:
            _run_dm_disql(
                config,
                f"SP_SET_PARA_DOUBLE_VALUE(2, '{name}', {float(value)});",
                user=admin_user,
                password=admin_password,
                timeout_seconds=60,
            )
            changed += 1
        except BenchmarkValidationError as exc:
            failures.append(f"{name}: {exc}")
            _emit_log(log_callback, f"DM 浮点参数设置失败，已继续: {name}={value}")
    _emit_log(log_callback, f"DM dm.ini 参数设置完成: 成功 {changed} 项，失败 {len(failures)} 项。")
    if failures and strict:
        raise BenchmarkValidationError("DM 刷参存在失败项，已按 --dm-tuning-strict true 中止: " + "；".join(failures[:8]))
    return changed


def _wait_for_dm_reconnect(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
    timeout_seconds: int = 900,
) -> None:
    deadline = monotonic() + timeout_seconds
    last_error = ""
    _emit_log(log_callback, "开始等待 DM 数据库恢复连接...")
    while monotonic() < deadline:
        _check_cancel(cancel_callback)
        try:
            _run_dm_disql(config, "select 1;", timeout_seconds=20)
            _emit_log(log_callback, "DM 数据库已恢复到可查询状态，继续执行后续压测步骤。")
            return
        except Exception as exc:
            last_error = str(exc)
            sleep(10)
    raise BenchmarkValidationError(f"执行 DM 刷参重启后等待数据库恢复超时。最后一次连接错误: {last_error}")


def _shutdown_dm_immediate_and_wait(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> None:
    options = _parse_cli_options(config.extra_options)
    strict_shutdown = _option_enabled(options, "dm_shutdown_strict", True)
    admin_user, admin_password = _dm_admin_credentials(config)
    _emit_log(log_callback, "DM 刷参已提交，开始执行 shutdown immediate 并等待实例恢复。")
    output = _run_dm_disql(
        config,
        "shutdown immediate;",
        user=admin_user,
        password=admin_password,
        timeout_seconds=120,
        allow_disconnect=True,
    )
    if re.search(r"fail|error|错误|失败|exception|无效", output, re.IGNORECASE):
        message = output[-1600:]
        if strict_shutdown:
            raise BenchmarkValidationError(f"DM shutdown immediate 执行失败: {message}")
        _emit_log(log_callback, f"DM shutdown immediate 执行失败，按 --dm-shutdown-strict false 继续后续压测: {message}")
        return
    _wait_for_dm_reconnect(config, log_callback=log_callback, cancel_callback=cancel_callback)


def _apply_dm_tuning_if_requested(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> None:
    options = _parse_cli_options(config.extra_options)
    if not _option_enabled(options, "dm_apply_tuning", False):
        return
    _emit_log(log_callback, "开始执行 DM 文档刷参流程: redo 扩容、写入 dm.ini 参数、shutdown immediate、等待恢复。")
    redo_changes = _resize_dm_redo_logs(config, log_callback=log_callback, cancel_callback=cancel_callback)
    parameter_changes = _apply_dm_ini_parameters(config, log_callback=log_callback, cancel_callback=cancel_callback)
    if redo_changes or parameter_changes:
        _shutdown_dm_immediate_and_wait(config, log_callback=log_callback, cancel_callback=cancel_callback)
    else:
        _emit_log(log_callback, "DM 刷参未产生可确认变更，跳过 shutdown immediate。")


def _check_dm_connectivity(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    masked_password = "******" if config.password else ""
    _emit_log(
        log_callback,
        f"连接检测启动: DM disql -> host={config.host} port={config.port} user={config.user} password={masked_password}",
    )
    try:
        _run_dm_disql(config, "select 1;", timeout_seconds=20)
    except BenchmarkValidationError as exc:
        raise BenchmarkValidationError(f"DM 连接测试失败: {exc}") from exc
    _emit_log(log_callback, "DM 连接检测通过。")


def _run_dm_benchmark(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> BenchmarkSummary:
    table_type = _dm_table_type(config)
    if table_type.endswith(("2", "3")) and config.table_size not in _dm_allowed_partition_warehouses():
        raise BenchmarkValidationError("DM 分区表模式仅支持 100、200、500、1000、2000 仓；如需其它仓数，请在其他参数设置中使用 --table-type 10。")
    _emit_log(log_callback, f"已接收测试任务: DM / {config.host}:{config.port}")
    _emit_log(
        log_callback,
        f"压测工具: benchmarksqlforDM | 并发配置: {','.join(str(value) for value in sorted(set(config.threads_values)))} | 时长: {config.duration_seconds}s | 仓库数: {config.table_size}",
    )
    _check_cancel(cancel_callback)
    _check_dm_connectivity(config, log_callback=log_callback)
    _apply_dm_tuning_if_requested(config, log_callback=log_callback, cancel_callback=cancel_callback)

    results: List[ConcurrencyResult] = []
    for concurrency in sorted(set(config.threads_values)):
        _check_cancel(cancel_callback)
        workdir = _prepare_dm_workdir(config, concurrency)
        _emit_log(log_callback, f"DM BenchmarkSQL 工作目录: {workdir}")
        if config.auto_prepare:
            _emit_log(log_callback, "开始执行 DM BenchmarkSQL TPC-C 造数...")
            _run_dm_tool_script(
                config,
                workdir,
                "tpcc_load.sh",
                timeout_seconds=max(7200, int(config.table_size) * 120),
                log_label="dm-benchmarksql-load",
                log_callback=log_callback,
                cancel_callback=cancel_callback,
            )
        _emit_log(log_callback, f"开始执行 DM BenchmarkSQL TPC-C 压测: terminals={concurrency}")
        output = _run_dm_tool_script(
            config,
            workdir,
            "tpcc_test.sh",
            timeout_seconds=config.duration_seconds + config.warmup_seconds + 900,
            log_label=f"{concurrency}并发DM BenchmarkSQL测试",
            log_callback=log_callback,
            cancel_callback=cancel_callback,
        )
        result = _parse_dm_output(config, concurrency, output, workdir)
        results.append(result)
        _emit_log(log_callback, f"并发 {concurrency}: DM BenchmarkSQL 完成，tpmTOTAL={result.tps}，tpmC={result.qps}")

    return BenchmarkSummary(
        report_title=config.report_title,
        executed_at=_beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
        target={
            "db_type": "DM",
            "host": config.host,
            "port": config.port,
            "database": config.database,
            "user": config.user,
            "collection_name": "",
        },
        parameters={
            "benchmark_engine": "benchmarksqlforDM",
            "workload": "TPC-C",
            "duration_seconds": config.duration_seconds,
            "warmup_seconds": config.warmup_seconds,
            "report_interval": 0,
            "table_count": config.table_count,
            "table_size": config.table_size,
            "operation_count": 0,
            "auth_database": "",
            "collection_name": "",
            "ssl_enabled": False,
            "ssl_verify": False,
            "auto_prepare": config.auto_prepare,
            "auto_cleanup": False,
            "dm_apply_tuning": _option_enabled(_parse_cli_options(config.extra_options), "dm_apply_tuning", False),
            "threads_values": sorted(set(config.threads_values)),
        },
        results=results,
        recommended_baseline=_select_recommended_baseline(results),
    )


def _find_oceanbase_benchmarksql_home() -> Path:
    configured_raw = os.environ.get("OCEANBASE_BENCHMARKSQL_HOME", "").strip()
    candidates: List[Path] = []
    if configured_raw:
        candidates.append(Path(configured_raw))
    candidates.extend(
        [
            Path("/app/vendor/benchmarksql-oceanbase"),
            BASE_DIR.parent / "vendor" / "benchmarksql-oceanbase",
            BASE_DIR.parent / "vendor" / "benchmarksql",
        ]
    )
    for candidate in candidates:
        if (
            (candidate / "run" / "runDatabaseBuild.sh").exists()
            and (candidate / "run" / "runDatabaseDestroy.sh").exists()
            and (candidate / "run" / "runBenchmark.sh").exists()
            and (candidate / "dist" / "BenchmarkSQL-5.0.jar").exists()
        ):
            return candidate
    raise BenchmarkValidationError(
        "未找到 OceanBase BenchmarkSQL 工具包。请使用已更新的 QDBmark 镜像，或设置 OCEANBASE_BENCHMARKSQL_HOME 指向 OceanBase 适配版 BenchmarkSQL。"
    )


def _oceanbase_option(config: BenchmarkConfig, name: str, default: Any) -> str:
    options = _parse_cli_options(config.extra_options)
    return str(options.get(name.replace("-", "_"), options.get(name, default))).strip()


def _oceanbase_run_minutes(config: BenchmarkConfig) -> int:
    return max(int((config.duration_seconds + 59) / 60), 1)


def _oceanbase_conn_string(config: BenchmarkConfig) -> str:
    configured_conn = _oceanbase_option(config, "conn", "")
    if configured_conn:
        return configured_conn
    conn_params = _oceanbase_option(
        config,
        "conn-params",
        "rewriteBatchedStatements=true&allowMultiQueries=true&useLocalSessionState=true&useUnicode=true&characterEncoding=utf-8&socketTimeout=3000000",
    )
    suffix = f"?{conn_params}" if conn_params else ""
    return f"jdbc:oceanbase://{config.host}:{config.port}/{config.database}{suffix}"


def _oceanbase_option_enabled(config: BenchmarkConfig, name: str, default: bool = False) -> bool:
    return _option_enabled(_parse_cli_options(config.extra_options), name.replace("-", "_"), default)


def _oceanbase_int_option(config: BenchmarkConfig, name: str, default: int) -> int:
    raw = _oceanbase_option(config, name, str(default))
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _write_oceanbase_props(path: Path, config: BenchmarkConfig, *, concurrency: int) -> None:
    driver = _oceanbase_option(config, "driver", "com.oceanbase.jdbc.Driver")
    load_workers = _oceanbase_option(config, "loadworkers", str(config.table_count)) or str(config.table_count)
    db_prop = _oceanbase_option(config, "db-prop", "oceanbase") or "oceanbase"
    content = f"""db={db_prop}
driver={driver}
conn={_oceanbase_conn_string(config)}
user={config.user}
password={config.password}

warehouses={config.table_size}
loadWorkers={load_workers}

terminals={concurrency}
//To run specified transactions per terminal- runMins must equal zero
runTxnsPerTerminal=0
//To run for specified minutes- runTxnsPerTerminal must equal zero
runMins={_oceanbase_run_minutes(config)}
//Number of total transactions per minute
limitTxnsPerMin=0

//Set to true to run in 4.x compatible mode. Set to false to use the
//entire configured database evenly.
terminalWarehouseFixed=true

//The following five values must add up to 100
newOrderWeight=45
paymentWeight=43
orderStatusWeight=4
deliveryWeight=4
stockLevelWeight=4

// Directory name to create for collecting detailed result data.
resultDirectory=qdbmark_ob_result_%tY-%tm-%td_%tH%tM%tS
// QDBmark already collects container metrics through Prometheus.
// Keep BenchmarkSQL osCollectorScript unset because the bundled helper is Python 2.
//osCollectorScript=./misc/os_collector_linux.py
//osCollectorInterval=1
//osCollectorSSHAddr=user@dbhost
//osCollectorDevices=net_eth0 blk_sda
"""
    path.write_text(content, encoding="utf-8")


def _prepare_oceanbase_workdir(config: BenchmarkConfig, concurrency: int) -> Path:
    source = _find_oceanbase_benchmarksql_home()
    if config.artifact_dir:
        base_dir = Path(config.artifact_dir)
    else:
        base_dir = Path(tempfile.mkdtemp(prefix="oceanbase_benchmark_"))
    base_dir.mkdir(parents=True, exist_ok=True)
    workdir = base_dir / f"benchmarksql_oceanbase_{concurrency}"
    if workdir.exists():
        shutil.rmtree(workdir)
    shutil.copytree(source, workdir)
    run_dir = workdir / "run"
    for script_name in ("runDatabaseDestroy.sh", "runDatabaseBuild.sh", "runBenchmark.sh", "runLoader.sh", "runSQL.sh"):
        script_path = run_dir / script_name
        if script_path.exists():
            script_path.chmod(script_path.stat().st_mode | 0o111)
    _write_oceanbase_props(run_dir / "props.qdbmark_ob", config, concurrency=concurrency)
    return workdir


def _run_oceanbase_tool_script(
    config: BenchmarkConfig,
    workdir: Path,
    script_name: str,
    timeout_seconds: int,
    log_label: str,
    log_callback: Optional[LogCallback],
    cancel_callback: Optional[CancelCallback],
    allow_failure: bool = False,
) -> str:
    return _run_command(
        ["bash", script_name, "props.qdbmark_ob"],
        timeout_seconds=timeout_seconds,
        allow_failure=allow_failure,
        log_callback=log_callback,
        log_prefix="[oceanbase] ",
        log_label=log_label,
        secrets=[config.password],
        cancel_callback=cancel_callback,
        cwd=str(workdir / "run"),
    )


def _oceanbase_build_failure_message(output: str) -> str:
    lowered = output.lower()
    if "no memory or reach tenant memory limit" in lowered:
        return "OceanBase 造数失败: 租户内存不足或已达到租户内存上限。请使用业务租户，并降低仓库数/loadWorkers，或给该租户扩容后重试。"
    if "db load configuration parameter 'warehouses' not found" in lowered:
        return "OceanBase 造数未完整成功: bmsql_config 中没有 warehouses 记录。通常是造数阶段提前失败导致，请先查看造数日志。"
    critical_patterns = (
        "cannot load jdbc driver",
        "access denied",
        "communications link failure",
        "could not connect",
    )
    for pattern in critical_patterns:
        if pattern in lowered:
            tail = output.strip()[-1000:]
            return f"OceanBase BenchmarkSQL 执行失败: {tail}"
    return ""


def _read_oceanbase_result_tail(workdir: Path) -> str:
    run_dir = workdir / "run"
    chunks: List[str] = []
    error_log = run_dir / "benchmarksql-error.log"
    if error_log.exists():
        try:
            chunks.append(error_log.read_text(encoding="utf-8", errors="replace")[-8000:])
        except Exception:
            pass
    for result_dir in sorted(run_dir.glob("qdbmark_ob_result_*"), key=lambda item: item.stat().st_mtime if item.exists() else 0, reverse=True)[:3]:
        for result_file in sorted(result_dir.rglob("*"), key=lambda item: item.stat().st_mtime if item.exists() else 0, reverse=True)[:8]:
            if not result_file.is_file():
                continue
            try:
                chunks.append(result_file.read_text(encoding="utf-8", errors="replace")[-8000:])
            except Exception:
                continue
    return "\n".join(chunks)


def _parse_oceanbase_output(config: BenchmarkConfig, concurrency: int, output: str, workdir: Path) -> ConcurrencyResult:
    parse_source = "\n".join(part for part in [output, _read_oceanbase_result_tail(workdir)] if part)
    tpmc_values = [float(value) for value in re.findall(r"Measured\s+tpmC\s*\(NewOrders\)\s*=\s*([0-9]+(?:\.[0-9]+)?)", parse_source, re.IGNORECASE)]
    tpmtotal_values = [float(value) for value in re.findall(r"Measured\s+tpmTOTAL\s*=\s*([0-9]+(?:\.[0-9]+)?)", parse_source, re.IGNORECASE)]
    qps = round(tpmc_values[-1], 2) if tpmc_values else 0.0
    tps = round(tpmtotal_values[-1], 2) if tpmtotal_values else qps
    if qps <= 0 and tps <= 0:
        raise BenchmarkValidationError(f"未能解析 OceanBase BenchmarkSQL 输出，请检查执行结果。\n{parse_source[-1200:]}")
    return ConcurrencyResult(
        concurrency=concurrency,
        elapsed_seconds=config.duration_seconds + config.warmup_seconds,
        total_transactions=int(tps),
        total_queries=int(qps),
        total_events=int(tps or qps),
        read_queries=0,
        write_queries=0,
        other_queries=0,
        failed_requests=0,
        qps=qps,
        tps=tps,
        avg_latency_ms=0.0,
        p95_latency_ms=0.0,
        min_latency_ms=0.0,
        max_latency_ms=0.0,
        success_rate=100.0,
        baseline_label="推荐基线" if qps > 0 else "待优化",
        sample_errors=_extract_sample_errors(parse_source),
        raw_output=parse_source,
    )


def _check_oceanbase_connectivity(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    import pymysql

    masked_password = "******" if config.password else ""
    _emit_log(
        log_callback,
        f"连接检测启动: OceanBase MySQL 协议 -> host={config.host} port={config.port} user={config.user} database={config.database} password={masked_password}",
    )
    try:
        connection = pymysql.connect(**_mysql_connect_kwargs(config))
        with connection.cursor() as cursor:
            cursor.execute("select version() as version")
            row = cursor.fetchone() or {}
        connection.close()
        version = str(row.get("version", "")).splitlines()[0] if isinstance(row, dict) else ""
        _emit_log(log_callback, f"OceanBase 连接检测通过: {version or 'select version() ok'}")
    except Exception as exc:
        raise BenchmarkValidationError(f"OceanBase 连接测试失败: {exc}") from exc


def _oceanbase_connection(config: BenchmarkConfig, *, read_timeout: int = 60, write_timeout: int = 60):
    kwargs = _mysql_connect_kwargs(config)
    kwargs["read_timeout"] = read_timeout
    kwargs["write_timeout"] = write_timeout
    return __import__("pymysql").connect(**kwargs)


def _oceanbase_safe_identifier(value: str) -> bool:
    return bool(re.match(r"^[A-Za-z0-9_.$-]+$", value or ""))


def _oceanbase_tenant_name(config: BenchmarkConfig) -> str:
    if str(config.oceanbase_tenant_name or "").strip():
        return str(config.oceanbase_tenant_name).strip()
    configured = _oceanbase_option(config, "tenant-name", "")
    if configured:
        return configured
    user = str(config.user or "").strip()
    if "@" not in user:
        return ""
    tenant = user.split("@", 1)[1]
    tenant = tenant.split("#", 1)[0]
    tenant = tenant.split(":", 1)[0]
    return tenant.strip()


def _apply_oceanbase_document_tuning(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    if not _oceanbase_option_enabled(config, "ob-apply-tuning", False):
        return
    statements = [
        ("ODP proxy_mem_limited", "ALTER PROXYCONFIG SET proxy_mem_limited='4G'"),
        ("ODP enable_compression_protocol", "ALTER PROXYCONFIG SET enable_compression_protocol=false"),
        ("observer enable_sql_audit", "ALTER SYSTEM SET enable_sql_audit=false"),
        ("observer enable_perf_event", "ALTER SYSTEM SET enable_perf_event=false"),
        ("observer syslog_level", "ALTER SYSTEM SET syslog_level='PERF'"),
        ("observer enable_record_trace_log", "ALTER SYSTEM SET enable_record_trace_log=false"),
    ]
    _emit_log(log_callback, "开始按 ODM 手册执行 OceanBase ODP / observer 参数优化。")
    try:
        connection = _oceanbase_connection(config, read_timeout=120, write_timeout=120)
    except Exception as exc:
        _emit_log(log_callback, f"OceanBase 参数优化连接失败，已继续后续测试: {exc}")
        return
    try:
        with connection.cursor() as cursor:
            for label, statement in statements:
                try:
                    cursor.execute(statement)
                    _emit_log(log_callback, f"OceanBase 参数优化完成: {label}")
                    if statement == "ALTER SYSTEM SET enable_sql_audit=false":
                        sleep(5)
                except Exception as exc:
                    _emit_log(log_callback, f"OceanBase 参数优化失败，已继续: {label}: {exc}")
        connection.commit()
    finally:
        connection.close()


def _wait_oceanbase_major_freeze(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    timeout_seconds = max(60, _oceanbase_int_option(config, "ob-major-freeze-timeout", 7200))
    poll_seconds = max(5, _oceanbase_int_option(config, "ob-major-freeze-poll", 30))
    deadline = monotonic() + timeout_seconds
    query = "SELECT COUNT(*) AS unfinished_count FROM __all_virtual_server_compaction_progress WHERE status != 'FINISH'"
    _emit_log(log_callback, "开始检查 OceanBase major freeze 合并进度，返回为空或未完成数为 0 后继续。")
    while True:
        try:
            connection = _oceanbase_connection(config, read_timeout=60, write_timeout=60)
            with connection.cursor() as cursor:
                cursor.execute(query)
                row = cursor.fetchone() or {}
            connection.close()
            unfinished_count = int(row.get("unfinished_count", 0) or 0) if isinstance(row, dict) else 0
            if unfinished_count <= 0:
                _emit_log(log_callback, "OceanBase major freeze 合并检查完成。")
                return
            _emit_log(log_callback, f"OceanBase major freeze 仍有 {unfinished_count} 个未完成项，继续等待。")
        except Exception as exc:
            _emit_log(log_callback, f"OceanBase major freeze 进度检查失败，已继续后续测试: {exc}")
            return
        if monotonic() >= deadline:
            _emit_log(log_callback, f"OceanBase major freeze 等待超过 {timeout_seconds}s，已继续后续测试。")
            return
        sleep(poll_seconds)


def _try_oceanbase_major_freeze(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    if not _oceanbase_option_enabled(config, "ob-wait-major-freeze", False):
        _emit_log(log_callback, "跳过 OceanBase major freeze: 页面未开启合并等待。")
        return
    tenant_name = _oceanbase_tenant_name(config)
    if not tenant_name:
        _emit_log(log_callback, "跳过 OceanBase major freeze: 未填写租户名，且无法从用户名中推断租户名。")
        return
    if not _oceanbase_safe_identifier(tenant_name):
        _emit_log(log_callback, f"跳过 OceanBase major freeze: 租户名包含不支持的字符 tenant={tenant_name}")
        return
    try:
        connection = _oceanbase_connection(config, read_timeout=120, write_timeout=120)
        with connection.cursor() as cursor:
            cursor.execute(f"ALTER SYSTEM major freeze tenant={tenant_name}")
        connection.close()
        _emit_log(log_callback, f"OceanBase major freeze 已提交: tenant={tenant_name}")
        _wait_oceanbase_major_freeze(config, log_callback=log_callback)
    except Exception as exc:
        _emit_log(log_callback, f"OceanBase major freeze 执行失败，已继续后续测试: {exc}")


def _try_oceanbase_gather_stats(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    if not _oceanbase_option_enabled(config, "ob-gather-stats", False):
        return
    schema_name = _oceanbase_option(config, "stats-schema", config.database) or config.database
    degree = max(1, _oceanbase_int_option(config, "stats-degree", 96))
    if not _oceanbase_safe_identifier(schema_name):
        _emit_log(log_callback, f"跳过 OceanBase 统计信息收集: schema 名称包含不支持的字符 schema={schema_name}")
        return
    try:
        connection = _oceanbase_connection(config, read_timeout=1800, write_timeout=1800)
        with connection.cursor() as cursor:
            cursor.execute(f"call dbms_stats.gather_schema_stats('{schema_name}',degree=>{degree})")
        connection.close()
        _emit_log(log_callback, f"OceanBase 统计信息收集完成: schema={schema_name}, degree={degree}")
    except Exception as exc:
        _emit_log(log_callback, f"OceanBase 统计信息收集失败，已继续后续测试: {exc}")


def _run_oceanbase_benchmark(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> BenchmarkSummary:
    _emit_log(log_callback, f"已接收测试任务: OceanBase / {config.host}:{config.port}/{config.database}")
    _emit_log(
        log_callback,
        f"压测工具: OceanBase BenchmarkSQL | 并发配置: {','.join(str(value) for value in sorted(set(config.threads_values)))} | 时长: {config.duration_seconds}s | 仓库数: {config.table_size}",
    )
    _check_cancel(cancel_callback)
    _check_oceanbase_connectivity(config, log_callback=log_callback)
    _apply_oceanbase_document_tuning(config, log_callback=log_callback)

    results: List[ConcurrencyResult] = []
    dataset_prepared = False
    post_build_steps_done = False
    for concurrency in sorted(set(config.threads_values)):
        _check_cancel(cancel_callback)
        workdir = _prepare_oceanbase_workdir(config, concurrency)
        _emit_log(log_callback, f"OceanBase BenchmarkSQL 工作目录: {workdir}")
        if config.auto_prepare and not dataset_prepared:
            _emit_log(log_callback, "开始执行 OceanBase BenchmarkSQL TPC-C 环境清理...")
            _run_oceanbase_tool_script(
                config,
                workdir,
                "runDatabaseDestroy.sh",
                timeout_seconds=max(900, int(config.table_size) * 10),
                log_label="oceanbase-benchmarksql-destroy",
                log_callback=log_callback,
                cancel_callback=cancel_callback,
                allow_failure=True,
            )
            _emit_log(log_callback, "开始执行 OceanBase BenchmarkSQL TPC-C 造数...")
            build_output = _run_oceanbase_tool_script(
                config,
                workdir,
                "runDatabaseBuild.sh",
                timeout_seconds=max(7200, int(config.table_size) * 120),
                log_label="oceanbase-benchmarksql-build",
                log_callback=log_callback,
                cancel_callback=cancel_callback,
            )
            build_failure = _oceanbase_build_failure_message(build_output)
            if build_failure:
                raise BenchmarkValidationError(build_failure)
            dataset_prepared = True
        elif config.auto_prepare:
            _emit_log(log_callback, "复用已构造的 OceanBase TPC-C 数据，按新的 terminals 继续压测。")
        if not post_build_steps_done:
            _try_oceanbase_major_freeze(config, log_callback=log_callback)
            _try_oceanbase_gather_stats(config, log_callback=log_callback)
            post_build_steps_done = True
        _emit_log(log_callback, f"开始执行 OceanBase BenchmarkSQL TPC-C 压测: terminals={concurrency}")
        output = _run_oceanbase_tool_script(
            config,
            workdir,
            "runBenchmark.sh",
            timeout_seconds=config.duration_seconds + config.warmup_seconds + 900,
            log_label=f"{concurrency}并发OceanBase BenchmarkSQL测试",
            log_callback=log_callback,
            cancel_callback=cancel_callback,
        )
        benchmark_failure = _oceanbase_build_failure_message(output)
        if benchmark_failure:
            raise BenchmarkValidationError(benchmark_failure)
        result = _parse_oceanbase_output(config, concurrency, output, workdir)
        results.append(result)
        _emit_log(log_callback, f"并发 {concurrency}: OceanBase BenchmarkSQL 完成，tpmTOTAL={result.tps}，tpmC={result.qps}")

    return BenchmarkSummary(
        report_title=config.report_title,
        executed_at=_beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
        target={
            "db_type": "OceanBase",
            "host": config.host,
            "port": config.port,
            "database": config.database,
            "user": config.user,
            "tenant_name": config.oceanbase_tenant_name,
            "collection_name": "",
        },
        parameters={
            "benchmark_engine": "OceanBase BenchmarkSQL",
            "workload": "TPC-C",
            "duration_seconds": config.duration_seconds,
            "warmup_seconds": config.warmup_seconds,
            "report_interval": 0,
            "table_count": config.table_count,
            "table_size": config.table_size,
            "operation_count": 0,
            "auth_database": "",
            "collection_name": "",
            "ssl_enabled": False,
            "ssl_verify": False,
            "auto_prepare": config.auto_prepare,
            "auto_cleanup": False,
            "threads_values": sorted(set(config.threads_values)),
        },
        results=results,
        recommended_baseline=_select_recommended_baseline(results),
    )


def _find_gaussdb_benchmarksql_home() -> Path:
    configured_raw = os.environ.get("GAUSSDB_BENCHMARKSQL_HOME", "").strip()
    candidates: List[Path] = []
    if configured_raw:
        candidates.append(Path(configured_raw))
    candidates.extend(
        [
            Path("/app/vendor/benchmarksql-gaussdb"),
            BASE_DIR.parent / "vendor" / "benchmarksql-gaussdb",
        ]
    )
    for candidate in candidates:
        if (
            (candidate / "run" / "runDatabaseBuild.sh").exists()
            and (candidate / "run" / "runDatabaseDestroy.sh").exists()
            and (candidate / "run" / "runBenchmark.sh").exists()
            and (candidate / "dist" / "BenchmarkSQL-5.0.jar").exists()
        ):
            _ensure_gaussdb_jdbc_driver(candidate)
            return candidate
    raise BenchmarkValidationError(
        "未找到 GaussDB BenchmarkSQL 工具包。请将 benchmarksql-5.0 放到 /app/vendor/benchmarksql-gaussdb，"
        "或设置 GAUSSDB_BENCHMARKSQL_HOME 指向该目录。"
    )


def _ensure_gaussdb_jdbc_driver(benchmarksql_home: Path) -> None:
    driver_dirs = [benchmarksql_home / "lib" / "postgres", benchmarksql_home / "lib"]
    for driver_dir in driver_dirs:
        if not driver_dir.exists():
            continue
        for jar in driver_dir.glob("*.jar"):
            jar_name = jar.name.lower()
            if "postgres" in jar_name or "postgresql" in jar_name:
                return
    raise BenchmarkValidationError("未找到 GaussDB 可用的 PostgreSQL JDBC 驱动，请将 postgresql JDBC jar 放到 BenchmarkSQL 的 lib/postgres 目录。")


def _gaussdb_option(config: BenchmarkConfig, name: str, default: Any) -> str:
    options = _parse_cli_options(config.extra_options)
    return str(options.get(name.replace("-", "_"), options.get(name, default))).strip()


def _gaussdb_run_minutes(config: BenchmarkConfig) -> int:
    return max(int((config.duration_seconds + 59) / 60), 1)


def _split_gaussdb_host_entry(raw: str, default_port: int) -> tuple[str, int]:
    value = str(raw or "").strip()
    if not value:
        raise BenchmarkValidationError("GaussDB CN 节点不能为空。")
    if ":" in value:
        host, port_raw = value.rsplit(":", 1)
        host = host.strip()
        try:
            port = int(port_raw.strip())
        except ValueError as exc:
            raise BenchmarkValidationError(f"GaussDB CN 节点端口无效: {value}") from exc
    else:
        host = value
        port = int(default_port)
    if not host:
        raise BenchmarkValidationError(f"GaussDB CN 节点地址无效: {value}")
    _validate_ipv4_address(host, "GaussDB CN 节点地址")
    if port <= 0 or port > 65535:
        raise BenchmarkValidationError(f"GaussDB CN 节点端口必须在 1-65535 之间: {value}")
    return host, port


def _gaussdb_host_entries(config: BenchmarkConfig) -> List[tuple[str, int]]:
    architecture = str(config.gaussdb_architecture or "centralized").strip().lower()
    raw_hosts = str(config.gaussdb_cn_hosts or "").strip() if architecture == "distributed" else ""
    if raw_hosts:
        entries = [item.strip() for item in re.split(r"[,;\n]+", raw_hosts) if item.strip()]
    else:
        entries = [f"{config.host}:{config.port}"]
    if not entries:
        raise BenchmarkValidationError("GaussDB 节点列表不能为空。")
    return [_split_gaussdb_host_entry(item, int(config.port)) for item in entries]


def _gaussdb_target_hosts(config: BenchmarkConfig) -> str:
    return ",".join(f"{host}:{port}" for host, port in _gaussdb_host_entries(config))


def _gaussdb_jdbc_hosts(config: BenchmarkConfig) -> str:
    return _gaussdb_target_hosts(config)


def _gaussdb_conn_params(config: BenchmarkConfig) -> str:
    configured = str(config.gaussdb_conn_params or "").strip()
    configured = _gaussdb_option(config, "conn-params", configured)
    return configured or "prepareThreshold=1&batchMode=on&fetchsize=10"


def _gaussdb_conn_string(config: BenchmarkConfig) -> str:
    configured_conn = _gaussdb_option(config, "conn", "")
    if configured_conn:
        return configured_conn
    conn_params = _gaussdb_conn_params(config)
    suffix = f"?{conn_params}" if conn_params else ""
    return f"jdbc:postgresql://{_gaussdb_jdbc_hosts(config)}/{config.database}{suffix}"


def _gaussdb_props_name(config: BenchmarkConfig) -> str:
    architecture = str(config.gaussdb_architecture or "centralized").strip().lower()
    return "props.gaussdb.fbs" if architecture == "distributed" else "props.gaussdb.jzs"


def _gaussdb_table_variant_name(config: BenchmarkConfig) -> str:
    architecture = str(config.gaussdb_architecture or "centralized").strip().lower()
    return "tableCreates.fbs.sql" if architecture == "distributed" else "tableCreates.jzs.sql"


def _write_gaussdb_props(path: Path, config: BenchmarkConfig, *, concurrency: int) -> None:
    driver = _gaussdb_option(config, "driver", "org.postgresql.Driver")
    load_workers = _gaussdb_option(config, "loadworkers", str(config.table_count)) or str(config.table_count)
    lines = [
        "db=postgres",
        f"driver={driver}",
        f"conn={_gaussdb_conn_string(config)}",
        f"user={config.user}",
        f"password={config.password}",
        "",
        f"warehouses={config.table_size}",
        f"loadWorkers={load_workers}",
        "",
        f"terminals={concurrency}",
        "runTxnsPerTerminal=0",
        f"runMins={_gaussdb_run_minutes(config)}",
        "limitTxnsPerMin=0",
        "terminalWarehouseFixed=false",
        "",
        "newOrderWeight=45",
        "paymentWeight=43",
        "orderStatusWeight=4",
        "deliveryWeight=4",
        "stockLevelWeight=4",
        "",
        "resultDirectory=qdbmark_gaussdb_result_%tY-%tm-%td_%tH%tM%tS",
        "# QDBmark disables BenchmarkSQL OSCollector because the bundled helper is Python 2 style.",
        "#osCollectorScript=./misc/os_collector_linux.py",
        "#osCollectorInterval=1",
        "#osCollectorSSHAddr=omm@127.0.0.1",
        "#osCollectorDevices=net_eth0 blk_sda",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def _prepare_gaussdb_workdir(config: BenchmarkConfig, concurrency: int) -> Path:
    source = _find_gaussdb_benchmarksql_home()
    if config.artifact_dir:
        base_dir = Path(config.artifact_dir)
    else:
        base_dir = Path(tempfile.mkdtemp(prefix="gaussdb_benchmark_"))
    base_dir.mkdir(parents=True, exist_ok=True)
    workdir = base_dir / f"benchmarksql_gaussdb_{str(config.gaussdb_architecture or 'centralized').strip().lower()}_{concurrency}"
    if workdir.exists():
        shutil.rmtree(workdir)
    shutil.copytree(source, workdir)
    run_dir = workdir / "run"
    for script_name in ("runDatabaseDestroy.sh", "runDatabaseBuild.sh", "runBenchmark.sh", "runLoader.sh", "runSQL.sh"):
        script_path = run_dir / script_name
        if script_path.exists():
            script_path.chmod(script_path.stat().st_mode | 0o111)
    sql_common = run_dir / "sql.common"
    variant_path = sql_common / _gaussdb_table_variant_name(config)
    default_table_creates = sql_common / "tableCreates.sql"
    if sql_common.exists():
        if not variant_path.exists() and default_table_creates.exists():
            shutil.copy2(default_table_creates, variant_path)
        if variant_path.exists():
            shutil.copy2(variant_path, default_table_creates)
    _write_gaussdb_props(run_dir / _gaussdb_props_name(config), config, concurrency=concurrency)
    return workdir


def _run_gaussdb_tool_script(
    config: BenchmarkConfig,
    workdir: Path,
    script_name: str,
    timeout_seconds: int,
    log_label: str,
    log_callback: Optional[LogCallback],
    cancel_callback: Optional[CancelCallback],
    allow_failure: bool = False,
) -> str:
    return _run_command(
        ["bash", script_name, _gaussdb_props_name(config)],
        timeout_seconds=timeout_seconds,
        allow_failure=allow_failure,
        log_callback=log_callback,
        log_prefix="[gaussdb] ",
        log_label=log_label,
        secrets=[config.password],
        cancel_callback=cancel_callback,
        cwd=str(workdir / "run"),
    )


def _read_gaussdb_result_tail(workdir: Path) -> str:
    run_dir = workdir / "run"
    chunks: List[str] = []
    for result_dir in sorted(run_dir.glob("qdbmark_gaussdb_result_*"), key=lambda item: item.stat().st_mtime if item.exists() else 0, reverse=True)[:3]:
        for result_file in sorted(result_dir.rglob("*"), key=lambda item: item.stat().st_mtime if item.exists() else 0, reverse=True)[:8]:
            if not result_file.is_file():
                continue
            try:
                chunks.append(result_file.read_text(encoding="utf-8", errors="replace")[-8000:])
            except Exception:
                continue
    return "\n".join(chunks)


def _parse_gaussdb_output(config: BenchmarkConfig, concurrency: int, output: str, workdir: Path) -> ConcurrencyResult:
    parse_source = "\n".join(part for part in [output, _read_gaussdb_result_tail(workdir)] if part)
    tpmc_values = [float(value) for value in re.findall(r"Measured\s+tpmC\s*\(NewOrders\)\s*=\s*([0-9]+(?:\.[0-9]+)?)", parse_source, re.IGNORECASE)]
    tpmtotal_values = [float(value) for value in re.findall(r"Measured\s+tpmTOTAL\s*=\s*([0-9]+(?:\.[0-9]+)?)", parse_source, re.IGNORECASE)]
    tx_counts = [int(value) for value in re.findall(r"Transaction\s+Count\s*=\s*([0-9]+)", parse_source, re.IGNORECASE)]
    qps = round(tpmc_values[-1], 2) if tpmc_values else 0.0
    tps = round(tpmtotal_values[-1], 2) if tpmtotal_values else qps
    if qps <= 0 and tps <= 0:
        raise BenchmarkValidationError(f"未能解析 GaussDB BenchmarkSQL 输出，请检查执行结果。\n{parse_source[-1200:]}")
    return ConcurrencyResult(
        concurrency=concurrency,
        elapsed_seconds=config.duration_seconds + config.warmup_seconds,
        total_transactions=tx_counts[-1] if tx_counts else int(tps),
        total_queries=int(qps),
        total_events=tx_counts[-1] if tx_counts else int(tps or qps),
        read_queries=0,
        write_queries=0,
        other_queries=0,
        failed_requests=0,
        qps=qps,
        tps=tps,
        avg_latency_ms=0.0,
        p95_latency_ms=0.0,
        min_latency_ms=0.0,
        max_latency_ms=0.0,
        success_rate=100.0,
        baseline_label="推荐基线" if qps > 0 else "待优化",
        sample_errors=_extract_sample_errors(parse_source),
        raw_output=parse_source,
    )


def _check_gaussdb_connectivity(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    masked_password = "******" if config.password else ""
    _emit_log(
        log_callback,
        f"连接检测启动: GaussDB JDBC -> hosts={_gaussdb_target_hosts(config)} user={config.user} database={config.database} password={masked_password}",
    )
    workdir = _prepare_gaussdb_workdir(config, sorted(set(config.threads_values))[0])
    run_dir = workdir / "run"
    sql_file = run_dir / "qdbmark_connectivity.sql"
    sql_file.write_text("-- QDBmark GaussDB connectivity check\nselect 1;\n", encoding="utf-8")
    _emit_log(log_callback, f"GaussDB JDBC 连通性工作目录: {workdir}")
    _emit_log(log_callback, f"GaussDB JDBC 连接串: {_gaussdb_conn_string(config)}")
    output = _run_command(
        ["bash", "runSQL.sh", _gaussdb_props_name(config), "qdbmark_connectivity.sql"],
        timeout_seconds=120,
        allow_failure=True,
        log_callback=log_callback,
        log_prefix="[gaussdb] ",
        log_label="gaussdb-jdbc-connectivity",
        secrets=[config.password],
        cancel_callback=None,
        cwd=str(run_dir),
    )
    lowered = output.lower()
    error_markers = [
        "exception",
        "could not connect",
        "connection refused",
        "connection timed out",
        "authentication failed",
        "password authentication failed",
        "no suitable driver",
        "sasl",
        "fatal",
        "error:",
    ]
    if any(marker in lowered for marker in error_markers):
        raise BenchmarkValidationError(f"GaussDB JDBC 连接测试失败，请检查地址、端口、用户、密码、数据库和 JDBC 驱动。\n{output[-1600:]}")
    _emit_log(log_callback, "GaussDB JDBC 连接检测通过。")


def _summarize_gaussdb_failure(output: str, config: BenchmarkConfig, concurrency: int) -> str:
    lines = [line.strip() for line in str(output).splitlines() if line.strip()]
    interesting = [
        line
        for line in lines
        if (
            "memory is temporarily unavailable" in line.lower()
            or line.startswith("ERROR")
            or " ERROR " in line
            or line.startswith("FATAL")
            or " FATAL " in line
            or "Unexpected SQLException" in line
            or "SyntaxError" in line
        )
    ]
    sample = "\n".join(interesting[:16] or lines[-16:])
    if "memory is temporarily unavailable" in str(output).lower():
        return (
            "GaussDB 压测失败：数据库返回 memory is temporarily unavailable。"
            f"当前 terminals={concurrency}、warehouses={config.table_size}、runMins={_gaussdb_run_minutes(config)}，"
            "说明目标库在该并发压力下内存或连接资源不足，BenchmarkSQL 工具本身已经成功连库并开始执行。"
            "建议先降低线程配置，或按 GaussDB 规格调大数据库内存/连接相关参数后重试。\n"
            f"关键日志：\n{sample}"
        )
    return f"GaussDB BenchmarkSQL 执行失败，请检查工具输出和数据库状态。\n关键日志：\n{sample}"


def _run_gaussdb_benchmark(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> BenchmarkSummary:
    architecture_label = "分布式" if str(config.gaussdb_architecture or "centralized").strip().lower() == "distributed" else "集中式"
    _emit_log(log_callback, f"已接收测试任务: GaussDB({architecture_label}) / {_gaussdb_target_hosts(config)}/{config.database}")
    _emit_log(
        log_callback,
        f"压测工具: GaussDB BenchmarkSQL TPC-C | 并发配置: {','.join(str(value) for value in sorted(set(config.threads_values)))} | 时长: {config.duration_seconds}s | 仓库数: {config.table_size} | 造数并发: {config.table_count}",
    )
    _check_cancel(cancel_callback)
    _check_gaussdb_connectivity(config, log_callback=log_callback)

    results: List[ConcurrencyResult] = []
    dataset_prepared = False
    for concurrency in sorted(set(config.threads_values)):
        _check_cancel(cancel_callback)
        workdir = _prepare_gaussdb_workdir(config, concurrency)
        _emit_log(log_callback, f"GaussDB BenchmarkSQL 工作目录: {workdir}")
        _emit_log(log_callback, f"GaussDB BenchmarkSQL 配置: {_gaussdb_props_name(config)}，建表脚本: {_gaussdb_table_variant_name(config)}")
        if config.auto_prepare and not dataset_prepared:
            _emit_log(log_callback, "开始执行 GaussDB BenchmarkSQL TPC-C 环境清理...")
            _run_gaussdb_tool_script(
                config,
                workdir,
                "runDatabaseDestroy.sh",
                timeout_seconds=max(900, int(config.table_size) * 10),
                log_label="gaussdb-benchmarksql-destroy",
                log_callback=log_callback,
                cancel_callback=cancel_callback,
                allow_failure=True,
            )
            _emit_log(log_callback, "开始执行 GaussDB BenchmarkSQL TPC-C 造数...")
            _run_gaussdb_tool_script(
                config,
                workdir,
                "runDatabaseBuild.sh",
                timeout_seconds=max(7200, int(config.table_size) * 120),
                log_label="gaussdb-benchmarksql-build",
                log_callback=log_callback,
                cancel_callback=cancel_callback,
            )
            dataset_prepared = True
        elif config.auto_prepare:
            _emit_log(log_callback, "GaussDB 压测数据已在本任务内准备完成，后续并发组复用同一数据集。")
        _emit_log(log_callback, f"开始执行 GaussDB BenchmarkSQL TPC-C 压测: terminals={concurrency}")
        try:
            output = _run_gaussdb_tool_script(
                config,
                workdir,
                "runBenchmark.sh",
                timeout_seconds=config.duration_seconds + config.warmup_seconds + 900,
                log_label=f"{concurrency}并发GaussDB BenchmarkSQL测试",
                log_callback=log_callback,
                cancel_callback=cancel_callback,
            )
        except BenchmarkValidationError as exc:
            failure_output = "\n".join(part for part in [str(exc), _read_gaussdb_result_tail(workdir)] if part)
            raise BenchmarkValidationError(_summarize_gaussdb_failure(failure_output, config, concurrency)) from exc
        result = _parse_gaussdb_output(config, concurrency, output, workdir)
        results.append(result)
        _emit_log(log_callback, f"并发 {concurrency}: GaussDB BenchmarkSQL 完成，tpmTOTAL={result.tps}，tpmC={result.qps}")

    return BenchmarkSummary(
        report_title=config.report_title,
        executed_at=_beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
        target={
            "db_type": "GaussDB",
            "host": config.host,
            "port": config.port,
            "hosts": _gaussdb_target_hosts(config),
            "database": config.database,
            "user": config.user,
            "collection_name": "",
            "gaussdb_architecture": str(config.gaussdb_architecture or "centralized").strip().lower(),
        },
        parameters={
            "benchmark_engine": "GaussDB BenchmarkSQL",
            "workload": "TPC-C",
            "gaussdb_architecture": str(config.gaussdb_architecture or "centralized").strip().lower(),
            "gaussdb_cn_hosts": str(config.gaussdb_cn_hosts or "").strip(),
            "gaussdb_conn_params": _gaussdb_conn_params(config),
            "duration_seconds": config.duration_seconds,
            "warmup_seconds": config.warmup_seconds,
            "report_interval": 0,
            "table_count": config.table_count,
            "table_size": config.table_size,
            "operation_count": 0,
            "auth_database": "",
            "collection_name": "",
            "ssl_enabled": False,
            "ssl_verify": False,
            "auto_prepare": config.auto_prepare,
            "auto_cleanup": False,
            "threads_values": sorted(set(config.threads_values)),
        },
        results=results,
        recommended_baseline=_select_recommended_baseline(results),
    )


def _find_kingbase_benchmarksql_home() -> Path:
    configured_raw = os.environ.get("KINGBASE_BENCHMARKSQL_HOME", "").strip()
    candidates: List[Path] = []
    if configured_raw:
        candidates.append(Path(configured_raw))
    candidates.extend(
        [
            Path("/app/vendor/benchmarksql-kingbase"),
            Path("/app/vendor/benchmarksql-5.0-kingbase"),
            BASE_DIR.parent / "vendor" / "benchmarksql-kingbase",
            BASE_DIR.parent / "vendor" / "benchmarksql-5.0-kingbase",
        ]
    )
    for candidate in candidates:
        if (
            (candidate / "run" / "runDatabaseBuild.sh").exists()
            and (candidate / "run" / "runDatabaseDestroy.sh").exists()
            and (candidate / "run" / "runBenchmark.sh").exists()
            and (candidate / "dist" / "BenchmarkSQL-5.0.jar").exists()
        ):
            _ensure_kingbase_jdbc_driver(candidate)
            return candidate
    raise BenchmarkValidationError(
        "未找到 KingBase BenchmarkSQL 工具包。请将按手册适配后的 benchmarksql-5.0-kingbase 放到 /app/vendor/benchmarksql-kingbase，"
        "或设置 KINGBASE_BENCHMARKSQL_HOME 指向该目录。"
    )


def _ensure_kingbase_jdbc_driver(benchmarksql_home: Path) -> None:
    driver_dirs = [
        benchmarksql_home / "lib" / "kingbase",
        benchmarksql_home / "lib",
    ]
    for driver_dir in driver_dirs:
        if not driver_dir.exists():
            continue
        for jar in driver_dir.glob("*.jar"):
            jar_name = jar.name.lower()
            if "kingbase" in jar_name or "kingbase8" in jar_name:
                return
    raise BenchmarkValidationError(
        "未找到 KingBase JDBC 驱动。请按手册将 kingbase8 JDBC jar 放到 BenchmarkSQL 的 lib/kingbase 目录后再执行。"
    )


def _kingbase_option(config: BenchmarkConfig, name: str, default: Any) -> str:
    options = _parse_cli_options(config.extra_options)
    return str(options.get(name.replace("-", "_"), options.get(name, default))).strip()


def _kingbase_run_minutes(config: BenchmarkConfig) -> int:
    return max(int((config.duration_seconds + 59) / 60), 1)


def _kingbase_conn_string(config: BenchmarkConfig) -> str:
    configured_conn = _kingbase_option(config, "conn", "")
    if configured_conn:
        return configured_conn
    conn_params = _kingbase_option(config, "conn-params", "")
    suffix = f"?{conn_params}" if conn_params else ""
    return f"jdbc:kingbase8://{config.host}:{config.port}/{config.database}{suffix}"


def _write_kingbase_props(path: Path, config: BenchmarkConfig, *, concurrency: int) -> None:
    driver = _kingbase_option(config, "driver", "com.kingbase8.Driver")
    load_workers = _kingbase_option(config, "loadworkers", str(config.table_count)) or str(config.table_count)
    content = f"""db=kingbase
driver={driver}
conn={_kingbase_conn_string(config)}
user={config.user}
password={config.password}

warehouses={config.table_size}
loadWorkers={load_workers}

terminals={concurrency}
//To run specified transactions per terminal- runMins must equal zero
runTxnsPerTerminal=0
//To run for specified minutes- runTxnsPerTerminal must equal zero
runMins={_kingbase_run_minutes(config)}
//Number of total transactions per minute
limitTxnsPerMin=0

//Set to true to run in 4.x compatible mode. Set to false to use the
//entire configured database evenly.
terminalWarehouseFixed=true

//The following five values must add up to 100
newOrderWeight=45
paymentWeight=43
orderStatusWeight=4
deliveryWeight=4
stockLevelWeight=4

// Directory name to create for collecting detailed result data.
resultDirectory=qdbmark_kingbase_result_%tY-%tm-%td_%tH%tM%tS
"""
    path.write_text(content, encoding="utf-8")


def _prepare_kingbase_workdir(config: BenchmarkConfig, concurrency: int) -> Path:
    source = _find_kingbase_benchmarksql_home()
    if config.artifact_dir:
        base_dir = Path(config.artifact_dir)
    else:
        base_dir = Path(tempfile.mkdtemp(prefix="kingbase_benchmark_"))
    base_dir.mkdir(parents=True, exist_ok=True)
    workdir = base_dir / f"benchmarksql_kingbase_{concurrency}"
    if workdir.exists():
        shutil.rmtree(workdir)
    shutil.copytree(source, workdir)
    run_dir = workdir / "run"
    for script_name in ("runDatabaseDestroy.sh", "runDatabaseBuild.sh", "runBenchmark.sh", "runLoader.sh", "runSQL.sh"):
        script_path = run_dir / script_name
        if script_path.exists():
            script_path.chmod(script_path.stat().st_mode | 0o111)
    _write_kingbase_props(run_dir / "props.qdbmark_kingbase", config, concurrency=concurrency)
    return workdir


def _run_kingbase_tool_script(
    config: BenchmarkConfig,
    workdir: Path,
    script_name: str,
    timeout_seconds: int,
    log_label: str,
    log_callback: Optional[LogCallback],
    cancel_callback: Optional[CancelCallback],
    allow_failure: bool = False,
) -> str:
    return _run_command(
        ["bash", script_name, "props.qdbmark_kingbase"],
        timeout_seconds=timeout_seconds,
        allow_failure=allow_failure,
        log_callback=log_callback,
        log_prefix="[kingbase] ",
        log_label=log_label,
        secrets=[config.password],
        cancel_callback=cancel_callback,
        cwd=str(workdir / "run"),
    )


def _read_kingbase_result_tail(workdir: Path) -> str:
    run_dir = workdir / "run"
    chunks: List[str] = []
    for result_dir in sorted(run_dir.glob("qdbmark_kingbase_result_*"), key=lambda item: item.stat().st_mtime if item.exists() else 0, reverse=True)[:3]:
        for result_file in sorted(result_dir.rglob("*"), key=lambda item: item.stat().st_mtime if item.exists() else 0, reverse=True)[:8]:
            if not result_file.is_file():
                continue
            try:
                chunks.append(result_file.read_text(encoding="utf-8", errors="replace")[-8000:])
            except Exception:
                continue
    return "\n".join(chunks)


def _parse_kingbase_output(config: BenchmarkConfig, concurrency: int, output: str, workdir: Path) -> ConcurrencyResult:
    parse_source = "\n".join(part for part in [output, _read_kingbase_result_tail(workdir)] if part)
    tpmc_values = [float(value) for value in re.findall(r"Measured\s+tpmC\s*\(NewOrders\)\s*=\s*([0-9]+(?:\.[0-9]+)?)", parse_source, re.IGNORECASE)]
    tpmtotal_values = [float(value) for value in re.findall(r"Measured\s+tpmTOTAL\s*=\s*([0-9]+(?:\.[0-9]+)?)", parse_source, re.IGNORECASE)]
    qps = round(tpmc_values[-1], 2) if tpmc_values else 0.0
    tps = round(tpmtotal_values[-1], 2) if tpmtotal_values else qps
    if qps <= 0 and tps <= 0:
        raise BenchmarkValidationError(f"未能解析 KingBase BenchmarkSQL 输出，请检查执行结果。\n{parse_source[-1200:]}")
    return ConcurrencyResult(
        concurrency=concurrency,
        elapsed_seconds=config.duration_seconds + config.warmup_seconds,
        total_transactions=int(tps),
        total_queries=int(qps),
        total_events=int(tps or qps),
        read_queries=0,
        write_queries=0,
        other_queries=0,
        failed_requests=0,
        qps=qps,
        tps=tps,
        avg_latency_ms=0.0,
        p95_latency_ms=0.0,
        min_latency_ms=0.0,
        max_latency_ms=0.0,
        success_rate=100.0,
        baseline_label="推荐基线" if qps > 0 else "待优化",
        sample_errors=_extract_sample_errors(parse_source),
        raw_output=parse_source,
    )


def _check_kingbase_connectivity(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    import psycopg2

    masked_password = "******" if config.password else ""
    _emit_log(
        log_callback,
        f"连接检测启动: KingBase -> host={config.host} port={config.port} user={config.user} database={config.database} password={masked_password}",
    )
    connection = None
    try:
        connection = psycopg2.connect(**_postgres_connect_kwargs(config))
        with connection.cursor() as cursor:
            cursor.execute("select version(), current_database(), now()")
            version, database_name, checked_at = cursor.fetchone()
        _emit_log(
            log_callback,
            f"KingBase 连接检测通过: version={str(version).splitlines()[0]}, database={database_name}, checked_at={checked_at}",
        )
    except Exception as exc:
        raise BenchmarkValidationError(f"KingBase 连接测试失败: {exc}") from exc
    finally:
        try:
            if connection is not None:
                connection.close()
        except Exception:
            pass


def _run_kingbase_benchmark(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> BenchmarkSummary:
    _emit_log(log_callback, f"已接收测试任务: KingBase / {config.host}:{config.port}/{config.database}")
    _emit_log(
        log_callback,
        f"压测工具: KingBase BenchmarkSQL | 并发配置: {','.join(str(value) for value in sorted(set(config.threads_values)))} | 时长: {config.duration_seconds}s | 仓库数: {config.table_size}",
    )
    _check_cancel(cancel_callback)
    _check_kingbase_connectivity(config, log_callback=log_callback)

    results: List[ConcurrencyResult] = []
    for concurrency in sorted(set(config.threads_values)):
        _check_cancel(cancel_callback)
        workdir = _prepare_kingbase_workdir(config, concurrency)
        _emit_log(log_callback, f"KingBase BenchmarkSQL 工作目录: {workdir}")
        if config.auto_prepare:
            _emit_log(log_callback, "开始执行 KingBase BenchmarkSQL TPC-C 环境清理...")
            _run_kingbase_tool_script(
                config,
                workdir,
                "runDatabaseDestroy.sh",
                timeout_seconds=max(900, int(config.table_size) * 10),
                log_label="kingbase-benchmarksql-destroy",
                log_callback=log_callback,
                cancel_callback=cancel_callback,
                allow_failure=True,
            )
            _emit_log(log_callback, "开始执行 KingBase BenchmarkSQL TPC-C 造数...")
            _run_kingbase_tool_script(
                config,
                workdir,
                "runDatabaseBuild.sh",
                timeout_seconds=max(7200, int(config.table_size) * 120),
                log_label="kingbase-benchmarksql-build",
                log_callback=log_callback,
                cancel_callback=cancel_callback,
            )
        _emit_log(log_callback, f"开始执行 KingBase BenchmarkSQL TPC-C 压测: terminals={concurrency}")
        output = _run_kingbase_tool_script(
            config,
            workdir,
            "runBenchmark.sh",
            timeout_seconds=config.duration_seconds + config.warmup_seconds + 900,
            log_label=f"{concurrency}并发KingBase BenchmarkSQL测试",
            log_callback=log_callback,
            cancel_callback=cancel_callback,
        )
        result = _parse_kingbase_output(config, concurrency, output, workdir)
        results.append(result)
        _emit_log(log_callback, f"并发 {concurrency}: KingBase BenchmarkSQL 完成，tpmTOTAL={result.tps}，tpmC={result.qps}")

    return BenchmarkSummary(
        report_title=config.report_title,
        executed_at=_beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
        target={
            "db_type": "KingBase",
            "host": config.host,
            "port": config.port,
            "database": config.database,
            "user": config.user,
            "collection_name": "",
        },
        parameters={
            "benchmark_engine": "KingBase BenchmarkSQL",
            "workload": "TPC-C",
            "duration_seconds": config.duration_seconds,
            "warmup_seconds": config.warmup_seconds,
            "report_interval": 0,
            "table_count": config.table_count,
            "table_size": config.table_size,
            "operation_count": 0,
            "auth_database": "",
            "collection_name": "",
            "ssl_enabled": False,
            "ssl_verify": False,
            "auto_prepare": config.auto_prepare,
            "auto_cleanup": False,
            "threads_values": sorted(set(config.threads_values)),
        },
        results=results,
        recommended_baseline=_select_recommended_baseline(results),
    )


def _check_sqlserver_connectivity(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    masked_password = "******" if config.password else ""
    _emit_log(
        log_callback,
        f"连接检测启动: SQL Server -> host={config.host} port={config.port} user={config.user} database={config.database} password={masked_password}",
    )
    try:
        import pymssql  # type: ignore

        with pymssql.connect(
            server=config.host,
            port=str(config.port),
            user=config.user,
            password=config.password,
            database=config.database,
            login_timeout=10,
            timeout=10,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("select @@version")
                row = cursor.fetchone()
        version = str(row[0] if row else "").splitlines()[0]
        _emit_log(log_callback, f"SQL Server 连接检测通过: {version}")
        return
    except ModuleNotFoundError:
        _emit_log(log_callback, "当前运行环境未安装 pymssql，先执行端口可达性检测；正式 HammerDB 执行会继续校验账号密码。")
    except Exception as exc:
        raise BenchmarkValidationError(f"SQL Server 连接测试失败: {exc}") from exc

    try:
        with socket.create_connection((config.host, int(config.port)), timeout=10):
            pass
    except Exception as exc:
        raise BenchmarkValidationError(f"SQL Server 端口不可达: {exc}") from exc
    _emit_log(log_callback, "SQL Server 端口连通性检测通过。")


def _parse_hammerdb_sqlserver_output(config: BenchmarkConfig, concurrency: int, output: str) -> ConcurrencyResult:
    parse_source = output
    nopm_values = [float(value) for value in re.findall(r"([0-9]+(?:\.[0-9]+)?)\s+NOPM", parse_source, flags=re.IGNORECASE)]
    tpm_values = [
        float(value)
        for value in re.findall(r"([0-9]+(?:\.[0-9]+)?)\s+(?:SQL\s+Server\s+)?TPM", parse_source, flags=re.IGNORECASE)
    ]
    if not nopm_values and not tpm_values:
        hammerdb_log = _read_hammerdb_log_tail(config)
        if hammerdb_log:
            parse_source = hammerdb_log
            nopm_values = [float(value) for value in re.findall(r"([0-9]+(?:\.[0-9]+)?)\s+NOPM", parse_source, flags=re.IGNORECASE)]
            tpm_values = [
                float(value)
                for value in re.findall(r"([0-9]+(?:\.[0-9]+)?)\s+(?:SQL\s+Server\s+)?TPM", parse_source, flags=re.IGNORECASE)
            ]
    qps = round(nopm_values[-1], 2) if nopm_values else 0.0
    tps = round(tpm_values[-1], 2) if tpm_values else qps
    if qps <= 0 and tps <= 0:
        raise BenchmarkValidationError(f"未能解析 HammerDB SQL Server 输出，请检查执行结果。\n{parse_source[-1200:]}")
    return ConcurrencyResult(
        concurrency=concurrency,
        elapsed_seconds=config.duration_seconds + config.warmup_seconds,
        total_transactions=int(tps),
        total_queries=int(qps),
        total_events=int(tps or qps),
        read_queries=0,
        write_queries=0,
        other_queries=0,
        failed_requests=0,
        qps=qps,
        tps=tps,
        avg_latency_ms=0.0,
        p95_latency_ms=0.0,
        min_latency_ms=0.0,
        max_latency_ms=0.0,
        success_rate=100.0,
        baseline_label="推荐基线" if qps > 0 else "待优化",
        sample_errors=_extract_sample_errors(parse_source),
        raw_output=parse_source,
    )


def _swingbench_connect_string(config: BenchmarkConfig) -> str:
    return _oracle_easy_connect(config)


def _run_swingbench_build(config: BenchmarkConfig, log_callback: Optional[LogCallback], cancel_callback: Optional[CancelCallback]) -> None:
    options = _parse_cli_options(config.extra_options)
    scale = options.get("scale", str(config.table_size))
    soe_user = options.get("soe_user", "soe")
    soe_pass = options.get("soe_pass", "soe")
    tablespace = options.get("tablespace", "SOE")
    tc = options.get("tc", "16")
    command = [
        _find_swingbench_command("oewizard"),
        "-scale",
        scale,
        "-create",
        "-nocompress",
        "-cs",
        _swingbench_connect_string(config),
        "-dbap",
        config.password,
        "-ts",
        tablespace,
        "-tc",
        tc,
        "-hashpart",
        "-allindexes",
        "-u",
        soe_user,
        "-p",
        soe_pass,
        "-cl",
        "-v",
    ]
    _run_command(
        command,
        timeout_seconds=7200,
        log_callback=log_callback,
        log_prefix="[swingbench-build] ",
        log_label="swingbench-oewizard",
        secrets=[config.password, soe_pass],
        cancel_callback=cancel_callback,
    )


def _run_swingbench(config: BenchmarkConfig, concurrency: int, log_callback: Optional[LogCallback], cancel_callback: Optional[CancelCallback]) -> str:
    options = _parse_cli_options(config.extra_options)
    soe_user = options.get("soe_user", "soe")
    soe_pass = options.get("soe_pass", "soe")
    runtime_minutes = max(int(config.duration_seconds / 60), 1)
    command = [
        _find_swingbench_command("charbench"),
        "-cs",
        _swingbench_connect_string(config),
        "-u",
        soe_user,
        "-p",
        soe_pass,
        "-uc",
        str(concurrency),
        "-rt",
        f"0:{runtime_minutes:02d}",
        "-v",
        "users,cpu,disk,tpm,tps,resp",
    ]
    return _run_command(
        command,
        timeout_seconds=config.duration_seconds + config.warmup_seconds + 900,
        log_callback=log_callback,
        log_prefix=f"[swingbench:{concurrency}] ",
        log_label=f"{concurrency}并发Oracle Swingbench测试",
        secrets=[config.password, soe_pass],
        cancel_callback=cancel_callback,
    )


def _parse_swingbench_oracle_output(config: BenchmarkConfig, concurrency: int, output: str) -> ConcurrencyResult:
    tps_matches = [float(value) for value in re.findall(r"(?:TPS|AverageTransactionsPerSecond)[^0-9]*([0-9]+(?:\.[0-9]+)?)", output, flags=re.IGNORECASE)]
    if not tps_matches:
        numeric_lines = re.findall(r"\\b([0-9]+(?:\\.[0-9]+)?)\\b", output)
        tps_matches = [float(numeric_lines[-1])] if numeric_lines else []
    tps = round(tps_matches[-1], 2) if tps_matches else 0.0
    if tps <= 0:
        raise BenchmarkValidationError(f"未能解析 Swingbench 输出，请检查执行结果。\n{output[-1200:]}")
    return ConcurrencyResult(
        concurrency=concurrency,
        elapsed_seconds=config.duration_seconds,
        total_transactions=int(tps * config.duration_seconds),
        total_queries=int(tps * config.duration_seconds),
        total_events=int(tps * config.duration_seconds),
        read_queries=0,
        write_queries=0,
        other_queries=0,
        failed_requests=0,
        qps=round(tps * 60, 2),
        tps=tps,
        avg_latency_ms=0.0,
        p95_latency_ms=0.0,
        min_latency_ms=0.0,
        max_latency_ms=0.0,
        success_rate=100.0,
        baseline_label="推荐基线",
        sample_errors=_extract_sample_errors(output),
        raw_output=output,
    )


def _fio_display_target(config: BenchmarkConfig) -> str:
    if config.fio_ssh_enabled:
        return f"{config.fio_ssh_user}@{config.fio_ssh_host}:{config.fio_target_path}"
    return config.fio_target_path


def _resolve_workload_entry(workload: str, sysbench_bin: str) -> str:
    sysbench_path = Path(sysbench_bin).resolve()
    candidate_dirs = [
        VENDORED_SYSBENCH_SHARE,
        sysbench_path.parent.parent / "share" / "sysbench",
        Path("/usr/share/sysbench"),
    ]
    for directory in candidate_dirs:
        candidate = directory / f"{workload}.lua"
        if candidate.exists():
            return str(candidate)
    return workload


def _emit_log(log_callback: Optional[LogCallback], message: str) -> None:
    if log_callback:
        log_callback(message)


def _check_cancel(cancel_callback: Optional[CancelCallback]) -> None:
    if cancel_callback and cancel_callback():
        raise BenchmarkCancelledError("测试已终止。")


def _masked_command(command: List[str], secrets: List[str]) -> str:
    masked_command: List[str] = []
    for item in command:
        current = item
        for secret in secrets:
            if secret:
                current = current.replace(secret, "******")
        masked_command.append(current)
    return " ".join(masked_command)


def _resolve_ycsb_workload_entry(workload: str, ycsb_bin: str) -> str:
    ycsb_path = Path(ycsb_bin).resolve()
    candidate_dirs = [
        VENDORED_YCSB_DIR / "workloads",
        ycsb_path.parent.parent / "workloads",
    ]
    for directory in candidate_dirs:
        candidate = directory / workload
        if candidate.exists():
            return str(candidate)
    return workload


def _mongo_connection_hosts(config: BenchmarkConfig) -> str:
    if config.mongo_hosts.strip():
        return config.mongo_hosts.strip()
    return f"{config.host}:{config.port}"


def _mongo_display_target(config: BenchmarkConfig) -> str:
    return f"{_mongo_connection_hosts(config)}/{config.database}"


def _build_mongodb_uri(config: BenchmarkConfig) -> str:
    credentials = ""
    if config.user:
        credentials = quote_plus(config.user)
        if config.password:
            credentials += f":{quote_plus(config.password)}"
        credentials += "@"

    query: Dict[str, str] = {"w": "0"}
    auth_database = config.auth_database or config.database
    if auth_database and auth_database != config.database:
        query["authSource"] = auth_database
    if config.mongo_topology == "standalone":
        query["directConnection"] = "true"
    if config.mongo_topology == "replica_set" and config.mongo_replica_set:
        query["replicaSet"] = config.mongo_replica_set
    if config.mongo_read_preference:
        query["readPreference"] = config.mongo_read_preference
    if config.ssl_enabled:
        query["tls"] = "true"
        if not config.ssl_verify:
            query["tlsAllowInvalidCertificates"] = "true"

    uri = f"mongodb://{credentials}{_mongo_connection_hosts(config)}/{config.database}"
    if query:
        uri = f"{uri}?{urlencode(query)}"
    return uri


def _create_mongo_client(config: BenchmarkConfig):
    from pymongo import MongoClient

    client_kwargs: Dict[str, Any] = {"serverSelectionTimeoutMS": 5000}
    if config.ssl_enabled and config.ssl_ca_file:
        client_kwargs["tlsCAFile"] = config.ssl_ca_file
    return MongoClient(_build_mongodb_uri(config), **client_kwargs)


def _mysql_connect_kwargs(config: BenchmarkConfig) -> Dict[str, Any]:
    import pymysql

    connect_kwargs: Dict[str, Any] = {
        "host": config.host,
        "port": config.port,
        "user": config.user,
        "password": config.password,
        "database": config.database,
        "connect_timeout": 5,
        "read_timeout": 10,
        "write_timeout": 10,
        "charset": "utf8mb4",
        "cursorclass": pymysql.cursors.DictCursor,
    }
    if config.ssl_enabled:
        connect_kwargs["ssl"] = {
            "cert_reqs": ssl.CERT_REQUIRED if config.ssl_verify else ssl.CERT_NONE,
            "check_hostname": bool(config.ssl_verify),
        }
        if config.ssl_ca_file:
            connect_kwargs["ssl"]["ca"] = config.ssl_ca_file
    return connect_kwargs


def _postgres_connect_kwargs(config: BenchmarkConfig) -> Dict[str, Any]:
    connect_kwargs: Dict[str, Any] = {
        "host": config.host,
        "port": config.port,
        "user": config.user,
        "password": config.password,
        "dbname": config.database,
        "connect_timeout": 5,
    }
    if config.ssl_enabled:
        connect_kwargs["sslmode"] = "verify-full" if config.ssl_verify else "require"
        if config.ssl_ca_file:
            connect_kwargs["sslrootcert"] = config.ssl_ca_file
    if config.postgres_schema.strip():
        connect_kwargs["options"] = f"-c {_postgres_search_path_setting(config.postgres_schema)}"
    return connect_kwargs


def _postgres_command_env(config: BenchmarkConfig) -> Dict[str, str]:
    env: Dict[str, str] = {}
    if config.password:
        env["PGPASSWORD"] = config.password
    if config.ssl_enabled:
        env["PGSSLMODE"] = "verify-full" if config.ssl_verify else "require"
        if config.ssl_ca_file:
            env["PGSSLROOTCERT"] = config.ssl_ca_file
    if config.postgres_schema.strip():
        env["PGOPTIONS"] = f"-c {_postgres_search_path_setting(config.postgres_schema)}"
    return env


def _postgres_quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _postgres_search_path_setting(schema_name: str) -> str:
    schema = _postgres_quote_identifier(schema_name.strip())
    return f"search_path={schema},public"


def _resolve_sysbench_ssl_mode(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> str:
    if not config.ssl_enabled:
        return "off"
    if config.ssl_verify:
        return "on"

    _emit_log(
        log_callback,
        "检测到当前配置为“启用 SSL 但不严格校验证书”。sysbench 仅支持 --mysql-ssl=on/off，不支持跳过证书校验，已自动将 sysbench SSL 切换为 off。",
    )
    return "off"


def _mysql_dataset_cache_key(config: BenchmarkConfig) -> str:
    return json.dumps(
        {
            "db_type": config.db_type,
            "host": config.host,
            "port": config.port,
            "database": config.database,
            "table_count": config.table_count,
            "table_size": config.table_size,
        },
        sort_keys=True,
        ensure_ascii=False,
    )


def _mysql_dataset_cache_state(config: BenchmarkConfig) -> Dict[str, Any]:
    cache_key = _mysql_dataset_cache_key(config)
    with MYSQL_DATASET_CACHE_LOCK:
        return dict(MYSQL_DATASET_CACHE.get(cache_key, {}))


def _mark_mysql_dataset_prepared(config: BenchmarkConfig) -> None:
    cache_key = _mysql_dataset_cache_key(config)
    with MYSQL_DATASET_CACHE_LOCK:
        state = dict(MYSQL_DATASET_CACHE.get(cache_key, {}))
        state["prepared"] = True
        state["prepared_at"] = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")
        MYSQL_DATASET_CACHE[cache_key] = state


def _mark_mysql_dataset_warmed(config: BenchmarkConfig, warmup_seconds: int) -> None:
    cache_key = _mysql_dataset_cache_key(config)
    with MYSQL_DATASET_CACHE_LOCK:
        state = dict(MYSQL_DATASET_CACHE.get(cache_key, {}))
        state["prepared"] = True
        state["warmup_seconds"] = max(int(state.get("warmup_seconds", 0)), int(warmup_seconds))
        state["warmed_at"] = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")
        MYSQL_DATASET_CACHE[cache_key] = state


def _clear_mysql_dataset_cache(config: BenchmarkConfig) -> None:
    cache_key = _mysql_dataset_cache_key(config)
    with MYSQL_DATASET_CACHE_LOCK:
        MYSQL_DATASET_CACHE.pop(cache_key, None)


def _mysql_stage_workload(stage: str, selected_workload: str) -> str:
    if stage in {"prepare", "warmup"}:
        return "oltp_read_write"
    return selected_workload


def _mysql_test_phase_label(workload: str) -> str:
    if workload in {"oltp_read_only", "oltp_point_select"}:
        return "QPS"
    if workload in {"oltp_read_write", "oltp_update_index"}:
        return "TPS"
    return "压测"


def _read_metric_workload(workload: str) -> bool:
    normalized = str(workload or "").strip().lower()
    return normalized in {"oltp_read_only", "oltp_point_select", "select_only", "read", "randread"} or normalized.endswith("read_only")


def _write_metric_workload(workload: str) -> bool:
    normalized = str(workload or "").strip().lower()
    return normalized in {"oltp_read_write", "oltp_update_index", "oltp_update_non_index", "tpcb_like", "write", "randwrite", "randrw"} or any(
        token in normalized for token in ("read_write", "update", "insert", "delete", "write")
    )


def _workload_metric_log_text(workload: str, result: ConcurrencyResult) -> str:
    if _read_metric_workload(workload):
        return f"QPS={result.qps}"
    if _write_metric_workload(workload):
        return f"TPS={result.tps}"
    return f"TPS={result.tps}，QPS={result.qps}"


def _common_command(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    workload: Optional[str] = None,
) -> List[str]:
    sysbench_bin = _find_sysbench()
    resolved_workload = workload or config.workload
    command = [
        sysbench_bin,
        _resolve_workload_entry(resolved_workload, sysbench_bin),
        "--db-driver=mysql",
        f"--mysql-host={config.host}",
        f"--mysql-port={config.port}",
        f"--mysql-user={config.user}",
        f"--mysql-password={config.password}",
        f"--mysql-db={config.database}",
        f"--tables={config.table_count}",
        f"--table-size={config.table_size}",
        "--db-ps-mode=disable",
    ]
    command.append(f"--mysql-ssl={_resolve_sysbench_ssl_mode(config, log_callback=log_callback)}")
    if config.ssl_enabled and config.ssl_ca_file:
        command.append(f"--mysql-ssl-ca={config.ssl_ca_file}")
    if config.extra_options.strip():
        command.extend(shlex.split(config.extra_options))
    return command


def _build_postgres_sysbench_command(
    config: BenchmarkConfig,
    workload: Optional[str] = None,
) -> List[str]:
    sysbench_bin = _find_sysbench()
    resolved_workload = workload or config.workload
    command = [
        sysbench_bin,
        _resolve_workload_entry(resolved_workload, sysbench_bin),
        "--db-driver=pgsql",
        f"--pgsql-host={config.host}",
        f"--pgsql-port={config.port}",
        f"--pgsql-user={config.user}",
        f"--pgsql-password={config.password}",
        f"--pgsql-db={config.database}",
        f"--tables={config.table_count}",
        f"--table-size={config.table_size}",
        "--db-ps-mode=disable",
    ]
    if config.extra_options.strip():
        command.extend(shlex.split(config.extra_options))
    return command


def _pgbench_query_count(workload: str) -> int:
    if workload == "tpcb_like":
        return 7
    return 1


def _build_pgbench_init_command(config: BenchmarkConfig) -> List[str]:
    return [
        _find_pgbench(),
        "-h",
        config.host,
        "-p",
        str(config.port),
        "-U",
        config.user,
        "-i",
        "-s",
        str(config.pgbench_scale),
        "-F",
        str(config.pgbench_fillfactor),
        config.database,
    ]


def _build_pgbench_run_command(config: BenchmarkConfig, clients: int) -> List[str]:
    pgbench_path = _find_pgbench()
    command = [
        pgbench_path,
        "-h",
        config.host,
        "-p",
        str(config.port),
        "-U",
        config.user,
        "-c",
        str(clients),
        "-T",
        str(config.duration_seconds),
        "-j",
        str(config.pgbench_jobs),
    ]
    if _pgbench_supports_option(pgbench_path, "-P"):
        command.extend(["-P", str(config.report_interval)])
    if _pgbench_supports_option(pgbench_path, "-L"):
        command.extend(["-L", str(config.pgbench_latency_limit)])
    command.extend(["-r", config.database])
    return command


def _run_command(
    command: List[str],
    timeout_seconds: int,
    allow_failure: bool = False,
    log_callback: Optional[LogCallback] = None,
    log_prefix: str = "",
    log_label: str = "命令",
    secrets: Optional[List[str]] = None,
    cancel_callback: Optional[CancelCallback] = None,
    env: Optional[Dict[str, str]] = None,
    cwd: Optional[str] = None,
    progress_callback: Optional[Callable[[], None]] = None,
) -> str:
    _check_cancel(cancel_callback)
    _emit_log(log_callback, f"{log_label}启动: {_masked_command(command, secrets or [])}")
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env={**os.environ, **env} if env else None,
        cwd=cwd,
        start_new_session=(os.name == "posix"),
    )

    def _stop_process_tree() -> None:
        if process.poll() is not None:
            return
        try:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGTERM)
            else:
                process.terminate()
            process.wait(timeout=5)
        except (OSError, subprocess.TimeoutExpired):
            try:
                if os.name == "posix":
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
            except OSError:
                pass

    lines: List[str] = []
    queue: Queue[str] = Queue()

    def _reader() -> None:
        assert process.stdout is not None
        try:
            for line in process.stdout:
                queue.put(line.rstrip())
        finally:
            process.stdout.close()

    reader = Thread(target=_reader, daemon=True)
    reader.start()
    deadline = monotonic() + timeout_seconds
    next_progress_poll = monotonic()

    while True:
        if cancel_callback and cancel_callback():
            _stop_process_tree()
            reader.join(timeout=1)
            raise BenchmarkCancelledError("测试已终止。")
        if monotonic() > deadline:
            _stop_process_tree()
            reader.join(timeout=1)
            raise BenchmarkValidationError(f"{log_label}执行超时，超过 {timeout_seconds} 秒。")

        if progress_callback and monotonic() >= next_progress_poll:
            try:
                progress_callback()
            except (OSError, UnicodeError):
                pass
            next_progress_poll = monotonic() + 0.5

        try:
            line = queue.get(timeout=0.2)
            lines.append(line)
            if line.strip():
                _emit_log(log_callback, f"{log_prefix}{line}")
        except Empty:
            pass

        if process.poll() is not None and queue.empty():
            break

    if progress_callback:
        try:
            progress_callback()
        except (OSError, UnicodeError):
            pass
    reader.join(timeout=1)
    return_code = process.wait()
    output = "\n".join(line for line in lines if line).strip()
    if return_code != 0 and not allow_failure:
        if _is_mysql_auth_plugin_load_error(output):
            raise BenchmarkValidationError(_mysql_auth_plugin_missing_message())
        if "rabbitmq" in str(log_label).lower():
            rabbitmq_error = _rabbitmq_error_summary(output)
            if rabbitmq_error:
                raise BenchmarkValidationError(rabbitmq_error)
        raise BenchmarkValidationError(output or f"{log_label}执行失败。")
    if return_code == 0:
        _emit_log(log_callback, f"{log_label}完成。")
    else:
        _emit_log(log_callback, f"{log_label}返回非 0，已按允许失败继续。")
    return output


def _check_mysql_connectivity(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    import pymysql

    _emit_log(log_callback, "开始检查数据库连接性...")
    masked_password = "******" if config.password else ""
    _emit_log(
        log_callback,
        "连接检测启动: pymysql connect -> "
        f"host={config.host} port={config.port} user={config.user} "
        f"database={config.database} ssl={'on' if config.ssl_enabled else 'off'} password={masked_password}",
    )

    connect_kwargs = _mysql_connect_kwargs(config)

    try:
        connection = pymysql.connect(**connect_kwargs)
        with connection.cursor() as cursor:
            cursor.execute("SELECT VERSION() AS version, NOW() AS checked_at")
            row = cursor.fetchone()
        _emit_log(log_callback, f"数据库连接检测通过: {row}")
    except Exception as exc:
        raise BenchmarkValidationError(str(exc)) from exc
    finally:
        try:
            connection.close()  # type: ignore[name-defined]
        except Exception:
            pass


def _check_postgresql_connectivity(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    import psycopg2

    _emit_log(log_callback, f"开始检查 {config.db_type} 连接性...")
    masked_password = "******" if config.password else ""
    _emit_log(
        log_callback,
        "连接检测启动: psycopg2 connect -> "
        f"host={config.host} port={config.port} user={config.user} "
        f"database={config.database} engine={config.postgres_engine} password={masked_password}",
    )

    connection = None
    try:
        connection = psycopg2.connect(**_postgres_connect_kwargs(config))
        with connection.cursor() as cursor:
            cursor.execute("SELECT version(), current_database(), now()")
            version, database_name, checked_at = cursor.fetchone()
        _emit_log(
            log_callback,
            f"{config.db_type} 连接检测通过: version={version}, database={database_name}, checked_at={checked_at}",
        )
    except Exception as exc:
        raise BenchmarkValidationError(str(exc)) from exc
    finally:
        try:
            if connection is not None:
                connection.close()
        except Exception:
            pass


def inspect_mysql_target_schema(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
) -> Dict[str, Any]:
    import pymysql

    config.validate()
    if config.db_type not in {"MySQL", "TiDB"}:
        raise BenchmarkValidationError("当前仅支持 MySQL / TiDB 目标库空库校验。")

    _emit_log(log_callback, f"开始校验目标库是否为空: {config.host}:{config.port}/{config.database}")
    if config.database in MYSQL_SYSTEM_DATABASES:
        _emit_log(
            log_callback,
            f"当前目标库 {config.database} 属于 MySQL/TiDB 系统库，检测到现有对象时将直接判定为非空。",
        )

    connection = pymysql.connect(**_mysql_connect_kwargs(config))
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT TABLE_NAME, TABLE_TYPE
                FROM information_schema.tables
                WHERE table_schema = %s
                ORDER BY TABLE_NAME
                """,
                (config.database,),
            )
            rows = cursor.fetchall() or []
    except Exception as exc:
        raise BenchmarkValidationError(f"MySQL 目标库空库校验失败: {exc}") from exc
    finally:
        try:
            connection.close()
        except Exception:
            pass

    object_names = [str(row.get("TABLE_NAME", "")).strip() for row in rows if str(row.get("TABLE_NAME", "")).strip()]
    object_types = [str(row.get("TABLE_TYPE", "")).strip() for row in rows if str(row.get("TABLE_NAME", "")).strip()]
    is_empty = len(object_names) == 0

    if is_empty:
        _emit_log(log_callback, f"目标库 {config.database} 为空库，可直接开始测试。")
    else:
        preview = "、".join(object_names[:12])
        if len(object_names) > 12:
            preview = f"{preview} 等 {len(object_names)} 个对象"
        _emit_log(
            log_callback,
            f"检测到目标库 {config.database} 中已存在 {len(object_names)} 个对象，示例: {preview}",
        )
        distinct_types = "、".join(sorted({item for item in object_types if item}))
        if distinct_types:
            _emit_log(log_callback, f"当前对象类型: {distinct_types}")
        _emit_log(log_callback, "有非空对象存在，请人工确认是否继续执行压测。")

    return {
        "database": config.database,
        "is_empty": is_empty,
        "table_count": len(object_names),
        "table_names": object_names,
        "table_types": object_types,
        "needs_confirmation": not is_empty,
    }


def _mysql_status_maps(config: BenchmarkConfig) -> tuple[Dict[str, Any], Dict[str, Any]]:
    import pymysql

    connection = pymysql.connect(**_mysql_connect_kwargs(config))
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SHOW GLOBAL VARIABLES
                WHERE Variable_name IN (
                    'version',
                    'version_comment',
                    'hostname',
                    'port',
                    'max_connections',
                    'innodb_buffer_pool_size'
                )
                """
            )
            variables_rows = cursor.fetchall() or []
            cursor.execute(
                """
                SHOW GLOBAL STATUS
                WHERE Variable_name IN (
                    'Threads_connected',
                    'Threads_running',
                    'Com_commit',
                    'Com_rollback',
                    'Questions',
                    'Queries',
                    'Bytes_received',
                    'Bytes_sent',
                    'Innodb_buffer_pool_bytes_data',
                    'Innodb_buffer_pool_pages_data',
                    'Innodb_buffer_pool_pages_total',
                    'Uptime'
                )
                """
            )
            status_rows = cursor.fetchall() or []

        variables = {str(item["Variable_name"]).lower(): item["Value"] for item in variables_rows}
        status = {str(item["Variable_name"]).lower(): item["Value"] for item in status_rows}
        return variables, status
    finally:
        connection.close()


def _format_mysql_max_connections(config: BenchmarkConfig, raw_value: Any) -> str:
    normalized = str(raw_value or "").strip()
    if config.db_type == "TiDB" and _safe_int(normalized) <= 0:
        return "不限"
    return normalized or "-"


def _build_mysql_instance_info(config: BenchmarkConfig) -> Dict[str, Any]:
    variables, status = _mysql_status_maps(config)
    buffer_pool_size = _safe_int(variables.get("innodb_buffer_pool_size"))
    prometheus_sample = _prometheus_mysql_runtime_sample(config) if config.prometheus_url else None
    container_stats = _resolve_container_runtime_stats(
        config,
        variables.get("hostname"),
        config.host,
    )
    cards = [
        {"label": "实例类型", "value": config.db_type or "MySQL"},
        {"label": "实例地址", "value": f"{config.host}:{config.port}/{config.database}"},
        {"label": "版本", "value": str(variables.get("version", "-"))},
        {"label": "版本说明", "value": str(variables.get("version_comment", "-"))},
        {"label": "实例主机", "value": str(variables.get("hostname", config.host))},
        {"label": "最大连接数", "value": _format_mysql_max_connections(config, variables.get("max_connections"))},
        {"label": "Buffer Pool", "value": _format_bytes(buffer_pool_size)},
        {"label": "实例运行时长", "value": f"{_safe_int(status.get('uptime'))} s"},
    ]
    if prometheus_sample:
        cards.extend(
            [
                {
                    "label": "CPU 上限",
                    "value": f"{prometheus_sample.get('container_cpu_limit_cores')} 核"
                    if prometheus_sample.get("container_cpu_limit_cores") is not None
                    else "-",
                },
                {
                    "label": "内存上限",
                    "value": _format_bytes(_safe_float(prometheus_sample.get("container_memory_limit_bytes"))),
                },
            ]
        )
    if container_stats:
        cards.extend(
            [
                {"label": "容器名称", "value": container_stats.get("container_name", "-")},
                {"label": "容器内存上限", "value": _format_bytes(_safe_float(container_stats.get("memory_limit_bytes")))},
            ]
        )
    return {
        "captured_at": _beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
        "cards": cards,
    }


def _build_mysql_runtime_sample(config: BenchmarkConfig) -> Dict[str, Any]:
    prometheus_sample = _prometheus_mysql_runtime_sample(config) if config.prometheus_url else None
    variables, status = _mysql_status_maps(config)
    buffer_pool_total = _safe_int(variables.get("innodb_buffer_pool_size"))
    buffer_pool_used = _safe_int(status.get("innodb_buffer_pool_bytes_data"))
    container_stats = _resolve_container_runtime_stats(
        config,
        variables.get("hostname"),
        config.host,
    )
    if buffer_pool_used <= 0:
        pages_data = _safe_int(status.get("innodb_buffer_pool_pages_data"))
        pages_total = _safe_int(status.get("innodb_buffer_pool_pages_total"))
        if pages_data > 0 and pages_total > 0 and buffer_pool_total > 0:
            buffer_pool_used = int(buffer_pool_total * (pages_data / pages_total))

    sample = {
        "captured_at": monotonic(),
        "configured_threads": ",".join(str(value) for value in sorted(set(config.threads_values))),
        "threads_connected": _safe_int(status.get("threads_connected")),
        "threads_running": _safe_int(status.get("threads_running")),
        "transactions_total": _safe_int(status.get("com_commit")) + _safe_int(status.get("com_rollback")),
        "queries_total": max(_safe_int(status.get("queries")), _safe_int(status.get("questions"))),
        "bytes_received_total": _safe_int(status.get("bytes_received")),
        "bytes_sent_total": _safe_int(status.get("bytes_sent")),
        "max_connections": _safe_int(variables.get("max_connections")),
        "buffer_pool_total": buffer_pool_total,
        "buffer_pool_used": buffer_pool_used,
        "container_name": (container_stats or {}).get("container_name", ""),
        "container_cpu_percent": (container_stats or {}).get("cpu_percent"),
        "container_memory_usage_bytes": (container_stats or {}).get("memory_usage_bytes"),
        "container_memory_limit_bytes": (container_stats or {}).get("memory_limit_bytes"),
        "container_io_ops_total": (container_stats or {}).get("io_ops_total"),
        "container_io_ops_rate": (container_stats or {}).get("io_ops_rate"),
    }
    if prometheus_sample:
        sample.update({key: value for key, value in prometheus_sample.items() if key != "captured_at"})
    return sample


def _format_mysql_runtime_metrics(
    config: BenchmarkConfig,
    current: Dict[str, Any],
    previous: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if current.get("metrics_source") == "prometheus":
        qps_value = _safe_float(current.get("prometheus_qps"), None)
        tps_value = _safe_float(current.get("prometheus_tps"), None)
        elapsed = max(_safe_float(current.get("captured_at")) - _safe_float((previous or {}).get("captured_at")), 0.0)
        if previous and elapsed > 0:
            if qps_value is None or qps_value <= 0:
                qps_value = max(_safe_int(current.get("queries_total")) - _safe_int(previous.get("queries_total")), 0) / elapsed
            if tps_value is None or tps_value <= 0:
                tps_value = max(
                    _safe_int(current.get("transactions_total")) - _safe_int(previous.get("transactions_total")),
                    0,
                ) / elapsed
        container_iops = _safe_float(current.get("container_io_ops_rate"), None)
        container_cpu_percent = _round_percent(current.get("container_cpu_percent"))
        container_cpu_limit = _safe_float(current.get("container_cpu_limit_cores"), None)
        container_memory_usage = _safe_float(current.get("container_memory_usage_bytes"), 0.0)
        container_memory_limit = _safe_float(current.get("container_memory_limit_bytes"), 0.0)
        container_memory_percent = _round_percent(current.get("container_memory_percent")) if current.get("container_memory_percent") is not None else None
        if container_memory_percent is None and container_memory_usage > 0 and container_memory_limit > 0:
            container_memory_percent = _round_percent(container_memory_usage / container_memory_limit * 100)
        pod_label = str(config.prometheus_instance or current.get("prometheus_pod") or "IOPS")
        return {
            "captured_at": _beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
            "cards": [
                {"label": "实时 QPS", "value": f"{round(qps_value, 2)} /s" if qps_value is not None else "-"},
                {"label": "实时 TPS", "value": f"{round(tps_value, 2)} /s" if tps_value is not None else "-"},
                {"label": "CPU 使用率", "value": f"{container_cpu_percent}%" if container_cpu_percent is not None else "-"},
                {"label": "内存使用率", "value": f"{container_memory_percent}%" if container_memory_percent is not None else "-"},
                {"label": "IOPS", "value": f"{round(container_iops, 2)} io/s" if container_iops is not None else "-"},
            ],
            "series": [
                {
                    "key": "queries_rate",
                    "label": "实时 QPS",
                    "chart": "qps",
                    "chart_label": "QPS",
                    "color": "#2f80ed",
                    "value": round(qps_value, 2) if qps_value is not None else None,
                    "display": f"{round(qps_value, 2)} /s" if qps_value is not None else "-",
                    "unit": "/s",
                    "format": "ops",
                },
                {
                    "key": "transactions_rate",
                    "label": "实时 TPS",
                    "chart": "tps",
                    "chart_label": "TPS",
                    "color": "#27ae60",
                    "value": round(tps_value, 2) if tps_value is not None else None,
                    "display": f"{round(tps_value, 2)} /s" if tps_value is not None else "-",
                    "unit": "/s",
                    "format": "ops",
                },
                {
                    "key": "container_cpu_pct",
                    "label": "CPU 使用率",
                    "chart": "cpu_usage",
                    "chart_label": "CPU 使用率",
                    "chart_meta": f"CPU 上限 {round(container_cpu_limit, 2)} 核" if container_cpu_limit is not None else "",
                    "color": "#65b84a",
                    "value": container_cpu_percent,
                    "display": f"{container_cpu_percent}%" if container_cpu_percent is not None else "-",
                    "unit": "%",
                    "format": "percent",
                },
                {
                    "key": "container_memory_pct",
                    "label": "内存使用率",
                    "chart": "memory_usage",
                    "chart_label": "内存使用率",
                    "color": "#65b84a",
                    "value": container_memory_percent,
                    "display": f"{container_memory_percent}%" if container_memory_percent is not None else "-",
                    "unit": "%",
                    "format": "percent",
                },
                {
                    "key": "container_iops",
                    "label": pod_label,
                    "chart": "iops",
                    "chart_label": "IOPS",
                    "color": "#65b84a",
                    "value": round(container_iops, 2) if container_iops is not None else None,
                    "display": f"{round(container_iops, 2)} io/s" if container_iops is not None else "-",
                    "unit": "io/s",
                    "format": "iops",
                },
            ],
        }

    elapsed = max(_safe_float(current.get("captured_at")) - _safe_float((previous or {}).get("captured_at")), 0.0)
    queries_rate = 0.0
    transactions_rate = 0.0
    container_iops = None
    if previous and elapsed > 0:
        queries_rate = max(_safe_int(current.get("queries_total")) - _safe_int(previous.get("queries_total")), 0) / elapsed
        transactions_rate = max(
            _safe_int(current.get("transactions_total")) - _safe_int(previous.get("transactions_total")),
            0,
        ) / elapsed
        direct_iops = _safe_float(current.get("container_io_ops_rate"), None)
        if direct_iops is not None:
            container_iops = round(direct_iops, 2)
        else:
            current_io_ops = _safe_float(current.get("container_io_ops_total"), -1.0)
            previous_io_ops = _safe_float(previous.get("container_io_ops_total"), -1.0)
            if current_io_ops >= 0 and previous_io_ops >= 0:
                container_iops = round(max(current_io_ops - previous_io_ops, 0.0) / elapsed, 2)
    container_cpu_percent = (
        _round_percent(current.get("container_cpu_percent"))
        if current.get("container_cpu_percent") is not None
        else None
    )
    container_memory_usage = _safe_float(current.get("container_memory_usage_bytes"))
    return {
        "captured_at": _beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
        "cards": [
            {"label": "实时 QPS", "value": f"{round(queries_rate, 2)} /s" if previous else "采集中"},
            {"label": "实时 TPS", "value": f"{round(transactions_rate, 2)} /s" if previous else "采集中"},
            {"label": "容器 IOPS", "value": f"{container_iops} /s" if container_iops is not None else "-"},
            {"label": "容器 CPU", "value": f"{container_cpu_percent}%" if container_cpu_percent is not None else "-"},
            {"label": "容器内存", "value": _format_bytes(container_memory_usage) if container_memory_usage > 0 else "-"},
        ],
        "series": [
            {
                "key": "queries_rate",
                "label": "实时 QPS",
                "value": round(queries_rate, 2) if previous else None,
                "display": f"{round(queries_rate, 2)} /s" if previous else "采集中",
                "unit": "/s",
            },
            {
                "key": "transactions_rate",
                "label": "实时 TPS",
                "value": round(transactions_rate, 2) if previous else None,
                "display": f"{round(transactions_rate, 2)} /s" if previous else "采集中",
                "unit": "/s",
            },
            {
                "key": "container_iops",
                "label": "容器 IOPS",
                "value": container_iops,
                "display": f"{container_iops} /s" if container_iops is not None else "-",
                "unit": "/s",
            },
            {
                "key": "container_cpu_pct",
                "label": "容器 CPU",
                "value": container_cpu_percent,
                "display": f"{container_cpu_percent}%" if container_cpu_percent is not None else "-",
                "unit": "%",
            },
            {
                "key": "container_memory_mb",
                "label": "容器内存",
                "value": round(container_memory_usage / (1024 * 1024), 2) if container_memory_usage > 0 else None,
                "display": _format_bytes(container_memory_usage) if container_memory_usage > 0 else "-",
                "unit": "MB",
            },
        ],
    }


def _build_postgresql_instance_info(config: BenchmarkConfig) -> Dict[str, Any]:
    import psycopg2

    connection = psycopg2.connect(**_postgres_connect_kwargs(config))
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    version(),
                    current_setting('server_version'),
                    current_setting('max_connections'),
                    current_setting('shared_buffers'),
                    EXTRACT(EPOCH FROM now() - pg_postmaster_start_time())::bigint
                """
            )
            version_comment, version, max_connections, shared_buffers, uptime_seconds = cursor.fetchone()
        container_stats = _resolve_container_runtime_stats(config, config.host)
        cards = [
            {"label": "实例类型", "value": config.db_type},
            {"label": "实例地址", "value": f"{config.host}:{config.port}/{config.database}"},
            {"label": "Schema", "value": config.postgres_schema or "-"},
            {"label": "版本", "value": str(version or "-")},
            {"label": "版本说明", "value": str(version_comment or "-")},
            {"label": "当前数据库", "value": config.database},
            {"label": "最大连接数", "value": str(max_connections or "-")},
            {"label": "shared_buffers", "value": str(shared_buffers or "-")},
            {"label": "实例运行时长", "value": f"{_safe_int(uptime_seconds)} s"},
        ]
        if container_stats:
            cards.extend(
                [
                    {"label": "容器名称", "value": container_stats.get("container_name", "-")},
                    {"label": "容器内存上限", "value": _format_bytes(_safe_float(container_stats.get("memory_limit_bytes")))},
                ]
            )
        return {
            "captured_at": _beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
            "cards": cards,
        }
    finally:
        connection.close()


def _build_postgresql_runtime_sample(config: BenchmarkConfig) -> Dict[str, Any]:
    import psycopg2

    connection = psycopg2.connect(**_postgres_connect_kwargs(config))
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    numbackends,
                    xact_commit,
                    xact_rollback,
                    tup_returned,
                    tup_fetched,
                    tup_inserted,
                    tup_updated,
                    tup_deleted,
                    blks_read,
                    blks_hit
                FROM pg_stat_database
                WHERE datname = current_database()
                """
            )
            row = cursor.fetchone()
        if row is None:
            raise BenchmarkValidationError("未在 pg_stat_database 中找到当前数据库统计信息。")
        prometheus_stats = _prometheus_postgresql_container_metrics(config)
        container_stats = prometheus_stats or _resolve_container_runtime_stats(config, config.host)
        (
            numbackends,
            xact_commit,
            xact_rollback,
            tup_returned,
            tup_fetched,
            tup_inserted,
            tup_updated,
            tup_deleted,
            blks_read,
            blks_hit,
        ) = row
        return {
            "captured_at": monotonic(),
            "connections_current": _safe_int(numbackends),
            "transactions_total": _safe_int(xact_commit) + _safe_int(xact_rollback),
            "queries_total": sum(
                _safe_int(value)
                for value in (tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted)
            ),
            "blocks_read_total": _safe_int(blks_read),
            "blocks_hit_total": _safe_int(blks_hit),
            "metrics_source": (container_stats or {}).get("metrics_source", "docker" if container_stats else "database"),
            "prometheus_qps": (container_stats or {}).get("prometheus_qps"),
            "prometheus_tps": (container_stats or {}).get("prometheus_tps"),
            "container_name": (container_stats or {}).get("container_name", ""),
            "container_cpu_usage_cores": (container_stats or {}).get("container_cpu_usage_cores"),
            "container_cpu_limit_cores": (container_stats or {}).get("container_cpu_limit_cores"),
            "container_cpu_percent": (container_stats or {}).get("container_cpu_percent", (container_stats or {}).get("cpu_percent")),
            "container_memory_percent": (container_stats or {}).get("container_memory_percent"),
            "container_memory_db_usage_bytes": (container_stats or {}).get(
                "container_memory_db_usage_bytes", (container_stats or {}).get("memory_usage_bytes")
            ),
            "container_memory_file_cache_bytes": (container_stats or {}).get("container_memory_file_cache_bytes"),
            "container_memory_usage_bytes": (container_stats or {}).get("memory_usage_bytes"),
            "container_memory_limit_bytes": (container_stats or {}).get("container_memory_limit_bytes", (container_stats or {}).get("memory_limit_bytes")),
            "container_io_ops_total": (container_stats or {}).get("io_ops_total"),
            "container_io_ops_rate": (container_stats or {}).get("container_io_ops_rate", (container_stats or {}).get("io_ops_rate")),
        }
    finally:
        connection.close()


def _format_postgresql_runtime_metrics(current: Dict[str, Any], previous: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    elapsed = max(_safe_float(current.get("captured_at")) - _safe_float((previous or {}).get("captured_at")), 0.0)
    transactions_rate = 0.0
    queries_rate = 0.0
    cache_hit_pct = None
    if previous and elapsed > 0:
        transactions_rate = max(
            _safe_int(current.get("transactions_total")) - _safe_int(previous.get("transactions_total")),
            0,
        ) / elapsed
        queries_rate = max(
            _safe_int(current.get("queries_total")) - _safe_int(previous.get("queries_total")),
            0,
        ) / elapsed
    prometheus_tps = _safe_float(current.get("prometheus_tps"), None)
    prometheus_qps = _safe_float(current.get("prometheus_qps"), None)
    if prometheus_tps is not None:
        transactions_rate = prometheus_tps
    if prometheus_qps is not None:
        queries_rate = prometheus_qps
    blocks_total = _safe_float(current.get("blocks_read_total")) + _safe_float(current.get("blocks_hit_total"))
    if blocks_total > 0:
        cache_hit_pct = round(_safe_float(current.get("blocks_hit_total")) / blocks_total * 100, 2)

    cpu_percent = (
        _round_percent(current.get("container_cpu_percent"))
        if current.get("container_cpu_percent") is not None
        else None
    )
    memory_percent = _round_percent(current.get("container_memory_percent")) if current.get("container_memory_percent") is not None else None
    if memory_percent is None:
        memory_usage = _safe_float(current.get("container_memory_db_usage_bytes", current.get("container_memory_usage_bytes")))
        memory_limit = _safe_float(current.get("container_memory_limit_bytes"))
        if memory_usage > 0 and memory_limit > 0:
            memory_percent = round(min(memory_usage / memory_limit * 100, 100), 2)

    container_iops = _safe_float(current.get("container_io_ops_rate"), None)
    transactions_ready = previous is not None or prometheus_tps is not None
    queries_ready = previous is not None or prometheus_qps is not None

    return {
        "captured_at": _beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
        "cards": [
            {"label": "当前连接数", "value": str(current.get("connections_current", 0))},
            {"label": "实时 TPS", "value": f"{round(transactions_rate, 2)} /s" if transactions_ready else "采集中"},
            {"label": "实时 QPS", "value": f"{round(queries_rate, 2)} /s" if queries_ready else "采集中"},
            {"label": "IOPS", "value": f"{round(container_iops, 2)} io/s" if container_iops is not None else "-"},
            {"label": "缓存命中率", "value": f"{cache_hit_pct}%" if cache_hit_pct is not None else "-"},
            {"label": "CPU 使用率", "value": f"{cpu_percent}%" if cpu_percent is not None else "-"},
            {"label": "内存使用率", "value": f"{memory_percent}%" if memory_percent is not None else "-"},
        ],
        "series": [
            {
                "key": "transactions_rate",
                "label": "实时 TPS",
                "chart": "tps",
                "chart_label": "TPS",
                "color": "#27ae60",
                "value": round(transactions_rate, 2) if transactions_ready else None,
                "display": f"{round(transactions_rate, 2)} /s" if transactions_ready else "采集中",
                "unit": "/s",
                "format": "ops",
            },
            {
                "key": "queries_rate",
                "label": "实时 QPS",
                "chart": "qps",
                "chart_label": "QPS",
                "color": "#2f80ed",
                "value": round(queries_rate, 2) if queries_ready else None,
                "display": f"{round(queries_rate, 2)} /s" if queries_ready else "采集中",
                "unit": "/s",
                "format": "ops",
            },
            {
                "key": "container_iops",
                "label": "IOPS",
                "chart": "iops",
                "chart_label": "IOPS",
                "color": "#f2994a",
                "value": round(container_iops, 2) if container_iops is not None else None,
                "display": f"{round(container_iops, 2)} io/s" if container_iops is not None else "-",
                "unit": "io/s",
                "format": "iops",
            },
            {
                "key": "container_cpu_pct",
                "label": "CPU 使用率",
                "chart": "cpu_usage",
                "chart_label": "CPU 使用率",
                "color": "#2f80ed",
                "value": cpu_percent,
                "display": f"{cpu_percent}%" if cpu_percent is not None else "-",
                "unit": "%",
                "format": "percent",
            },
            {
                "key": "container_memory_pct",
                "label": "内存使用率",
                "chart": "memory_usage",
                "chart_label": "内存使用率",
                "color": "#27ae60",
                "value": memory_percent,
                "display": f"{memory_percent}%" if memory_percent is not None else "-",
                "unit": "%",
                "format": "percent",
            },
        ],
    }


def _build_mongodb_instance_info(config: BenchmarkConfig) -> Dict[str, Any]:
    client = _create_mongo_client(config)
    try:
        build_info = client.admin.command("buildInfo")
        try:
            host_info = client.admin.command("hostInfo")
        except Exception:
            host_info = {}
        system = host_info.get("system", {}) if isinstance(host_info, dict) else {}
        container_stats = _resolve_container_runtime_stats(
            config,
            system.get("hostname"),
            build_info.get("hostname"),
            _mongo_connection_hosts(config),
            config.host,
        )
        cards = [
            {"label": "实例类型", "value": f"MongoDB ({config.mongo_topology})"},
            {"label": "节点列表", "value": _mongo_connection_hosts(config)},
            {"label": "版本", "value": str(build_info.get("version", "-"))},
            {"label": "认证库", "value": config.auth_database or config.database},
            {"label": "读偏好", "value": config.mongo_read_preference},
            {"label": "副本集", "value": config.mongo_replica_set or "-"},
            {"label": "分片键字段", "value": config.mongo_shard_key or "_id"},
            {"label": "CPU 核数", "value": str(system.get("numCores", "-"))},
            {"label": "物理内存", "value": _format_mib(_safe_float(system.get("memSizeMB", 0)))},
        ]
        if container_stats:
            cards.extend(
                [
                    {"label": "容器名称", "value": container_stats.get("container_name", "-")},
                    {"label": "容器内存上限", "value": _format_bytes(_safe_float(container_stats.get("memory_limit_bytes")))},
                ]
            )
        return {
            "captured_at": _beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
            "cards": cards,
        }
    finally:
        client.close()


def _build_mongodb_runtime_sample(config: BenchmarkConfig) -> Dict[str, Any]:
    client = _create_mongo_client(config)
    try:
        server_status = client.admin.command("serverStatus")
        connections = server_status.get("connections", {}) if isinstance(server_status, dict) else {}
        opcounters = server_status.get("opcounters", {}) if isinstance(server_status, dict) else {}
        network = server_status.get("network", {}) if isinstance(server_status, dict) else {}
        memory = server_status.get("mem", {}) if isinstance(server_status, dict) else {}
        wired_tiger = server_status.get("wiredTiger", {}) if isinstance(server_status, dict) else {}
        cache = wired_tiger.get("cache", {}) if isinstance(wired_tiger, dict) else {}
        prometheus_stats = _prometheus_database_runtime_sample(config)
        container_stats = prometheus_stats or _resolve_container_runtime_stats(
            config,
            server_status.get("host"),
            _mongo_connection_hosts(config),
            config.host,
        )
        ops_total = sum(_safe_int(opcounters.get(key)) for key in ("query", "insert", "update", "delete", "command"))
        return {
            "captured_at": monotonic(),
            "connections_current": _safe_int(connections.get("current")),
            "connections_available": _safe_int(connections.get("available")),
            "ops_total": ops_total,
            "bytes_in_total": _safe_int(network.get("bytesIn")),
            "bytes_out_total": _safe_int(network.get("bytesOut")),
            "resident_mb": _safe_float(memory.get("resident")),
            "virtual_mb": _safe_float(memory.get("virtual")),
            "cache_current_bytes": _safe_float(cache.get("bytes currently in the cache")),
            "cache_max_bytes": _safe_float(cache.get("maximum bytes configured")),
            "metrics_source": (container_stats or {}).get("metrics_source", "docker" if container_stats else "database"),
            "prometheus_qps": (container_stats or {}).get("prometheus_qps"),
            "prometheus_tps": (container_stats or {}).get("prometheus_tps"),
            "container_name": (container_stats or {}).get("container_name", ""),
            "container_cpu_percent": (container_stats or {}).get("container_cpu_percent", (container_stats or {}).get("cpu_percent")),
            "container_memory_percent": (container_stats or {}).get("container_memory_percent"),
            "container_memory_usage_bytes": (container_stats or {}).get("container_memory_usage_bytes", (container_stats or {}).get("memory_usage_bytes")),
            "container_memory_limit_bytes": (container_stats or {}).get("container_memory_limit_bytes", (container_stats or {}).get("memory_limit_bytes")),
            "container_io_ops_rate": (container_stats or {}).get("container_io_ops_rate", (container_stats or {}).get("io_ops_rate")),
        }
    finally:
        client.close()


def _format_mongodb_runtime_metrics(current: Dict[str, Any], previous: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    elapsed = max(_safe_float(current.get("captured_at")) - _safe_float((previous or {}).get("captured_at")), 0.0)
    ops_rate = 0.0
    bytes_in_rate = 0.0
    bytes_out_rate = 0.0
    if previous and elapsed > 0:
        ops_rate = max(_safe_int(current.get("ops_total")) - _safe_int(previous.get("ops_total")), 0) / elapsed
        bytes_in_rate = max(_safe_int(current.get("bytes_in_total")) - _safe_int(previous.get("bytes_in_total")), 0) / elapsed
        bytes_out_rate = max(_safe_int(current.get("bytes_out_total")) - _safe_int(previous.get("bytes_out_total")), 0) / elapsed
    prometheus_qps = _safe_float(current.get("prometheus_qps"), None)
    if prometheus_qps is not None:
        ops_rate = prometheus_qps
    bytes_in_mb = bytes_in_rate / (1024 * 1024)
    bytes_out_mb = bytes_out_rate / (1024 * 1024)
    container_cpu_percent = (
        _round_percent(current.get("container_cpu_percent"))
        if current.get("container_cpu_percent") is not None
        else None
    )
    container_memory_usage = _safe_float(current.get("container_memory_usage_bytes"))
    container_memory_percent = _round_percent(current.get("container_memory_percent")) if current.get("container_memory_percent") is not None else None
    if container_memory_percent is None:
        container_memory_limit = _safe_float(current.get("container_memory_limit_bytes"))
        if container_memory_usage > 0 and container_memory_limit > 0:
            container_memory_percent = _round_percent(container_memory_usage / container_memory_limit * 100)
    container_iops = _safe_float(current.get("container_io_ops_rate"), None)
    cache_pct = round(
        (_safe_float(current.get("cache_current_bytes")) / _safe_float(current.get("cache_max_bytes")) * 100),
        2,
    ) if _safe_float(current.get("cache_max_bytes")) > 0 else None
    return {
        "captured_at": _beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
        "cards": [
            {"label": "当前连接数", "value": str(current.get("connections_current", 0))},
            {"label": "可用连接余量", "value": str(current.get("connections_available", 0))},
            {"label": "实时 OPS", "value": f"{round(ops_rate, 2)} /s" if previous else "采集中"},
            {"label": "入流量", "value": f"{_format_bytes(bytes_in_rate)}/s" if previous else "采集中"},
            {"label": "出流量", "value": f"{_format_bytes(bytes_out_rate)}/s" if previous else "采集中"},
            {"label": "容器 CPU", "value": f"{container_cpu_percent}%" if container_cpu_percent is not None else "-"},
            {"label": "内存使用率", "value": f"{container_memory_percent}%" if container_memory_percent is not None else "-"},
            {"label": "容器 IOPS", "value": f"{round(container_iops, 2)} io/s" if container_iops is not None else "-"},
            {"label": "容器内存", "value": _format_bytes(container_memory_usage) if container_memory_usage > 0 else "-"},
            {"label": "Resident Memory", "value": _format_mib(_safe_float(current.get("resident_mb", 0)))},
            {
                "label": "WT Cache 使用率",
                "value": _format_pct(_safe_float(current.get("cache_current_bytes")), _safe_float(current.get("cache_max_bytes"))),
            },
        ],
        "series": [
            {
                "key": "connections_current",
                "label": "当前连接数",
                "value": float(_safe_int(current.get("connections_current"))),
                "display": str(current.get("connections_current", 0)),
                "unit": "",
            },
            {
                "key": "connections_available",
                "label": "可用连接余量",
                "value": float(_safe_int(current.get("connections_available"))),
                "display": str(current.get("connections_available", 0)),
                "unit": "",
            },
            {
                "key": "ops_rate",
                "label": "实时 OPS",
                "value": round(ops_rate, 2) if previous else None,
                "display": f"{round(ops_rate, 2)} /s" if previous else "采集中",
                "unit": "/s",
            },
            {
                "key": "bytes_in_mb",
                "label": "入流量",
                "value": round(bytes_in_mb, 4) if previous else None,
                "display": f"{_format_bytes(bytes_in_rate)}/s" if previous else "采集中",
                "unit": "MB/s",
            },
            {
                "key": "bytes_out_mb",
                "label": "出流量",
                "value": round(bytes_out_mb, 4) if previous else None,
                "display": f"{_format_bytes(bytes_out_rate)}/s" if previous else "采集中",
                "unit": "MB/s",
            },
            {
                "key": "container_cpu_pct",
                "label": "容器 CPU",
                "value": container_cpu_percent,
                "display": f"{container_cpu_percent}%" if container_cpu_percent is not None else "-",
                "unit": "%",
            },
            {
                "key": "container_memory_mb",
                "label": "容器内存",
                "value": round(container_memory_usage / (1024 * 1024), 2) if container_memory_usage > 0 else None,
                "display": _format_bytes(container_memory_usage) if container_memory_usage > 0 else "-",
                "unit": "MB",
            },
            {
                "key": "container_memory_pct",
                "label": "内存使用率",
                "chart": "memory_usage",
                "chart_label": "内存使用率",
                "value": container_memory_percent,
                "display": f"{container_memory_percent}%" if container_memory_percent is not None else "-",
                "unit": "%",
                "format": "percent",
            },
            {
                "key": "container_iops",
                "label": "容器 IOPS",
                "chart": "iops",
                "chart_label": "IOPS",
                "value": round(container_iops, 2) if container_iops is not None else None,
                "display": f"{round(container_iops, 2)} io/s" if container_iops is not None else "-",
                "unit": "io/s",
                "format": "iops",
            },
            {
                "key": "resident_mb",
                "label": "Resident Memory",
                "value": round(_safe_float(current.get("resident_mb")), 2),
                "display": _format_mib(_safe_float(current.get("resident_mb", 0))),
                "unit": "MB",
            },
            {
                "key": "cache_pct",
                "label": "WT Cache 使用率",
                "value": cache_pct,
                "display": _format_pct(_safe_float(current.get("cache_current_bytes")), _safe_float(current.get("cache_max_bytes"))),
                "unit": "%",
            },
        ],
    }


def _build_prometheus_resource_runtime_sample(config: BenchmarkConfig) -> Optional[Dict[str, Any]]:
    return _prometheus_database_runtime_sample(config)


def _format_prometheus_resource_runtime_metrics(
    config: BenchmarkConfig,
    current: Dict[str, Any],
    previous: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    cpu_percent = (
        _round_percent(current.get("container_cpu_percent"))
        if current.get("container_cpu_percent") is not None
        else None
    )
    memory_percent = _round_percent(current.get("container_memory_percent")) if current.get("container_memory_percent") is not None else None
    if memory_percent is None:
        memory_usage = _safe_float(current.get("container_memory_usage_bytes"))
        memory_limit = _safe_float(current.get("container_memory_limit_bytes"))
        if memory_usage > 0 and memory_limit > 0:
            memory_percent = round(min(memory_usage / memory_limit * 100, 100), 2)
    iops = _safe_float(current.get("container_io_ops_rate"), None)
    qps = _safe_float(current.get("prometheus_qps"), None)
    tps = _safe_float(current.get("prometheus_tps"), None)
    scope = str(current.get("prometheus_container") or current.get("container_name") or config.prometheus_instance or "-")
    cards = [{"label": "监控对象", "value": scope}]
    if qps is not None:
        cards.append({"label": "实时 QPS", "value": f"{round(qps, 2)} /s"})
    if tps is not None:
        cards.append({"label": "实时 TPS", "value": f"{round(tps, 2)} /s"})

    extra_specs = [
        ("clickhouse_select_qps", "CH Select", "/s"),
        ("clickhouse_insert_qps", "CH Insert", "/s"),
        ("clickhouse_failed_qps", "CH 失败查询", "/s"),
        ("clickhouse_active_queries", "CH 活跃查询", ""),
        ("clickhouse_selected_rows_rate", "CH 读取行", "行/s"),
        ("clickhouse_inserted_rows_rate", "CH 写入行", "行/s"),
        ("rabbitmq_messages_ready", "RabbitMQ Ready", "条"),
        ("rabbitmq_messages_unacked", "RabbitMQ Unacked", "条"),
        ("rabbitmq_connections", "RabbitMQ 连接", "个"),
        ("rabbitmq_channels", "RabbitMQ Channel", "个"),
        ("rabbitmq_consumers", "RabbitMQ Consumer", "个"),
        ("rocketmq_put_latency_p99", "RocketMQ P99", "ms"),
        ("rocketmq_disk_percent", "RocketMQ 磁盘", "%"),
    ]
    for key, label, unit in extra_specs:
        value = _safe_float(current.get(key), None)
        if value is None:
            continue
        display = f"{round(value, 2)}{(' ' + unit) if unit and unit != '%' else unit}"
        cards.append({"label": label, "value": display})

    cards.extend(
        [
            {"label": "CPU 使用率", "value": f"{cpu_percent}%" if cpu_percent is not None else "-"},
            {"label": "内存使用率", "value": f"{memory_percent}%" if memory_percent is not None else "-"},
            {"label": "IOPS", "value": f"{round(iops, 2)} io/s" if iops is not None else "-"},
        ]
    )
    series = []
    if qps is not None:
        series.append(
            {
                "key": "queries_rate",
                "label": "实时 QPS",
                "chart": "qps",
                "chart_label": "QPS",
                "color": "#2f80ed",
                "value": round(qps, 2),
                "display": f"{round(qps, 2)} /s",
                "unit": "/s",
                "format": "ops",
            }
        )
    if tps is not None:
        series.append(
            {
                "key": "transactions_rate",
                "label": "实时 TPS",
                "chart": "tps",
                "chart_label": "TPS",
                "color": "#27ae60",
                "value": round(tps, 2),
                "display": f"{round(tps, 2)} /s",
                "unit": "/s",
                "format": "ops",
            }
        )
    series.extend(
        [
            {
                "key": "container_cpu_pct",
                "label": "CPU 使用率",
                "chart": "cpu_usage",
                "chart_label": "CPU 使用率",
                "color": "#65b84a",
                "value": cpu_percent,
                "display": f"{cpu_percent}%" if cpu_percent is not None else "-",
                "unit": "%",
                "format": "percent",
            },
            {
                "key": "container_memory_pct",
                "label": "内存使用率",
                "chart": "memory_usage",
                "chart_label": "内存使用率",
                "color": "#5b8def",
                "value": memory_percent,
                "display": f"{memory_percent}%" if memory_percent is not None else "-",
                "unit": "%",
                "format": "percent",
            },
            {
                "key": "container_iops",
                "label": "IOPS",
                "chart": "iops",
                "chart_label": "IOPS",
                "color": "#9b51e0",
                "value": round(iops, 2) if iops is not None else None,
                "display": f"{round(iops, 2)} io/s" if iops is not None else "-",
                "unit": "io/s",
                "format": "iops",
            },
        ]
    )
    return {
        "captured_at": _beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
        "cards": cards,
        "series": series,
    }



def _clickhouse_scheme(config: BenchmarkConfig) -> str:
    return "https" if config.ssl_enabled else "http"


def _clickhouse_database(config: BenchmarkConfig) -> str:
    return str(config.database or "default").strip() or "default"


def _clickhouse_user(config: BenchmarkConfig) -> str:
    return str(config.user or "default").strip() or "default"


def _clickhouse_task_suffix(config: BenchmarkConfig) -> str:
    raw = re.sub(r"[^A-Za-z0-9_]", "_", str(config.task_id or "").strip())
    return raw[:8]


def _clickhouse_table_prefix(config: BenchmarkConfig) -> str:
    raw = str(config.collection_name or "").strip()
    if not raw or raw == "test":
        suffix = _clickhouse_task_suffix(config)
        return f"{CLICKHOUSE_DEFAULT_TABLE}_{suffix}" if suffix else CLICKHOUSE_DEFAULT_TABLE
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", raw):
        raise BenchmarkValidationError("ClickHouse 表名前缀只能包含字母、数字和下划线，且不能以数字开头。")
    return raw


def _clickhouse_table_names(config: BenchmarkConfig) -> List[str]:
    prefix = _clickhouse_table_prefix(config)
    table_count = max(1, int(config.table_count or 1))
    if table_count == 1:
        return [prefix]
    return [f"{prefix}_{index:03d}" for index in range(1, table_count + 1)]


def _clickhouse_table(config: BenchmarkConfig) -> str:
    return _clickhouse_table_names(config)[0]


def _clickhouse_table_display(config: BenchmarkConfig) -> str:
    table_names = _clickhouse_table_names(config)
    if len(table_names) == 1:
        return table_names[0]
    return f"{_clickhouse_table_prefix(config)}_*（{len(table_names)} 张）"


def _clickhouse_quote_identifier(identifier: str) -> str:
    return "`" + str(identifier).replace("`", "``") + "`"


def _clickhouse_table_ref(config: BenchmarkConfig, table_name: Optional[str] = None) -> str:
    return f"{_clickhouse_quote_identifier(_clickhouse_database(config))}.{_clickhouse_quote_identifier(table_name or _clickhouse_table(config))}"


def _clickhouse_string_literal(value: str) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _clickhouse_cluster_candidates(config: BenchmarkConfig) -> List[str]:
    try:
        output = _clickhouse_request(
            config,
            "SELECT cluster, shard_num, replica_num, host_name FROM system.clusters ORDER BY cluster, shard_num, replica_num FORMAT TabSeparated",
            timeout=15,
            database="default",
        )
    except Exception:
        return []

    clusters: Dict[str, Dict[str, Any]] = {}
    for line in output.splitlines():
        parts = line.split("\t")
        if len(parts) < 4:
            continue
        cluster, shard, _replica, host_name = parts[0], parts[1], parts[2], parts[3]
        if not cluster or cluster == "default" or host_name in {"localhost", "127.0.0.1"}:
            continue
        entry = clusters.setdefault(cluster, {"shards": set(), "nodes": 0})
        entry["shards"].add(shard)
        entry["nodes"] += 1

    if not clusters:
        return []

    preferred: List[str] = []
    if "all-sharded" in clusters:
        preferred.append("all-sharded")
    current_primary = str(config.current_primary or "").strip()
    if "-replica" in current_primary:
        derived = current_primary.split("-replica", 1)[0]
        if derived in clusters:
            preferred.append(derived)
    preferred.extend(
        sorted(
            clusters,
            key=lambda name: (
                0 if len(clusters[name]["shards"]) > 1 else 1,
                -len(clusters[name]["shards"]),
                -clusters[name]["nodes"],
                name,
            ),
        )
    )

    result: List[str] = []
    for name in preferred:
        if name in clusters and name not in result:
            result.append(name)
    return result


def _clickhouse_cluster_name(config: BenchmarkConfig) -> str:
    candidates = _clickhouse_cluster_candidates(config)
    return candidates[0] if candidates else ""


def _clickhouse_on_cluster(cluster_name: str) -> str:
    return f" ON CLUSTER {_clickhouse_string_literal(cluster_name)}" if cluster_name else ""


def _clickhouse_local_table_name(table_name: str) -> str:
    return f"{table_name}_local"


def _clickhouse_base_url(config: BenchmarkConfig) -> str:
    return f"{_clickhouse_scheme(config)}://{config.host}:{config.port}/"


def _clickhouse_ssl_context(config: BenchmarkConfig):
    if not config.ssl_enabled:
        return None
    if config.ssl_verify:
        if config.ssl_ca_file:
            return ssl.create_default_context(cafile=config.ssl_ca_file)
        return ssl.create_default_context()
    return ssl._create_unverified_context()


def _clickhouse_request(
    config: BenchmarkConfig,
    sql: str,
    timeout: int = 60,
    database: Optional[str] = None,
) -> str:
    params = {"database": database if database is not None else _clickhouse_database(config)}
    url = f"{_clickhouse_base_url(config)}?{urlencode(params)}"
    request = urllib.request.Request(
        url,
        data=sql.encode("utf-8"),
        headers={"Content-Type": "text/plain; charset=utf-8"},
        method="POST",
    )
    user = _clickhouse_user(config)
    if user or config.password:
        import base64

        token = base64.b64encode(f"{user}:{config.password}".encode("utf-8")).decode("ascii")
        request.add_header("Authorization", f"Basic {token}")
    try:
        with urllib.request.urlopen(request, timeout=timeout, context=_clickhouse_ssl_context(config)) as response:
            return response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:800]
        raise BenchmarkValidationError(f"ClickHouse HTTP {exc.code}: {detail}") from exc
    except Exception as exc:
        raise BenchmarkValidationError(f"ClickHouse 连接失败: {exc}") from exc


def _clickhouse_wait_for_table(
    config: BenchmarkConfig,
    table_name: str,
    timeout_seconds: int = 60,
) -> None:
    database = _clickhouse_database(config)
    deadline = monotonic() + max(1, timeout_seconds)
    last_error = ""
    while monotonic() < deadline:
        try:
            output = _clickhouse_request(
                config,
                f"EXISTS TABLE {_clickhouse_table_ref(config, table_name)} FORMAT TabSeparated",
                timeout=10,
                database="default",
            )
            if output.strip() == "1":
                return
            last_error = f"{database}.{table_name} 尚未可见"
        except Exception as exc:
            last_error = str(exc)
        sleep(1)
    raise BenchmarkValidationError(f"ClickHouse 表 {database}.{table_name} 创建后仍不可见: {last_error}")


def _clickhouse_table_rows(config: BenchmarkConfig, table_name: str) -> int:
    output = _clickhouse_request(
        config,
        f"SELECT count() FROM {_clickhouse_table_ref(config, table_name)} FORMAT TabSeparated",
        timeout=60,
        database="default",
    )
    try:
        return int(float(output.strip() or "0"))
    except ValueError:
        return 0


def _check_clickhouse_connectivity(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    _emit_log(log_callback, "开始检查 ClickHouse 连接性...")
    database = _clickhouse_database(config)
    _emit_log(
        log_callback,
        "连接检测启动: "
        f"{_clickhouse_scheme(config)}://{config.host}:{config.port}/{database} "
        f"user={_clickhouse_user(config)} password={'******' if config.password else ''}",
    )
    output = _clickhouse_request(
        config,
        f"SELECT version(), currentDatabase(), now(), countIf(name = {_clickhouse_string_literal(database)}) FROM system.databases FORMAT TabSeparated",
        timeout=15,
        database="default",
    )
    values = output.strip().split("\t")
    version = values[0] if values else "-"
    current_database = values[1] if len(values) > 1 else "default"
    checked_at = values[2] if len(values) > 2 else "-"
    target_exists = str(values[3]).strip() == "1" if len(values) > 3 else False
    if not target_exists and not config.auto_prepare:
        raise BenchmarkValidationError(f"ClickHouse 数据库 {database} 不存在，请启用自动准备或先创建数据库。")
    _emit_log(
        log_callback,
        f"ClickHouse 连接检测通过: version={version}, current_database={current_database}, "
        f"target_database={database}, target_exists={'yes' if target_exists else 'no'}, checked_at={checked_at}",
    )


def _build_clickhouse_instance_info(config: BenchmarkConfig) -> Optional[Dict[str, Any]]:
    cards = [
        {"label": "实例类型", "value": "ClickHouse"},
        {"label": "实例地址", "value": f"{config.host}:{config.port}/{_clickhouse_database(config)}"},
        {"label": "压测工具", "value": "QDBmark ClickHouse HTTP Benchmark"},
    ]
    try:
        output = _clickhouse_request(
            config,
            "SELECT version(), currentDatabase(), hostName(), uptime() FORMAT TabSeparated",
            timeout=10,
        )
        values = output.strip().split("\t")
        if values:
            cards.extend(
                [
                    {"label": "版本", "value": values[0] or "-"},
                    {"label": "当前库", "value": values[1] if len(values) > 1 else _clickhouse_database(config)},
                    {"label": "节点名", "value": values[2] if len(values) > 2 else "-"},
                    {"label": "运行时长(秒)", "value": values[3] if len(values) > 3 else "-"},
                ]
            )
    except BenchmarkValidationError:
        pass
    return {"captured_at": _beijing_now().strftime("%Y-%m-%d %H:%M:%S"), "cards": cards}


def _clickhouse_prepare_dataset(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> None:
    _check_cancel(cancel_callback)
    database = _clickhouse_database(config)
    table_names = _clickhouse_table_names(config)
    rows = max(1, int(config.table_size or 0))
    cluster_name = _clickhouse_cluster_name(config)
    cluster_clause = _clickhouse_on_cluster(cluster_name)
    cluster_label = cluster_name or "single-node"
    _emit_log(
        log_callback,
        f"ClickHouse 造数开始: database={database}, table_prefix={_clickhouse_table_prefix(config)}, "
        f"tables={len(table_names)}, rows_per_table={rows}, cluster={cluster_label}",
    )
    _clickhouse_request(
        config,
        f"CREATE DATABASE IF NOT EXISTS {_clickhouse_quote_identifier(database)}{cluster_clause}",
        timeout=120,
        database="default",
    )
    for index, table_name in enumerate(table_names, start=1):
        _check_cancel(cancel_callback)
        table = _clickhouse_table_ref(config, table_name)
        _emit_log(log_callback, f"ClickHouse 准备表 {index}/{len(table_names)}: {table_name}")
        if cluster_name:
            local_table_name = _clickhouse_local_table_name(table_name)
            local_table = _clickhouse_table_ref(config, local_table_name)
            _clickhouse_request(
                config,
                f"""
                CREATE TABLE IF NOT EXISTS {local_table}{cluster_clause}
                (
                    event_date Date,
                    user_id UInt64,
                    category UInt32,
                    value Float64,
                    payload String
                )
                ENGINE = MergeTree
                PARTITION BY toYYYYMM(event_date)
                ORDER BY (event_date, category, user_id)
                """,
                timeout=180,
                database="default",
            )
            _clickhouse_wait_for_table(config, local_table_name, timeout_seconds=120)
            _clickhouse_request(
                config,
                f"""
                CREATE TABLE IF NOT EXISTS {table}{cluster_clause}
                AS {local_table}
                ENGINE = Distributed({_clickhouse_string_literal(cluster_name)}, {_clickhouse_string_literal(database)}, {_clickhouse_string_literal(local_table_name)}, rand())
                """,
                timeout=180,
                database="default",
            )
            _clickhouse_wait_for_table(config, table_name, timeout_seconds=120)
            _clickhouse_request(config, f"TRUNCATE TABLE {local_table}{cluster_clause}", timeout=180, database="default")
        else:
            _clickhouse_request(
                config,
                f"""
                CREATE TABLE IF NOT EXISTS {table}
                (
                    event_date Date,
                    user_id UInt64,
                    category UInt32,
                    value Float64,
                    payload String
                )
                ENGINE = MergeTree
                PARTITION BY toYYYYMM(event_date)
                ORDER BY (event_date, category, user_id)
                """,
                timeout=120,
                database="default",
            )
            _clickhouse_wait_for_table(config, table_name, timeout_seconds=60)
            _clickhouse_request(config, f"TRUNCATE TABLE {table}", timeout=120, database="default")
        _check_cancel(cancel_callback)
        _clickhouse_request(
            config,
            f"""
            INSERT INTO {table}
            SELECT
                today() - toIntervalDay(number % 90),
                toUInt64(number % 1000000),
                toUInt32(number % 100),
                toFloat64(cityHash64(number) % 100000) / 100,
                concat('qdbmark-', toString(number % 10000))
            FROM numbers({rows})
            """,
            timeout=max(300, rows // 2000),
            database="default",
        )
        loaded_rows = _clickhouse_table_rows(config, table_name)
        if loaded_rows <= 0:
            raise BenchmarkValidationError(f"ClickHouse 表 {database}.{table_name} 造数后行数为 0，请检查集群写入与用户权限。")
    _emit_log(log_callback, f"ClickHouse 造数完成: {len(table_names)} 张表，每表 {rows} 行。")


def _clickhouse_workload_sql_for_table(table: str, workload: str) -> str:
    workload = str(workload or "count").strip()
    if workload == "count":
        return f"SELECT count() FROM {table}"
    if workload == "filter":
        return f"SELECT count(), avg(value) FROM {table} WHERE event_date >= today() - 30 AND category IN (3, 7, 11)"
    if workload == "aggregation":
        return f"SELECT category, count(), avg(value), quantile(0.95)(value) FROM {table} GROUP BY category ORDER BY category"
    if workload == "group_by":
        return f"SELECT event_date, category, count(), sum(value) FROM {table} GROUP BY event_date, category ORDER BY event_date, category LIMIT 100"
    if workload == "top_n":
        return f"SELECT user_id, sum(value) AS score FROM {table} GROUP BY user_id ORDER BY score DESC LIMIT 100"
    raise BenchmarkValidationError(f"未知 ClickHouse 查询模型: {workload}")


def _clickhouse_workload_sql(config: BenchmarkConfig) -> List[str]:
    workload = str(config.workload or "count").strip()
    return [_clickhouse_workload_sql_for_table(_clickhouse_table_ref(config, table_name), workload) for table_name in _clickhouse_table_names(config)]


def _percentile(values: List[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(round((len(ordered) - 1) * percentile))))
    return round(ordered[index], 2)


def _run_clickhouse_query_loop(
    config: BenchmarkConfig,
    queries: List[str],
    concurrency: int,
    duration_seconds: int,
    log_callback: Optional[LogCallback],
    cancel_callback: Optional[CancelCallback],
    record_result: bool = True,
) -> Dict[str, Any]:
    started_at = monotonic()
    stop_at = started_at + max(1, duration_seconds)
    lock = Lock()
    latencies: List[float] = []
    success_count = 0
    failed_count = 0
    sample_errors: List[str] = []

    if not queries:
        raise BenchmarkValidationError("ClickHouse 查询模板不能为空。")

    def worker(worker_index: int) -> None:
        nonlocal success_count, failed_count
        query_index = worker_index
        while monotonic() < stop_at:
            if cancel_callback and cancel_callback():
                break
            query_started = monotonic()
            query = queries[query_index % len(queries)]
            query_index += concurrency
            try:
                _clickhouse_request(config, query, timeout=max(30, duration_seconds + 10))
                latency_ms = (monotonic() - query_started) * 1000.0
                if record_result:
                    with lock:
                        success_count += 1
                        latencies.append(latency_ms)
            except Exception as exc:
                if record_result:
                    with lock:
                        failed_count += 1
                        if len(sample_errors) < 5:
                            sample_errors.append(str(exc))

    threads = [Thread(target=worker, args=(index,), daemon=True) for index in range(max(1, concurrency))]
    for thread in threads:
        thread.start()
    while any(thread.is_alive() for thread in threads):
        _check_cancel(cancel_callback)
        for thread in threads:
            thread.join(timeout=0.2)

    elapsed_seconds = max(0.001, monotonic() - started_at)
    total = success_count + failed_count
    success_rate = round((success_count / total) * 100.0, 2) if total else 0.0
    return {
        "elapsed_seconds": round(elapsed_seconds, 2),
        "success_count": success_count,
        "failed_count": failed_count,
        "qps": round(success_count / elapsed_seconds, 2),
        "avg_latency_ms": round(sum(latencies) / len(latencies), 2) if latencies else 0.0,
        "p95_latency_ms": _percentile(latencies, 0.95),
        "min_latency_ms": round(min(latencies), 2) if latencies else 0.0,
        "max_latency_ms": round(max(latencies), 2) if latencies else 0.0,
        "success_rate": success_rate,
        "sample_errors": sample_errors,
    }


def _run_clickhouse_cleanup(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> None:
    _check_cancel(cancel_callback)
    table_names = _clickhouse_table_names(config)
    cluster_name = _clickhouse_cluster_name(config)
    cluster_clause = _clickhouse_on_cluster(cluster_name)
    for table_name in table_names:
        _check_cancel(cancel_callback)
        table = _clickhouse_table_ref(config, table_name)
        _clickhouse_request(config, f"DROP TABLE IF EXISTS {table}{cluster_clause}", timeout=180, database="default")
        if cluster_name:
            local_table = _clickhouse_table_ref(config, _clickhouse_local_table_name(table_name))
            _clickhouse_request(config, f"DROP TABLE IF EXISTS {local_table}{cluster_clause}", timeout=180, database="default")
    _emit_log(log_callback, f"已删除 ClickHouse 测试表: {_clickhouse_table_display(config)}")


def _run_clickhouse_benchmark(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> BenchmarkSummary:
    _emit_log(log_callback, f"已接收测试任务: ClickHouse / {config.host}:{config.port}/{_clickhouse_database(config)}")
    _emit_log(log_callback, f"压测模型: {config.workload} | 表: {_clickhouse_table_display(config)} | 单表行数: {config.table_size}")
    _check_clickhouse_connectivity(config, log_callback=log_callback)
    if config.auto_prepare:
        _clickhouse_prepare_dataset(config, log_callback=log_callback, cancel_callback=cancel_callback)

    queries = _clickhouse_workload_sql(config)
    _emit_log(log_callback, f"ClickHouse 查询模板数量: {len(queries)}")
    if len(queries) == 1:
        _emit_log(log_callback, f"ClickHouse 查询模板: {queries[0]}")
    if config.warmup_seconds > 0:
        warmup_concurrency = max(sorted(set(config.threads_values)))
        _emit_log(log_callback, f"ClickHouse 预热开始: 并发={warmup_concurrency}, 时长={config.warmup_seconds}s。")
        _run_clickhouse_query_loop(
            config,
            queries,
            warmup_concurrency,
            config.warmup_seconds,
            log_callback=log_callback,
            cancel_callback=cancel_callback,
            record_result=False,
        )
        _emit_log(log_callback, "ClickHouse 预热完成。")

    results: List[ConcurrencyResult] = []
    try:
        for concurrency in sorted(set(config.threads_values)):
            _check_cancel(cancel_callback)
            _emit_log(log_callback, f"并发 {concurrency}: 开始执行 ClickHouse {config.workload} 查询压测...")
            stats = _run_clickhouse_query_loop(
                config,
                queries,
                concurrency,
                config.duration_seconds,
                log_callback=log_callback,
                cancel_callback=cancel_callback,
            )
            result = ConcurrencyResult(
                concurrency=concurrency,
                elapsed_seconds=stats["elapsed_seconds"],
                total_transactions=stats["success_count"],
                total_queries=stats["success_count"],
                total_events=stats["success_count"],
                read_queries=stats["success_count"],
                write_queries=0,
                other_queries=0,
                failed_requests=stats["failed_count"],
                qps=stats["qps"],
                tps=stats["qps"],
                avg_latency_ms=stats["avg_latency_ms"],
                p95_latency_ms=stats["p95_latency_ms"],
                min_latency_ms=stats["min_latency_ms"],
                max_latency_ms=stats["max_latency_ms"],
                success_rate=stats["success_rate"],
                baseline_label=_classify_baseline(stats["success_rate"], stats["p95_latency_ms"]),
                sample_errors=stats["sample_errors"],
                raw_output="",
            )
            results.append(result)
            _emit_log(
                log_callback,
                f"并发 {concurrency}: ClickHouse 测试完成，QPS={result.qps}，P95={result.p95_latency_ms}ms，成功率={result.success_rate}%",
            )
    finally:
        if config.auto_cleanup:
            _run_clickhouse_cleanup(config, log_callback=log_callback, cancel_callback=cancel_callback)

    return BenchmarkSummary(
        report_title=config.report_title,
        executed_at=_beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
        target={
            "db_type": "ClickHouse",
            "host": config.host,
            "port": config.port,
            "database": _clickhouse_database(config),
            "user": _clickhouse_user(config),
            "collection_name": _clickhouse_table_display(config),
        },
        parameters={
            "benchmark_engine": "qdbmark-clickhouse-http",
            "workload": config.workload,
            "duration_seconds": config.duration_seconds,
            "warmup_seconds": config.warmup_seconds,
            "report_interval": config.report_interval,
            "table_count": config.table_count,
            "table_size": config.table_size,
            "operation_count": 0,
            "auth_database": "",
            "collection_name": _clickhouse_table_display(config),
            "ssl_enabled": config.ssl_enabled,
            "ssl_verify": config.ssl_verify,
            "auto_prepare": config.auto_prepare,
            "auto_cleanup": config.auto_cleanup,
            "threads_values": sorted(set(config.threads_values)),
        },
        results=results,
        recommended_baseline=_select_recommended_baseline(results),
    )


def _find_esrally() -> str:
    candidates = [
        "/opt/esrally-venv/bin/esrally",
        shutil.which("esrally"),
        "/opt/conda/bin/esrally",
        "/usr/local/bin/esrally",
    ]
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return str(candidate)
    raise BenchmarkValidationError("未找到 esrally。请使用 Docker 重建镜像，或先安装官方 Elastic Rally。")


def _elasticsearch_target_hosts(config: BenchmarkConfig) -> str:
    raw_host = str(config.host or "").strip()
    if not raw_host:
        return ""
    hosts: list[str] = []
    for item in raw_host.split(","):
        item = item.strip()
        if not item:
            continue
        hosts.append(item if ":" in item else f"{item}:{config.port}")
    return ",".join(hosts)


def _elasticsearch_scheme(config: BenchmarkConfig) -> str:
    return "https" if config.ssl_enabled else "http"


def _elasticsearch_base_url(config: BenchmarkConfig) -> str:
    first_host = _elasticsearch_target_hosts(config).split(",", 1)[0]
    if not first_host:
        raise BenchmarkValidationError("ElasticSearch 地址不能为空。")
    return f"{_elasticsearch_scheme(config)}://{first_host}"


def _elasticsearch_ssl_context(config: BenchmarkConfig):
    if not config.ssl_enabled:
        return None
    if config.ssl_verify:
        if config.ssl_ca_file:
            return ssl.create_default_context(cafile=config.ssl_ca_file)
        return ssl.create_default_context()
    return ssl._create_unverified_context()


def _elasticsearch_request_json(config: BenchmarkConfig, path: str, timeout: int = 10) -> Dict[str, Any]:
    url = f"{_elasticsearch_base_url(config).rstrip('/')}/{path.lstrip('/')}"
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    if config.user:
        import base64
        token = base64.b64encode(f"{config.user}:{config.password}".encode("utf-8")).decode("ascii")
        request.add_header("Authorization", f"Basic {token}")
    try:
        with urllib.request.urlopen(request, timeout=timeout, context=_elasticsearch_ssl_context(config)) as response:
            body = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:400]
        raise BenchmarkValidationError(f"ElasticSearch HTTP {exc.code}: {detail}") from exc
    except Exception as exc:
        raise BenchmarkValidationError(f"ElasticSearch 连接失败: {exc}") from exc
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise BenchmarkValidationError(f"ElasticSearch 返回内容不是 JSON: {body[:300]}") from exc
    return payload if isinstance(payload, dict) else {}


def _check_elasticsearch_connectivity(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    target_hosts = _elasticsearch_target_hosts(config)
    _emit_log(log_callback, "开始检查 ElasticSearch 连接性...")
    _emit_log(log_callback, f"连接检测启动: {_elasticsearch_scheme(config)}://{target_hosts}")
    root = _elasticsearch_request_json(config, "/")
    health = _elasticsearch_request_json(config, "/_cluster/health")
    cluster_name = root.get("cluster_name") or health.get("cluster_name") or "-"
    version = root.get("version", {}).get("number") if isinstance(root.get("version"), dict) else "-"
    status = health.get("status", "-")
    _emit_log(log_callback, f"ElasticSearch 连接检测通过: cluster={cluster_name}, version={version}, status={status}")


def _build_elasticsearch_instance_info(config: BenchmarkConfig) -> Optional[Dict[str, Any]]:
    try:
        root = _elasticsearch_request_json(config, "/")
        health = _elasticsearch_request_json(config, "/_cluster/health")
    except BenchmarkValidationError:
        return {
            "captured_at": _beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
            "cards": [
                {"label": "实例类型", "value": "ElasticSearch"},
                {"label": "实例地址", "value": _elasticsearch_target_hosts(config)},
                {"label": "压测工具", "value": "Elastic Rally"},
            ],
        }
    version = root.get("version", {}).get("number") if isinstance(root.get("version"), dict) else "-"
    return {
        "captured_at": _beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
        "cards": [
            {"label": "实例类型", "value": "ElasticSearch"},
            {"label": "实例地址", "value": _elasticsearch_target_hosts(config)},
            {"label": "集群名称", "value": root.get("cluster_name") or health.get("cluster_name") or "-"},
            {"label": "版本", "value": version or "-"},
            {"label": "集群状态", "value": health.get("status", "-")},
            {"label": "节点数", "value": str(health.get("number_of_nodes", "-"))},
            {"label": "压测工具", "value": "Elastic Rally"},
        ],
    }


def _build_elasticsearch_runtime_sample(config: BenchmarkConfig) -> Optional[Dict[str, Any]]:
    try:
        stats = _elasticsearch_request_json(config, "/_nodes/stats/indices,jvm,process")
    except BenchmarkValidationError:
        return None
    nodes = stats.get("nodes", {}) if isinstance(stats.get("nodes"), dict) else {}
    indexing_total = 0.0
    search_total = 0.0
    heap_used = 0.0
    heap_max = 0.0
    cpu_values: list[float] = []
    for node in nodes.values():
        if not isinstance(node, dict):
            continue
        indices = node.get("indices", {}) if isinstance(node.get("indices"), dict) else {}
        indexing = indices.get("indexing", {}) if isinstance(indices.get("indexing"), dict) else {}
        search = indices.get("search", {}) if isinstance(indices.get("search"), dict) else {}
        query = search.get("query", {}) if isinstance(search.get("query"), dict) else {}
        indexing_total += _safe_float(indexing.get("index_total"))
        search_total += _safe_float(query.get("total"))
        jvm = node.get("jvm", {}) if isinstance(node.get("jvm"), dict) else {}
        mem = jvm.get("mem", {}) if isinstance(jvm.get("mem"), dict) else {}
        heap_used += _safe_float(mem.get("heap_used_in_bytes"))
        heap_max += _safe_float(mem.get("heap_max_in_bytes"))
        process = node.get("process", {}) if isinstance(node.get("process"), dict) else {}
        cpu = process.get("cpu", {}) if isinstance(process.get("cpu"), dict) else {}
        cpu_percent = _safe_float(cpu.get("percent"), -1.0)
        if cpu_percent >= 0:
            cpu_values.append(cpu_percent)
    return {
        "captured_at_ts": monotonic(),
        "indexing_total": indexing_total,
        "search_total": search_total,
        "heap_used_bytes": heap_used,
        "heap_max_bytes": heap_max,
        "cpu_percent": round(sum(cpu_values) / len(cpu_values), 2) if cpu_values else None,
    }


def _format_elasticsearch_runtime_metrics(current: Dict[str, Any], previous: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    elapsed = max(0.001, _safe_float(current.get("captured_at_ts")) - _safe_float((previous or {}).get("captured_at_ts"))) if previous else 0.0
    indexing_rate = None
    search_rate = None
    if previous and elapsed > 0:
        indexing_rate = max(0.0, (_safe_float(current.get("indexing_total")) - _safe_float(previous.get("indexing_total"))) / elapsed)
        search_rate = max(0.0, (_safe_float(current.get("search_total")) - _safe_float(previous.get("search_total"))) / elapsed)
    heap_used = _safe_float(current.get("heap_used_bytes"))
    heap_max = _safe_float(current.get("heap_max_bytes"))
    heap_percent = round((heap_used / heap_max) * 100, 2) if heap_used > 0 and heap_max > 0 else None
    cards: list[dict[str, Any]] = []
    series: list[dict[str, Any]] = []
    if indexing_rate is not None:
        value = round(indexing_rate, 2)
        cards.append({"label": "索引吞吐", "value": f"{value} docs/s"})
        series.append({"key": "es_indexing_rate", "label": "索引吞吐", "chart": "throughput", "chart_label": "索引吞吐", "color": "#4caf50", "value": value, "display": f"{value} docs/s", "unit": "docs/s", "format": "number"})
    if search_rate is not None:
        value = round(search_rate, 2)
        cards.append({"label": "查询吞吐", "value": f"{value} ops/s"})
        series.append({"key": "es_search_rate", "label": "查询吞吐", "chart": "throughput", "chart_label": "查询吞吐", "color": "#4f7cff", "value": value, "display": f"{value} ops/s", "unit": "ops/s", "format": "number"})
    if heap_percent is not None:
        cards.append({"label": "JVM Heap", "value": f"{heap_percent}%"})
        series.append({"key": "es_heap_percent", "label": "JVM Heap", "chart": "memory", "chart_label": "JVM Heap", "color": "#ff9800", "value": heap_percent, "display": f"{heap_percent}%", "unit": "%", "format": "percent"})
    cpu_percent = current.get("cpu_percent")
    if cpu_percent is not None:
        value = _round_percent(cpu_percent)
        cards.append({"label": "进程 CPU", "value": f"{value}%" if value is not None else "-"})
        series.append({"key": "es_cpu_percent", "label": "进程 CPU", "chart": "cpu", "chart_label": "进程 CPU", "color": "#e95f2b", "value": value, "display": f"{value}%" if value is not None else "-", "unit": "%", "format": "percent"})
    return {"captured_at": _beijing_now().strftime("%Y-%m-%d %H:%M:%S"), "cards": cards, "series": series}


def _esrally_quote_client_option(value: str) -> str:
    return "'" + value.replace("'", "") + "'"


def _build_esrally_client_options(config: BenchmarkConfig) -> str:
    options: list[str] = ["timeout:60"]
    if config.user:
        options.append(f"basic_auth_user:{_esrally_quote_client_option(config.user)}")
        options.append(f"basic_auth_password:{_esrally_quote_client_option(config.password)}")
    if config.ssl_enabled:
        options.append("use_ssl:true")
        options.append(f"verify_certs:{'true' if config.ssl_verify else 'false'}")
        if config.ssl_ca_file:
            options.append(f"ca_certs:{_esrally_quote_client_option(config.ssl_ca_file)}")
    return ",".join(options)


def _merge_es_track_params(config: BenchmarkConfig, concurrency: int) -> str:
    params: dict[str, str] = {}
    raw = str(config.es_track_params or "").strip()
    if raw:
        for part in re.split(r"[,\n]+", raw):
            part = part.strip()
            if not part:
                continue
            if ":" in part:
                key, value = part.split(":", 1)
            elif "=" in part:
                key, value = part.split("=", 1)
            else:
                continue
            params[key.strip()] = value.strip()
    params["bulk_indexing_clients"] = str(concurrency)
    if "ingest_percentage" not in params:
        params["ingest_percentage"] = f"{config.es_ingest_percentage:g}"
    if config.table_size > 0 and "bulk_size" not in params:
        params["bulk_size"] = str(config.table_size)
    return ",".join(f"{key}:{value}" for key, value in params.items())


def _build_esrally_command(config: BenchmarkConfig, concurrency: int, report_file: Path) -> List[str]:
    command = [
        _find_esrally(),
        "race",
        f"--pipeline={config.es_pipeline}",
        f"--target-hosts={_elasticsearch_target_hosts(config)}",
        f"--track={config.workload}",
        f"--track-params={_merge_es_track_params(config, concurrency)}",
        "--report-format=csv",
        f"--report-file={report_file}",
        "--show-in-report=all-percentiles",
    ]
    if config.es_challenge:
        command.append(f"--challenge={config.es_challenge}")
    client_options = _build_esrally_client_options(config)
    if client_options:
        command.append(f"--client-options={client_options}")
    if config.extra_options.strip():
        command.extend(shlex.split(config.extra_options))
    return command


def _elasticsearch_rally_timeout_seconds(config: BenchmarkConfig) -> int:
    """Allow a complete Rally track to finish, including long force merges.

    Rally's official tracks are corpus based rather than bounded by QDBmark's
    duration setting. For example, geonames imports roughly 11.4 million
    documents and permits a single force-merge request to run for two hours.
    Keep the outer watchdog comfortably above that limit so QDBmark does not
    terminate an otherwise healthy race before Rally can write its report.
    """
    return max(4 * 3600, int(config.duration_seconds or 0) + 3 * 3600)


def _elasticsearch_rally_progress_callback(
    log_path: Path,
    start_offset: int,
    log_callback: Optional[LogCallback],
) -> Callable[[], None]:
    """Forward Rally task transitions from its internal log to the job log."""
    state: Dict[str, Any] = {
        "offset": max(0, start_offset),
        "last_finished_task": "",
        "last_finished_seconds": None,
    }
    task_pattern = re.compile(r"executing tasks: \['([^']+)'\]")
    finished_pattern = re.compile(r"finished executing tasks \['([^']+)'\] in ([\d.]+) seconds")
    progress_pattern = re.compile(r"join point \[(\d+)/(\d+)\]")

    def _poll() -> None:
        if not log_path.exists():
            return
        file_size = log_path.stat().st_size
        if file_size < state["offset"]:
            state["offset"] = 0
        with log_path.open("r", encoding="utf-8", errors="replace") as fp:
            fp.seek(state["offset"])
            for line in fp:
                finished_match = finished_pattern.search(line)
                if finished_match:
                    state["last_finished_task"] = finished_match.group(1)
                    state["last_finished_seconds"] = float(finished_match.group(2))
                    continue

                progress_match = progress_pattern.search(line)
                if progress_match:
                    current, total = progress_match.groups()
                    task = state["last_finished_task"] or "当前步骤"
                    elapsed = state["last_finished_seconds"]
                    elapsed_text = f"，耗时 {elapsed:.2f} 秒" if elapsed is not None else ""
                    _emit_log(log_callback, f"[Rally进度] {current}/{total}: {task} 已完成{elapsed_text}")
                    state["last_finished_task"] = ""
                    state["last_finished_seconds"] = None
                    continue

                task_match = task_pattern.search(line)
                if task_match:
                    _emit_log(log_callback, f"[Rally步骤] 开始执行: {task_match.group(1)}")
            state["offset"] = fp.tell()

    return _poll


def _parse_esrally_csv_report(report_file: Path) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    if not report_file.exists():
        return metrics
    with report_file.open(newline="", encoding="utf-8", errors="replace") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            lowered = {str(k or "").strip().lower(): str(v or "").strip() for k, v in row.items()}
            metric = lowered.get("metric") or lowered.get("name") or ""
            value = _safe_float(lowered.get("value"))
            task = lowered.get("task") or ""
            unit = lowered.get("unit") or ""
            key = metric.lower()
            if key == "mean throughput" and value > metrics.get("throughput", 0.0):
                metrics["throughput"] = value
                metrics["throughput_unit"] = unit
                metrics["throughput_task"] = task
            elif key == "median throughput" and not metrics.get("throughput"):
                metrics["throughput"] = value
                metrics["throughput_unit"] = unit
                metrics["throughput_task"] = task
            elif key == "mean latency":
                metrics["avg_latency_ms"] = value
            elif key in {"95th percentile latency", "99th percentile latency", "90th percentile latency"}:
                existing = metrics.get("p95_latency_ms", 0.0)
                if not existing or key.startswith("95th"):
                    metrics["p95_latency_ms"] = value
            elif key == "min latency":
                metrics["min_latency_ms"] = value
            elif key == "max latency":
                metrics["max_latency_ms"] = value
            elif key == "error rate":
                metrics["error_rate"] = max(_safe_float(metrics.get("error_rate")), value)
    return metrics


def _parse_esrally_output(concurrency: int, output: str, report_file: Path, config: BenchmarkConfig) -> ConcurrencyResult:
    metrics = _parse_esrally_csv_report(report_file)
    throughput = _safe_float(metrics.get("throughput"))
    if throughput <= 0:
        throughput = _extract_float(r"Mean Throughput\s*\|[^|]*\|\s*([\d.]+)", output)
    if throughput <= 0:
        raise BenchmarkValidationError(f"未能解析 Rally 输出，请检查 esrally 报告。\n{output[:800]}")
    elapsed_seconds = float(config.duration_seconds or 0)
    total_events = int(round(throughput * elapsed_seconds)) if elapsed_seconds > 0 else 0
    error_rate = _safe_float(metrics.get("error_rate"))
    success_rate = round(max(0.0, 100.0 - error_rate), 2)
    avg_latency_ms = round(_safe_float(metrics.get("avg_latency_ms")), 2)
    p95_latency_ms = round(_safe_float(metrics.get("p95_latency_ms")), 2)
    min_latency_ms = round(_safe_float(metrics.get("min_latency_ms")), 2)
    max_latency_ms = round(_safe_float(metrics.get("max_latency_ms")), 2)
    return ConcurrencyResult(
        concurrency=concurrency,
        elapsed_seconds=elapsed_seconds,
        total_transactions=0,
        total_queries=total_events,
        total_events=total_events,
        read_queries=0,
        write_queries=total_events,
        other_queries=0,
        failed_requests=int(round(total_events * error_rate / 100.0)) if total_events else 0,
        qps=round(throughput, 2),
        tps=round(throughput, 2),
        avg_latency_ms=avg_latency_ms,
        p95_latency_ms=p95_latency_ms,
        min_latency_ms=min_latency_ms,
        max_latency_ms=max_latency_ms,
        success_rate=success_rate,
        baseline_label=_classify_baseline(success_rate, p95_latency_ms),
        sample_errors=_extract_sample_errors(output),
        raw_output=output,
    )


def _run_elasticsearch_benchmark(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> BenchmarkSummary:
    _emit_log(log_callback, f"已接收测试任务: ElasticSearch / {_elasticsearch_target_hosts(config)}")
    _emit_log(log_callback, f"压测工具: Elastic Rally | track={config.workload} | pipeline={config.es_pipeline} | challenge={config.es_challenge or '默认'}")
    _check_cancel(cancel_callback)
    _check_elasticsearch_connectivity(config, log_callback=log_callback)
    artifact_dir = Path(config.artifact_dir or tempfile.mkdtemp(prefix="esrally-"))
    artifact_dir.mkdir(parents=True, exist_ok=True)
    rally_timeout_seconds = _elasticsearch_rally_timeout_seconds(config)
    _emit_log(
        log_callback,
        "Rally 按 track 完整语料执行；持续时间参数不限制完整 track 总耗时。"
        f"本次外层保护超时为 {rally_timeout_seconds} 秒，以覆盖数据导入、force merge 和查询阶段。",
    )
    results: list[ConcurrencyResult] = []
    for concurrency in sorted(set(config.threads_values)):
        _check_cancel(cancel_callback)
        report_file = artifact_dir / f"esrally_{concurrency}.csv"
        command = _build_esrally_command(config, concurrency, report_file)
        rally_log_path = Path.home() / ".rally" / "logs" / "rally.log"
        rally_log_offset = rally_log_path.stat().st_size if rally_log_path.exists() else 0
        progress_callback = _elasticsearch_rally_progress_callback(
            rally_log_path,
            rally_log_offset,
            log_callback,
        )
        _emit_log(log_callback, f"并发 {concurrency}: 开始执行 Elastic Rally race...")
        output = _run_command(
            command,
            timeout_seconds=rally_timeout_seconds,
            log_callback=log_callback,
            log_prefix=f"[esrally:{concurrency}] ",
            log_label=f"{concurrency}并发Elastic Rally测试",
            secrets=[config.password],
            cancel_callback=cancel_callback,
            progress_callback=progress_callback,
        )
        result = _parse_esrally_output(concurrency, output, report_file, config)
        results.append(result)
        _emit_log(log_callback, f"并发 {concurrency}: Rally 完成，吞吐={result.qps} ops/s，P95={result.p95_latency_ms}ms，成功率={result.success_rate}%")
    return BenchmarkSummary(
        report_title=config.report_title,
        executed_at=_beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
        target={
            "db_type": "ElasticSearch",
            "host": config.host,
            "port": config.port,
            "database": config.database or "-",
            "user": config.user,
            "collection_name": "",
            "target_hosts": _elasticsearch_target_hosts(config),
        },
        parameters={
            "benchmark_engine": "esrally",
            "workload": config.workload,
            "duration_seconds": config.duration_seconds,
            "warmup_seconds": config.warmup_seconds,
            "report_interval": config.report_interval,
            "table_count": 0,
            "table_size": config.table_size,
            "operation_count": 0,
            "auth_database": "",
            "collection_name": "",
            "ssl_enabled": config.ssl_enabled,
            "ssl_verify": config.ssl_verify,
            "auto_prepare": False,
            "auto_cleanup": False,
            "threads_values": sorted(set(config.threads_values)),
            "es_pipeline": config.es_pipeline,
            "es_track": config.workload,
            "es_challenge": config.es_challenge,
            "es_ingest_percentage": config.es_ingest_percentage,
            "es_track_params": config.es_track_params,
        },
        results=results,
        recommended_baseline=_select_recommended_baseline(results),
    )

def collect_instance_info(config: BenchmarkConfig) -> Optional[Dict[str, Any]]:
    config.validate()
    if config.db_type in {"MySQL", "TiDB"}:
        return _build_mysql_instance_info(config)
    if config.db_type == "MongoDB":
        return _build_mongodb_instance_info(config)
    if config.db_type == "Redis":
        return _build_redis_instance_info(config)
    if config.db_type == "ElasticSearch":
        return _build_elasticsearch_instance_info(config)
    if config.db_type == "ClickHouse":
        return _build_clickhouse_instance_info(config)
    if config.db_type == "Kafka":
        return {
            "captured_at": _beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
            "cards": [
                {"label": "实例类型", "value": "Kafka"},
                {"label": "Bootstrap Servers", "value": _kafka_bootstrap_servers(config)},
                {"label": "Topic", "value": config.database},
                {"label": "压测工具", "value": "Kafka 官方 perf-test"},
            ],
        }
    if config.db_type == "RocketMQ":
        return {
            "captured_at": _beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
            "cards": [
                {"label": "实例类型", "value": "RocketMQ"},
                {"label": "NameServer", "value": _rocketmq_namesrv(config)},
                {"label": "Topic", "value": config.database},
                {"label": "压测工具", "value": "Apache RocketMQ 官方 benchmark Producer/Consumer"},
            ],
        }
    if config.db_type == "Oracle":
        return {
            "captured_at": _beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
            "cards": [
                {"label": "实例类型", "value": "Oracle"},
                {"label": "实例地址", "value": f"{config.host}:{config.port}/{config.database}"},
                {"label": "压测工具", "value": "HammerDB TPROC-C"},
            ],
        }
    if config.db_type == "KingBase":
        return {
            "captured_at": _beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
            "cards": [
                {"label": "实例类型", "value": "KingBase"},
                {"label": "实例地址", "value": f"{config.host}:{config.port}/{config.database}"},
                {"label": "压测工具", "value": "BenchmarkSQL TPC-C"},
            ],
        }
    if config.db_type == "GaussDB":
        return {
            "captured_at": _beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
            "cards": [
                {"label": "实例类型", "value": "GaussDB"},
                {"label": "架构", "value": "分布式" if str(config.gaussdb_architecture or "").lower() == "distributed" else "集中式"},
                {"label": "实例地址", "value": f"{_gaussdb_target_hosts(config)}/{config.database}"},
                {"label": "压测工具", "value": "BenchmarkSQL TPC-C"},
            ],
        }
    if config.db_type in POSTGRES_DB_TYPES:
        return _build_postgresql_instance_info(config)
    return None


def collect_runtime_sample(config: BenchmarkConfig) -> Optional[Dict[str, Any]]:
    config.validate()
    if config.db_type in {"MySQL", "TiDB"}:
        return _build_mysql_runtime_sample(config)
    if config.db_type == "MongoDB":
        return _build_mongodb_runtime_sample(config)
    if config.db_type == "Redis":
        return _build_redis_runtime_sample(config)
    if config.db_type == "ElasticSearch":
        return _build_elasticsearch_runtime_sample(config)
    if config.db_type in {"ClickHouse", "Kafka", "RocketMQ", "RabbitMQ"}:
        return _build_prometheus_resource_runtime_sample(config)
    if config.db_type in POSTGRES_DB_TYPES:
        return _build_postgresql_runtime_sample(config)
    if config.db_type in {"DM", "SQL Server", "Oracle", "OceanBase", "KingBase"}:
        return _build_prometheus_resource_runtime_sample(config)
    return None


def format_runtime_metrics(
    config: BenchmarkConfig,
    current: Optional[Dict[str, Any]],
    previous: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    if not current:
        return None
    if config.db_type in {"MySQL", "TiDB"}:
        return _format_mysql_runtime_metrics(config, current, previous)
    if config.db_type == "MongoDB":
        return _format_mongodb_runtime_metrics(current, previous)
    if config.db_type == "Redis":
        return _format_redis_runtime_metrics(current, previous)
    if config.db_type == "ElasticSearch":
        return _format_elasticsearch_runtime_metrics(current, previous)
    if config.db_type in {"ClickHouse", "Kafka", "RocketMQ", "RabbitMQ"}:
        return _format_prometheus_resource_runtime_metrics(config, current, previous)
    if config.db_type in POSTGRES_DB_TYPES:
        return _format_postgresql_runtime_metrics(current, previous)
    if config.db_type in {"DM", "SQL Server", "Oracle", "OceanBase", "KingBase"}:
        return _format_prometheus_resource_runtime_metrics(config, current, previous)
    return None


def _check_mongodb_connectivity(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    from pymongo import MongoClient

    uri = _build_mongodb_uri(config)
    masked_uri = uri.replace(config.password, "******") if config.password else uri
    _emit_log(log_callback, "开始检查 MongoDB 连接性...")
    _emit_log(log_callback, f"MongoDB 拓扑: {config.mongo_topology}")
    _emit_log(log_callback, f"连接检测启动: pymongo ping -> {masked_uri}")

    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    try:
        result = client.admin.command("ping")
        _emit_log(log_callback, f"MongoDB 连接检测通过: {result}")
    except Exception as exc:
        raise BenchmarkValidationError(str(exc)) from exc
    finally:
        client.close()


def _check_fio_connectivity(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    target_path = Path(config.fio_target_path).expanduser()
    _emit_log(log_callback, "开始检查 FIO 目标路径...")
    if config.fio_ssh_enabled:
        probe_path = f"{config.fio_target_path}.probe.{os.getpid()}"
        remote_script = " && ".join(
            [
                "command -v fio >/dev/null 2>&1",
                f"mkdir -p {shlex.quote(str(Path(config.fio_target_path).parent))}",
                f": > {shlex.quote(probe_path)}",
                f"rm -f {shlex.quote(probe_path)}",
            ]
        )
        _run_remote_command(
            config,
            f"sh -lc {shlex.quote(remote_script)}",
            timeout_seconds=15,
            log_callback=log_callback,
            log_label="FIO 远程连接检测",
        )
        _emit_log(log_callback, f"FIO 远程路径检测通过: {_fio_display_target(config)}")
        return

    parent = target_path.parent
    _emit_log(log_callback, f"连接检测启动: target={target_path}")
    _validate_local_fio_target_path(target_path)
    try:
        probe = parent / f".fio-probe-{os.getpid()}"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
    except Exception as exc:
        raise BenchmarkValidationError(f"FIO 目标路径不可写: {exc}") from exc
    _emit_log(log_callback, f"FIO 路径检测通过: {target_path}")


def run_connectivity_check(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    config.validate()
    connectivity_target = (
        _fio_display_target(config)
        if config.db_type == "FIO"
        else _mongo_display_target(config)
        if config.db_type == "MongoDB"
        else _elasticsearch_target_hosts(config)
        if config.db_type == "ElasticSearch"
        else f"{_kafka_bootstrap_servers(config)}/{config.database}"
        if config.db_type == "Kafka"
        else f"{_rocketmq_namesrv(config)}/{config.database}"
        if config.db_type == "RocketMQ"
        else f"{_rabbitmq_broker(config)}/{config.database}"
        if config.db_type == "RabbitMQ"
        else f"{config.host}:{config.port}/{_clickhouse_database(config)}"
        if config.db_type == "ClickHouse"
        else _gaussdb_target_hosts(config) + f"/{config.database}"
        if config.db_type == "GaussDB"
        else f"{config.host}:{config.port}/{config.database}"
    )
    _emit_log(log_callback, f"已接收连通性测试: {config.db_type} / {connectivity_target}")
    if config.db_type in {"MySQL", "TiDB"}:
        _check_mysql_connectivity(config, log_callback=log_callback)
        return
    if config.db_type == "MongoDB":
        _check_mongodb_connectivity(config, log_callback=log_callback)
        return
    if config.db_type == "Redis":
        _check_redis_connectivity(config, log_callback=log_callback)
        return
    if config.db_type == "ClickHouse":
        _check_clickhouse_connectivity(config, log_callback=log_callback)
        return
    if config.db_type == "Kafka":
        _check_kafka_connectivity(config, log_callback=log_callback)
        return
    if config.db_type == "RocketMQ":
        _check_rocketmq_connectivity(config, log_callback=log_callback)
        return
    if config.db_type == "RabbitMQ":
        _check_rabbitmq_connectivity(config, log_callback=log_callback)
        return
    if config.db_type == "Oracle":
        _check_oracle_connectivity(config, log_callback=log_callback)
        return
    if config.db_type == "SQL Server":
        _check_sqlserver_connectivity(config, log_callback=log_callback)
        return
    if config.db_type == "DM":
        _check_dm_connectivity(config, log_callback=log_callback)
        return
    if config.db_type == "GaussDB":
        _check_gaussdb_connectivity(config, log_callback=log_callback)
        return
    if config.db_type in POSTGRES_DB_TYPES:
        _check_postgresql_connectivity(config, log_callback=log_callback)
        return
    if config.db_type == "FIO":
        _check_fio_connectivity(config, log_callback=log_callback)
        return
    raise BenchmarkValidationError(
        f"当前版本仅支持 MySQL / TiDB / SQL Server / Oracle / PostgreSQL / GaussDB / OpenGauss / DM / OceanBase / KingBase / Vastbase / MongoDB / Redis / ElasticSearch / ClickHouse / Kafka / RocketMQ / RabbitMQ / FIO 连通性测试，已选择 {config.db_type}。"
    )


def _extract_int(pattern: str, text: str, flags: int = 0) -> int:
    match = re.search(pattern, text, re.MULTILINE | flags)
    return int(match.group(1)) if match else 0


def _extract_float(pattern: str, text: str, flags: int = 0) -> float:
    match = re.search(pattern, text, re.MULTILINE | flags)
    return round(float(match.group(1)), 2) if match else 0.0


def _extract_sample_errors(output: str) -> List[str]:
    error_lines: List[str] = []
    for line in output.splitlines():
        lowered = line.lower()
        stripped = lowered.strip()
        if "ignored errors:" in lowered:
            match = re.search(r"ignored errors:\s+(\d+)", lowered)
            if match and int(match.group(1)) == 0:
                continue
        if re.search(r'"error"\s*:\s*0\b', stripped):
            continue
        if re.search(r'"failed"\s*:\s*0\b', stripped):
            continue
        if "error" in lowered or "failed" in lowered:
            candidate = line.strip()
            if candidate and candidate not in error_lines:
                error_lines.append(candidate)
        if len(error_lines) >= 5:
            break
    return error_lines


def _extract_ycsb_metrics(output: str) -> Dict[str, Dict[str, str]]:
    metrics: Dict[str, Dict[str, str]] = {}
    for line in output.splitlines():
        if not line.startswith("["):
            continue
        parts = [part.strip() for part in line.split(",", 2)]
        if len(parts) != 3:
            continue
        section = parts[0].strip("[]").strip()
        key = parts[1].strip()
        value = parts[2].strip()
        metrics.setdefault(section, {})[key] = value
    return metrics


def _metric_number(value: str) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _latency_metric_ms(section_metrics: Dict[str, str], key_prefix: str) -> float:
    for suffix, divisor in (("(us)", 1000.0), ("(ms)", 1.0)):
        key = f"{key_prefix}{suffix}"
        if key in section_metrics:
            return round(_metric_number(section_metrics[key]) / divisor, 2)
    return 0.0


def _calculate_percentile(values: List[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(round((len(ordered) - 1) * percentile))))
    return round(ordered[index], 2)


def _fio_percentile_ms(section: Dict[str, Any]) -> float:
    clat_ns = section.get("clat_ns") or {}
    percentiles = clat_ns.get("percentile") or {}
    for key in ("95.000000", "95.0000000", "95.0000000000"):
        if key in percentiles:
            return round(float(percentiles[key]) / 1_000_000.0, 2)
    return 0.0


def _fio_mean_ms(section: Dict[str, Any]) -> float:
    clat_ns = section.get("clat_ns") or {}
    mean = clat_ns.get("mean")
    if mean is None:
        return 0.0
    return round(float(mean) / 1_000_000.0, 2)


def _fio_min_ms(section: Dict[str, Any]) -> float:
    clat_ns = section.get("clat_ns") or {}
    value = clat_ns.get("min")
    if value is None:
        return 0.0
    return round(float(value) / 1_000_000.0, 2)


def _fio_max_ms(section: Dict[str, Any]) -> float:
    clat_ns = section.get("clat_ns") or {}
    value = clat_ns.get("max")
    if value is None:
        return 0.0
    return round(float(value) / 1_000_000.0, 2)


def _run_prepare(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> None:
    prepare_workload = _mysql_stage_workload("prepare", config.workload)
    _emit_log(log_callback, "开始准备 sysbench 测试数据...")
    _run_command(
        _common_command(config, log_callback=log_callback, workload=prepare_workload) + ["cleanup"],
        timeout_seconds=300,
        allow_failure=True,
        log_callback=log_callback,
        log_prefix="[prepare] ",
        log_label="prepare-cleanup",
        secrets=[config.password],
        cancel_callback=cancel_callback,
        env=_mysql_sysbench_env(log_callback),
    )
    _run_command(
        _common_command(config, log_callback=log_callback, workload=prepare_workload) + ["prepare"],
        timeout_seconds=1800,
        log_callback=log_callback,
        log_prefix="[prepare] ",
        log_label="prepare",
        secrets=[config.password],
        cancel_callback=cancel_callback,
        env=_mysql_sysbench_env(log_callback),
    )
    _mark_mysql_dataset_prepared(config)


def _run_mysql_warmup(
    config: BenchmarkConfig,
    concurrency: int,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> None:
    if config.warmup_seconds <= 0:
        return

    warmup_workload = _mysql_stage_workload("warmup", config.workload)
    _emit_log(
        log_callback,
        f"数据预热启动: 线程 {concurrency} / {config.warmup_seconds}s / workload={warmup_workload}",
    )
    _run_command(
        _common_command(config, log_callback=log_callback, workload=warmup_workload)
        + [
            f"--threads={concurrency}",
            f"--time={config.warmup_seconds}",
            "--events=0",
            f"--report-interval={config.report_interval}",
            "run",
        ],
        timeout_seconds=config.warmup_seconds + 180,
        log_callback=log_callback,
        log_prefix=f"[warmup:{concurrency}] ",
        log_label=f"{concurrency}线程数据预热",
        secrets=[config.password],
        cancel_callback=cancel_callback,
        env=_mysql_sysbench_env(log_callback),
    )
    _mark_mysql_dataset_warmed(config, config.warmup_seconds)


def _run_cleanup(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> None:
    _emit_log(log_callback, "开始清理 sysbench 测试数据...")
    _run_command(
        _common_command(config, log_callback=log_callback, workload=_mysql_stage_workload("prepare", config.workload))
        + ["cleanup"],
        timeout_seconds=300,
        allow_failure=True,
        log_callback=log_callback,
        log_prefix="[cleanup] ",
        log_label="cleanup",
        secrets=[config.password],
        cancel_callback=cancel_callback,
        env=_mysql_sysbench_env(log_callback),
    )
    _clear_mysql_dataset_cache(config)


def _parse_sysbench_output(concurrency: int, output: str) -> ConcurrencyResult:
    total_transactions = _extract_int(r"transactions:\s+(\d+)", output)
    total_queries = _extract_int(r"queries:\s+(\d+)", output)
    total_events = _extract_int(r"total number of events:\s+(\d+)", output) or total_transactions
    read_queries = _extract_int(r"read:\s+(\d+)", output)
    write_queries = _extract_int(r"write:\s+(\d+)", output)
    other_queries = _extract_int(r"other:\s+(\d+)", output)
    failed_requests = _extract_int(r"ignored errors:\s+(\d+)", output)
    tps = _extract_float(r"transactions:\s+\d+\s+\(([\d.]+)\s+per sec\.\)", output)
    qps = _extract_float(r"queries:\s+\d+\s+\(([\d.]+)\s+per sec\.\)", output)
    elapsed_seconds = _extract_float(r"total time:\s+([\d.]+)s", output)
    avg_latency_ms = _extract_float(r"avg:\s+([\d.]+)", output)
    p95_latency_ms = _extract_float(r"95th percentile:\s+([\d.]+)", output)
    min_latency_ms = _extract_float(r"min:\s+([\d.]+)", output)
    max_latency_ms = _extract_float(r"max:\s+([\d.]+)", output)
    success_rate = round(((total_events - failed_requests) / total_events) * 100, 2) if total_events else 0.0

    if not total_events and not total_transactions:
        raise BenchmarkValidationError(f"未能解析 sysbench 输出，请检查目标数据库连接。\n{output[:800]}")

    return ConcurrencyResult(
        concurrency=concurrency,
        elapsed_seconds=elapsed_seconds,
        total_transactions=total_transactions,
        total_queries=total_queries,
        total_events=total_events,
        read_queries=read_queries,
        write_queries=write_queries,
        other_queries=other_queries,
        failed_requests=failed_requests,
        qps=qps,
        tps=tps,
        avg_latency_ms=avg_latency_ms,
        p95_latency_ms=p95_latency_ms,
        min_latency_ms=min_latency_ms,
        max_latency_ms=max_latency_ms,
        success_rate=success_rate,
        baseline_label=_classify_baseline(success_rate, p95_latency_ms),
        sample_errors=_extract_sample_errors(output),
        raw_output=output,
    )


def _parse_ycsb_output(concurrency: int, output: str) -> ConcurrencyResult:
    metrics = _extract_ycsb_metrics(output)
    overall = metrics.get("OVERALL", {})
    runtime_ms = _metric_number(overall.get("RunTime(ms)", "0"))
    throughput = round(_metric_number(overall.get("Throughput(ops/sec)", "0")), 2)

    read_queries = 0
    write_queries = 0
    total_events = 0
    failed_requests = 0
    weighted_latency = 0.0
    p95_candidates: List[float] = []
    min_candidates: List[float] = []
    max_candidates: List[float] = []

    for section, section_metrics in metrics.items():
        if section in {"OVERALL", "CLEANUP"}:
            continue
        operations = int(_metric_number(section_metrics.get("Operations", "0")))
        if operations <= 0:
            continue

        total_events += operations
        average_ms = _latency_metric_ms(section_metrics, "AverageLatency")
        p95_ms = _latency_metric_ms(section_metrics, "95thPercentileLatency")
        min_ms = _latency_metric_ms(section_metrics, "MinLatency")
        max_ms = _latency_metric_ms(section_metrics, "MaxLatency")

        weighted_latency += average_ms * operations
        if p95_ms:
            p95_candidates.append(p95_ms)
        if min_ms:
            min_candidates.append(min_ms)
        if max_ms:
            max_candidates.append(max_ms)

        if section in {"READ", "SCAN"}:
            read_queries += operations
        else:
            write_queries += operations

        for key, value in section_metrics.items():
            if not key.startswith("Return="):
                continue
            if key != "Return=OK":
                failed_requests += int(_metric_number(value))

    if total_events <= 0 and throughput > 0 and runtime_ms > 0:
        total_events = int(round(throughput * runtime_ms / 1000.0))
    if total_events <= 0:
        raise BenchmarkValidationError(f"未能解析 YCSB 输出，请检查 MongoDB 连接或 YCSB 执行结果。\n{output[:800]}")

    avg_latency_ms = round(weighted_latency / total_events, 2) if weighted_latency else 0.0
    p95_latency_ms = max(p95_candidates) if p95_candidates else 0.0
    min_latency_ms = min(min_candidates) if min_candidates else 0.0
    max_latency_ms = max(max_candidates) if max_candidates else 0.0
    success_rate = round(((total_events - failed_requests) / total_events) * 100, 2) if total_events else 0.0

    return ConcurrencyResult(
        concurrency=concurrency,
        elapsed_seconds=round(runtime_ms / 1000.0, 2),
        total_transactions=total_events,
        total_queries=total_events,
        total_events=total_events,
        read_queries=read_queries,
        write_queries=write_queries,
        other_queries=0,
        failed_requests=failed_requests,
        qps=throughput,
        tps=throughput,
        avg_latency_ms=avg_latency_ms,
        p95_latency_ms=p95_latency_ms,
        min_latency_ms=min_latency_ms,
        max_latency_ms=max_latency_ms,
        success_rate=success_rate,
        baseline_label=_classify_baseline(success_rate, p95_latency_ms),
        sample_errors=_extract_sample_errors(output),
        raw_output=output,
    )


def _parse_pgbench_output(concurrency: int, output: str, workload: str) -> ConcurrencyResult:
    total_transactions = _extract_int(r"number of transactions actually processed:\s+(\d+)", output)
    failed_requests = _extract_int(r"number of failed transactions:\s+(\d+)", output)
    elapsed_seconds = _extract_float(r"duration:\s+([\d.]+)\s+s", output)
    avg_latency_ms = _extract_float(r"latency average\s*(?:=|:)\s+([\d.]+)\s+ms", output)
    tps = _extract_float(r"tps =\s+([\d.]+)\s+\(excluding connections establishing\)", output)
    if tps <= 0:
        tps = _extract_float(r"tps =\s+([\d.]+)\s+\(including connections establishing\)", output)

    if total_transactions <= 0 or tps <= 0:
        raise BenchmarkValidationError(f"未能解析 pgbench 输出，请检查目标数据库连接。\n{output[:800]}")

    query_count = _pgbench_query_count(workload)
    total_queries = total_transactions * query_count
    qps = round(tps * query_count, 2)
    total_events = total_transactions + failed_requests
    success_rate = round((total_transactions / total_events) * 100, 2) if total_events else 0.0
    p95_latency_ms = avg_latency_ms

    return ConcurrencyResult(
        concurrency=concurrency,
        elapsed_seconds=elapsed_seconds,
        total_transactions=total_transactions,
        total_queries=total_queries,
        total_events=total_events,
        read_queries=0,
        write_queries=total_queries,
        other_queries=0,
        failed_requests=failed_requests,
        qps=qps,
        tps=tps,
        avg_latency_ms=avg_latency_ms,
        p95_latency_ms=p95_latency_ms,
        min_latency_ms=0.0,
        max_latency_ms=0.0,
        success_rate=success_rate,
        baseline_label=_classify_baseline(success_rate, p95_latency_ms),
        sample_errors=_extract_sample_errors(output),
        raw_output=output,
    )


def _select_recommended_baseline(results: List[ConcurrencyResult]) -> Optional[ConcurrencyResult]:
    stable_results = [
        result
        for result in results
        if result.success_rate >= 99 and result.baseline_label in {"推荐基线", "稳定", "高负载"}
    ]
    if stable_results:
        return max(stable_results, key=lambda item: (item.concurrency, item.tps, item.qps))
    return max(results, key=lambda item: (item.tps, item.qps), default=None)


def _build_ycsb_command(config: BenchmarkConfig, mode: str, threads: int) -> List[str]:
    ycsb_bin = _find_ycsb()
    workload_entry = _resolve_ycsb_workload_entry(config.workload, ycsb_bin)
    command = [
        ycsb_bin,
        mode,
        "mongodb",
        "-s",
        "-P",
        workload_entry,
        "-p",
        f"mongodb.url={_build_mongodb_uri(config)}",
        "-p",
        f"table={config.collection_name}",
        "-p",
        f"recordcount={config.table_size}",
        "-p",
        f"operationcount={config.table_size if mode == 'load' else config.operation_count}",
        "-threads",
        str(threads),
    ]
    return command


def _estimate_ycsb_timeout(total_operations: int, threads: int, floor_seconds: int = 3600, ceiling_seconds: int = 21600) -> int:
    if total_operations <= 0:
        return floor_seconds
    # MongoDB load throughput can drop sharply on sharded clusters because the first
    # bulk import also creates chunks, indexes, and journal pressure. Use a conservative
    # floor so near-complete loads are not killed by the platform timeout.
    estimated_seconds = int((total_operations / max(1, threads)) / 15.0) + 900
    return max(floor_seconds, min(ceiling_seconds, estimated_seconds))


def _run_ycsb_stage(
    config: BenchmarkConfig,
    mode: str,
    threads: int,
    timeout_seconds: int,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> ConcurrencyResult:
    stage_label = "YCSB load" if mode == "load" else "YCSB run"
    command = _build_ycsb_command(config, mode, threads)
    output = _run_command(
        command,
        timeout_seconds=timeout_seconds,
        log_callback=log_callback,
        log_prefix=f"[{mode}:{threads}] ",
        log_label=f"{threads}线程{stage_label}",
        secrets=[config.password],
        cancel_callback=cancel_callback,
    )
    return _parse_ycsb_output(threads, output)


def _mongo_collection_exists(collection: Any) -> bool:
    try:
        names = collection.database.list_collection_names(filter={"name": collection.name})
        return collection.name in names
    except TypeError:
        return collection.name in collection.database.list_collection_names()


def _mongo_estimated_document_count(collection: Any) -> int:
    if not _mongo_collection_exists(collection):
        return 0
    try:
        return int(collection.estimated_document_count(maxTimeMS=5000))
    except Exception:
        return int(collection.count_documents({}, maxTimeMS=5000))


def _check_mongodb_ycsb_readiness(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    require_data: bool = False,
) -> Dict[str, Any]:
    client = _create_mongo_client(config)
    try:
        database = client[config.database]
        collection = database[config.collection_name]
        collection_exists = _mongo_collection_exists(collection)
        document_count = _mongo_estimated_document_count(collection) if collection_exists else 0
        sharded = False

        if config.mongo_topology == "sharded":
            if not collection_exists:
                raise BenchmarkValidationError(
                    f"分片集群模式下未找到测试集合 {config.database}.{config.collection_name}。"
                    "请先在 mongos 上手工创建集合、索引并执行 shardCollection，再运行 MongoDB YCSB 压测。"
                )
            try:
                stats = database.command("collStats", config.collection_name)
            except Exception as exc:
                raise BenchmarkValidationError(f"无法获取分片集合状态: {exc}") from exc
            sharded = bool(stats.get("sharded"))
            if not sharded:
                raise BenchmarkValidationError(
                    f"集合 {config.database}.{config.collection_name} 当前尚未启用分片。"
                    "请参考手册先执行 sh.enableSharding/createIndex/shardCollection。"
                )

            index_info = collection.index_information()
            has_shard_key_index = any(
                any(part[0] == config.mongo_shard_key for part in info.get("key", []))
                for info in index_info.values()
            )
            if has_shard_key_index:
                _emit_log(
                    log_callback,
                    f"分片集合校验通过: {config.database}.{config.collection_name} 已开启分片，分片键字段 {config.mongo_shard_key} 已存在索引。",
                )
            else:
                _emit_log(
                    log_callback,
                    f"分片集合已开启分片，但未检测到分片键字段 {config.mongo_shard_key} 的索引定义，请确认手工分片配置是否与页面参数一致。",
                )
        elif not collection_exists:
            raise BenchmarkValidationError(
                f"未找到测试集合 {config.database}.{config.collection_name}。若需要自动造数，请开启“测试前自动载入 YCSB 数据”。"
            )

        if require_data and document_count < config.table_size:
            raise BenchmarkValidationError(
                f"当前集合预计仅有 {document_count} 条记录，低于当前配置的 recordcount={config.table_size}。"
                "请先开启“测试前自动载入 YCSB 数据”重新造数，或把记录总数调整到当前集合实际规模。"
            )

        return {
            "collection_exists": collection_exists,
            "document_count": document_count,
            "sharded": sharded,
        }
    finally:
        client.close()


def _clear_mongo_collection_data(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    allow_failure: bool = False,
) -> None:
    client = _create_mongo_client(config)
    try:
        collection = client[config.database][config.collection_name]
        if config.mongo_topology == "sharded":
            deleted = collection.delete_many({})
            _emit_log(
                log_callback,
                f"已清空 MongoDB 分片集合中的历史数据: {config.database}.{config.collection_name}，删除 {deleted.deleted_count} 条记录。",
            )
        else:
            client[config.database].drop_collection(config.collection_name)
            _emit_log(log_callback, f"已删除 MongoDB 测试集合: {config.database}.{config.collection_name}")
    except Exception as exc:
        if allow_failure:
            _emit_log(log_callback, f"MongoDB 集合清理失败，已忽略: {exc}")
            return
        raise BenchmarkValidationError(str(exc)) from exc
    finally:
        client.close()


def _run_mongo_prepare(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> None:
    _emit_log(log_callback, "开始准备 MongoDB YCSB 测试数据...")
    if config.mongo_topology == "sharded":
        _check_mongodb_ycsb_readiness(config, log_callback=log_callback, require_data=False)
    _clear_mongo_collection_data(config, log_callback=log_callback, allow_failure=True)
    load_threads = max(sorted(set(config.threads_values)))
    timeout_seconds = _estimate_ycsb_timeout(config.table_size, load_threads)
    result = _run_ycsb_stage(
        config,
        "load",
        load_threads,
        timeout_seconds=timeout_seconds,
        log_callback=log_callback,
        cancel_callback=cancel_callback,
    )
    _emit_log(
        log_callback,
        f"MongoDB YCSB load 完成，线程={load_threads}，写入吞吐={result.qps} ops/s，P95={result.p95_latency_ms}ms。",
    )


def _run_mongo_cleanup(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    _emit_log(log_callback, "开始清理 MongoDB 测试集合...")
    _clear_mongo_collection_data(config, log_callback=log_callback, allow_failure=True)


def _run_postgres_sysbench_prepare(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> None:
    prepare_workload = _mysql_stage_workload("prepare", config.workload)
    _emit_log(log_callback, f"开始准备 {config.db_type} sysbench 测试数据...")
    _run_command(
        _build_postgres_sysbench_command(config, workload=prepare_workload) + ["cleanup"],
        timeout_seconds=300,
        allow_failure=True,
        log_callback=log_callback,
        log_prefix="[pg-prepare] ",
        log_label="pgsysbench-cleanup",
        secrets=[config.password],
        cancel_callback=cancel_callback,
        env=_postgres_command_env(config),
    )
    _run_command(
        _build_postgres_sysbench_command(config, workload=prepare_workload) + ["prepare"],
        timeout_seconds=3600,
        log_callback=log_callback,
        log_prefix="[pg-prepare] ",
        log_label="pgsysbench-prepare",
        secrets=[config.password],
        cancel_callback=cancel_callback,
        env=_postgres_command_env(config),
    )
    vacuumdb_path = _find_vacuumdb()
    vacuum_command = [
        vacuumdb_path,
        "-h",
        config.host,
        "-p",
        str(config.port),
        "-U",
        config.user,
        "-d",
        config.database,
        "-z",
    ]
    if _vacuumdb_supports_jobs(vacuumdb_path):
        vacuum_jobs = 16 if config.db_type == "OpenGauss" else max(1, min(16, max(sorted(set(config.threads_values)))))
        vacuum_command[7:7] = ["-j", str(vacuum_jobs)]
    else:
        _emit_log(log_callback, "当前 vacuumdb 不支持 -j/--jobs，已自动降级为单线程 ANALYZE。")
    _run_command(
        vacuum_command,
        timeout_seconds=1800,
        log_callback=log_callback,
        log_prefix="[vacuumdb] ",
        log_label="vacuumdb-analyze",
        cancel_callback=cancel_callback,
        env=_postgres_command_env(config),
    )


def _run_postgres_sysbench_cleanup(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> None:
    _emit_log(log_callback, f"开始清理 {config.db_type} sysbench 测试数据...")
    _run_command(
        _build_postgres_sysbench_command(config, workload=_mysql_stage_workload("prepare", config.workload)) + ["cleanup"],
        timeout_seconds=300,
        allow_failure=True,
        log_callback=log_callback,
        log_prefix="[pg-cleanup] ",
        log_label="pgsysbench-cleanup",
        secrets=[config.password],
        cancel_callback=cancel_callback,
        env=_postgres_command_env(config),
    )


def _run_pgbench_prepare(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> None:
    _emit_log(
        log_callback,
        f"开始初始化 pgbench 数据: scale={config.pgbench_scale}, fillfactor={config.pgbench_fillfactor}",
    )
    _run_command(
        _build_pgbench_init_command(config),
        timeout_seconds=3600,
        log_callback=log_callback,
        log_prefix="[pgbench-init] ",
        log_label="pgbench-init",
        cancel_callback=cancel_callback,
        env=_postgres_command_env(config),
    )


def _run_pgbench_cleanup(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> None:
    import psycopg2

    _emit_log(log_callback, "开始清理 pgbench 测试表...")
    connection = psycopg2.connect(**_postgres_connect_kwargs(config))
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                DROP TABLE IF EXISTS
                    pgbench_accounts,
                    pgbench_branches,
                    pgbench_history,
                    pgbench_tellers
                """
            )
        _emit_log(log_callback, "已删除 pgbench_accounts / pgbench_branches / pgbench_history / pgbench_tellers。")
    except Exception as exc:
        raise BenchmarkValidationError(f"清理 pgbench 测试表失败: {exc}") from exc
    finally:
        try:
            connection.close()
        except Exception:
            pass
    _check_cancel(cancel_callback)


def _friendly_fio_failure_message(config: BenchmarkConfig, output: str) -> str:
    message = str(output or "")
    if (
        "destination does not support O_DIRECT" in message
        or "direct=1/buffered=0" in message
        or "O_DIRECT" in message
    ):
        return (
            "FIO 测试失败: 当前目标路径所在文件系统不支持 Direct IO/O_DIRECT。"
            f" 目标: {_fio_display_target(config)}。"
            " 当前命令启用了 direct=1，fio 无法打开测试文件，因此没有执行任何 I/O。"
            " 处理方式: 关闭页面中的 Direct IO 选项后重试，或将测试文件路径改到支持 O_DIRECT 的本地磁盘/块设备文件系统"
            "（常见如 ext4/xfs 的真实磁盘挂载路径，避免 tmpfs、部分 overlay/NFS/特殊挂载目录）。"
        )
    if "No space left on device" in message:
        return (
            "FIO 测试失败: 目标路径可用空间不足。"
            f" 目标: {_fio_display_target(config)}。"
            " 请减小测试文件大小，或更换到剩余空间充足的路径。"
        )
    if "command not found" in message or "fio: command not found" in message:
        return "FIO 测试失败: 远程节点未安装 fio，请先在目标节点安装 fio 后重试。"
    return message


def _build_fio_command(config: BenchmarkConfig, concurrency: int) -> List[str]:
    return [
        _find_fio(),
        f"--name=fio_{concurrency}",
        f"--filename={config.fio_target_path}",
        f"--rw={config.workload}",
        f"--bs={config.fio_block_size}",
        f"--iodepth={config.fio_iodepth}",
        f"--numjobs={concurrency}",
        f"--size={config.fio_size_mb}M",
        f"--runtime={config.duration_seconds}",
        f"--ramp_time={config.warmup_seconds}",
        "--time_based=1",
        "--group_reporting=1",
        f"--direct={1 if config.fio_direct else 0}",
        "--ioengine=libaio",
        "--output-format=json",
    ] + ([f"--rwmixread={config.fio_read_percent}"] if config.workload == "randrw" else [])


def _extract_fio_json_payload(output: str) -> dict:
    import json

    text = str(output or "")
    decoder = json.JSONDecoder()
    for index, char in enumerate(text):
        if char != "{":
            continue
        try:
            payload, _ = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and "jobs" in payload:
            return payload
    raise BenchmarkValidationError(f"未能解析 fio 输出，请检查执行结果。\n{text[:800]}")


def _parse_fio_output(concurrency: int, output: str) -> ConcurrencyResult:
    payload = _extract_fio_json_payload(output)

    jobs = payload.get("jobs") or []
    if not jobs:
        raise BenchmarkValidationError(f"fio 输出中没有 jobs 数据。\n{output[:800]}")

    job = jobs[0]
    read = job.get("read") or {}
    write = job.get("write") or {}
    error_count = int(job.get("error", 0))
    elapsed_seconds = float(job.get("job_runtime", 0.0)) / 1000.0

    read_ios = int(read.get("total_ios", 0))
    write_ios = int(write.get("total_ios", 0))
    total_ios = read_ios + write_ios
    bw_bytes = float(read.get("bw_bytes", 0)) + float(write.get("bw_bytes", 0))
    bw_mb = round(bw_bytes / 1024.0 / 1024.0, 2)
    iops = round(float(read.get("iops", 0)) + float(write.get("iops", 0)), 2)

    mean_candidates = [value for value in (_fio_mean_ms(read), _fio_mean_ms(write)) if value > 0]
    avg_latency_ms = round(sum(mean_candidates) / len(mean_candidates), 2) if mean_candidates else 0.0
    p95_latency_ms = max(_fio_percentile_ms(read), _fio_percentile_ms(write))
    min_latency_ms = min(value for value in (_fio_min_ms(read), _fio_min_ms(write)) if value > 0) if any(
        value > 0 for value in (_fio_min_ms(read), _fio_min_ms(write))
    ) else 0.0
    max_latency_ms = max(_fio_max_ms(read), _fio_max_ms(write))
    success_rate = round(((total_ios - error_count) / total_ios) * 100, 2) if total_ios else 0.0

    return ConcurrencyResult(
        concurrency=concurrency,
        elapsed_seconds=round(elapsed_seconds, 2),
        total_transactions=total_ios,
        total_queries=total_ios,
        total_events=total_ios,
        read_queries=read_ios,
        write_queries=write_ios,
        other_queries=0,
        failed_requests=error_count,
        qps=iops,
        tps=bw_mb,
        avg_latency_ms=avg_latency_ms,
        p95_latency_ms=p95_latency_ms,
        min_latency_ms=min_latency_ms,
        max_latency_ms=max_latency_ms,
        success_rate=success_rate,
        baseline_label=_classify_baseline(success_rate, p95_latency_ms),
        sample_errors=_extract_sample_errors(output),
        raw_output=output,
    )


def _run_fio_cleanup(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> None:
    if config.fio_ssh_enabled:
        try:
            _run_remote_command(
                config,
                f"sh -lc {shlex.quote(f'rm -f {shlex.quote(config.fio_target_path)}')}",
                timeout_seconds=20,
                allow_failure=True,
                log_callback=log_callback,
                log_label="FIO 远程清理",
                cancel_callback=cancel_callback,
            )
            _emit_log(log_callback, f"已删除远程 FIO 测试文件: {_fio_display_target(config)}")
        except Exception as exc:
            _emit_log(log_callback, f"远程 FIO 测试文件清理失败，已忽略: {exc}")
        return

    target_path = Path(config.fio_target_path).expanduser()
    try:
        target_path.unlink(missing_ok=True)
        _emit_log(log_callback, f"已删除 FIO 测试文件: {target_path}")
    except Exception as exc:
        _emit_log(log_callback, f"FIO 测试文件清理失败，已忽略: {exc}")


def cleanup_benchmark_data(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> None:
    config.validate()
    _check_cancel(cancel_callback)
    _emit_log(log_callback, f"开始执行 {config.db_type} 测试数据删除...")

    if config.db_type == "TiDB" and config.tidb_engine == "tiup_tpcc":
        _run_tidb_tiup_stage(
            config,
            "cleanup",
            timeout_seconds=1800,
            allow_failure=True,
            log_callback=log_callback,
            cancel_callback=cancel_callback,
        )
    elif config.db_type in {"MySQL", "TiDB"}:
        _run_cleanup(config, log_callback=log_callback, cancel_callback=cancel_callback)
    elif config.db_type == "Redis":
        _run_redis_cleanup(config, log_callback=log_callback, cancel_callback=cancel_callback)
    elif config.db_type == "MongoDB":
        _run_mongo_cleanup(config, log_callback=log_callback)
    elif config.db_type == "ClickHouse":
        _run_clickhouse_cleanup(config, log_callback=log_callback, cancel_callback=cancel_callback)
    elif config.db_type == "Kafka":
        _run_kafka_cleanup(config, log_callback=log_callback, cancel_callback=cancel_callback)
    elif config.db_type == "RocketMQ":
        _run_rocketmq_cleanup(config, log_callback=log_callback, cancel_callback=cancel_callback)
    elif config.db_type == "RabbitMQ":
        _run_rabbitmq_cleanup(config, log_callback=log_callback, cancel_callback=cancel_callback)
    elif config.db_type in POSTGRES_DB_TYPES:
        if config.postgres_engine == "pgbench":
            _run_pgbench_cleanup(config, log_callback=log_callback, cancel_callback=cancel_callback)
        else:
            _run_postgres_sysbench_cleanup(config, log_callback=log_callback, cancel_callback=cancel_callback)
    elif config.db_type == "FIO":
        _run_fio_cleanup(config, log_callback=log_callback, cancel_callback=cancel_callback)
    elif config.db_type == "SQL Server":
        _run_sqlserver_cleanup(config, log_callback=log_callback, cancel_callback=cancel_callback)
    elif config.db_type == "Oracle":
        if config.oracle_engine != "hammerdb":
            raise BenchmarkValidationError("Oracle Swingbench 数据清理暂未内置，请使用 HammerDB 任务或手动清理 SOE schema。")
        _run_hammerdb_script(
            _hammerdb_oracle_deleteschema_script(config),
            timeout_seconds=1800,
            log_callback=log_callback,
            log_label="hammerdb-oracle-deleteschema",
            secrets=[config.password],
            cancel_callback=cancel_callback,
            artifact_dir=config.artifact_dir,
        )
    elif config.db_type == "DM":
        schema = _dm_benchmark_schema(config)
        with tempfile.TemporaryDirectory(prefix="dm_cleanup_") as tmpdir:
            sql_path = Path(tmpdir) / "dm_cleanup.sql"
            _dm_write_user_cleanup_sql(sql_path, schema)
            output = _run_dm_disql(config, sql_path.read_text(encoding="utf-8"), timeout_seconds=300, allow_disconnect=True)
            _emit_log(log_callback, output[-1200:] if output else f"DM schema {schema} 清理 SQL 已执行。")
    elif config.db_type == "OceanBase":
        workdir = _prepare_oceanbase_workdir(config, max(sorted(set(config.threads_values))))
        _run_oceanbase_tool_script(
            config,
            workdir,
            "runDatabaseDestroy.sh",
            timeout_seconds=max(900, int(config.table_size) * 10),
            log_label="oceanbase-benchmarksql-destroy",
            log_callback=log_callback,
            cancel_callback=cancel_callback,
            allow_failure=True,
        )
    elif config.db_type == "KingBase":
        workdir = _prepare_kingbase_workdir(config, max(sorted(set(config.threads_values))))
        _run_kingbase_tool_script(
            config,
            workdir,
            "runDatabaseDestroy.sh",
            timeout_seconds=max(900, int(config.table_size) * 10),
            log_label="kingbase-benchmarksql-destroy",
            log_callback=log_callback,
            cancel_callback=cancel_callback,
            allow_failure=True,
        )
    elif config.db_type == "GaussDB":
        workdir = _prepare_gaussdb_workdir(config, max(sorted(set(config.threads_values))))
        _run_gaussdb_tool_script(
            config,
            workdir,
            "runDatabaseDestroy.sh",
            timeout_seconds=max(900, int(config.table_size) * 10),
            log_label="gaussdb-benchmarksql-destroy",
            log_callback=log_callback,
            cancel_callback=cancel_callback,
            allow_failure=True,
        )
    elif config.db_type == "ElasticSearch":
        _emit_log(log_callback, "ElasticSearch Rally 未固定生成单一测试索引，平台已跳过自动删除，避免误删业务索引。")
    else:
        raise BenchmarkValidationError(f"{config.db_type} 暂不支持删除测试数据。")

    _check_cancel(cancel_callback)
    _emit_log(log_callback, f"{config.db_type} 测试数据删除流程完成。")


def _tidb_tiup_duration(seconds: int) -> str:
    safe_seconds = max(1, int(seconds or 0))
    if safe_seconds % 60 == 0:
        return f"{safe_seconds // 60}m"
    return f"{safe_seconds}s"


def _tidb_tiup_base_command(config: BenchmarkConfig) -> List[str]:
    command = [
        _find_tiup(),
        "bench",
        "tpcc",
        "-H",
        config.host,
        "-P",
        str(config.port),
        "-D",
        config.database,
        "-U",
        config.user,
        "--warehouses",
        str(config.table_size),
    ]
    if config.password:
        command.extend(["-p", config.password])
    if config.extra_options.strip():
        command.extend(shlex.split(config.extra_options))
    return command


def _run_tidb_tiup_stage(
    config: BenchmarkConfig,
    action: str,
    threads: Optional[int] = None,
    duration_seconds: Optional[int] = None,
    timeout_seconds: int = 3600,
    allow_failure: bool = False,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> str:
    command = _tidb_tiup_base_command(config)
    if threads is not None:
        command.extend(["--threads", str(threads)])
    if duration_seconds is not None:
        command.extend(["--time", _tidb_tiup_duration(duration_seconds)])
    command.append(action)
    return _run_command(
        command,
        timeout_seconds=timeout_seconds,
        allow_failure=allow_failure,
        log_callback=log_callback,
        log_prefix=f"[tiup-tpcc:{action}] ",
        log_label=f"TiDB TPC-C {action}",
        secrets=[config.password],
        cancel_callback=cancel_callback,
        env={"TIUP_HOME": os.environ.get("TIUP_HOME", "/root/.tiup")},
    )


def _parse_tidb_tiup_tpcc_output(concurrency: int, output: str) -> ConcurrencyResult:
    summary_line = ""
    for line in output.splitlines():
        upper_line = line.upper()
        if "NEW_ORDER" in upper_line and "TPM" in upper_line and ("SUMMARY" in upper_line or not summary_line):
            summary_line = line
            if "SUMMARY" in upper_line:
                break

    parse_source = summary_line or output
    tpmc = _extract_float(r"\btpmC\b\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", output, flags=re.IGNORECASE)
    if tpmc <= 0:
        tpmc = _extract_float(r"\bTPM\b\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", parse_source, flags=re.IGNORECASE)
    if tpmc <= 0:
        tpmc = _extract_float(r"NEW[_ -]?ORDER.*?\bTPM\b\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", output, flags=re.IGNORECASE)

    total_transactions = _extract_int(r"\bCount\b\s*[:=]\s*(\d+)", parse_source, flags=re.IGNORECASE)
    elapsed_seconds = _extract_float(r"\bTakes\(s\)\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", parse_source, flags=re.IGNORECASE)
    avg_latency_ms = _extract_float(r"\bAvg\(ms\)\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", parse_source, flags=re.IGNORECASE)
    p95_latency_ms = _extract_float(r"\b95th\(ms\)\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", parse_source, flags=re.IGNORECASE)
    if p95_latency_ms <= 0:
        p95_latency_ms = _extract_float(r"\bP95\b.*?([0-9]+(?:\.[0-9]+)?)\s*ms", output, flags=re.IGNORECASE)
    min_latency_ms = _extract_float(r"\bMin\(ms\)\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", parse_source, flags=re.IGNORECASE)
    max_latency_ms = _extract_float(r"\bMax\(ms\)\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", parse_source, flags=re.IGNORECASE)

    if tpmc <= 0:
        raise BenchmarkValidationError(f"未能解析 tiup bench TPC-C 输出，请检查 TiDB 连接或 TiUP bench 执行结果。\n{output[:800]}")
    if total_transactions <= 0 and elapsed_seconds > 0:
        total_transactions = int(round(tpmc * elapsed_seconds / 60.0))

    success_rate = 100.0
    return ConcurrencyResult(
        concurrency=concurrency,
        elapsed_seconds=elapsed_seconds,
        total_transactions=total_transactions,
        total_queries=0,
        total_events=total_transactions,
        read_queries=0,
        write_queries=total_transactions,
        other_queries=0,
        failed_requests=0,
        qps=round(tpmc, 2),
        tps=round(tpmc, 2),
        avg_latency_ms=avg_latency_ms,
        p95_latency_ms=p95_latency_ms,
        min_latency_ms=min_latency_ms,
        max_latency_ms=max_latency_ms,
        success_rate=success_rate,
        baseline_label=_classify_baseline(success_rate, p95_latency_ms),
        sample_errors=_extract_sample_errors(output),
        raw_output=output,
    )


def _run_tidb_tiup_tpcc_benchmark(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> BenchmarkSummary:
    _emit_log(log_callback, f"已接收测试任务: TiDB tiup bench TPC-C / {config.host}:{config.port}/{config.database}")
    _emit_log(
        log_callback,
        f"线程配置: {','.join(str(value) for value in sorted(set(config.threads_values)))} | 仓库数: {config.table_size} | 造数并发: {config.table_count} | 时长: {config.duration_seconds}s",
    )

    results: List[ConcurrencyResult] = []
    try:
        _check_cancel(cancel_callback)
        _check_mysql_connectivity(config, log_callback=log_callback)
        if config.auto_prepare:
            _emit_log(log_callback, "TiDB TPC-C 自动准备开启，先清理旧 TPC-C 数据，再执行 prepare。")
            _run_tidb_tiup_stage(
                config,
                "cleanup",
                timeout_seconds=1800,
                allow_failure=True,
                log_callback=log_callback,
                cancel_callback=cancel_callback,
            )
            prepare_timeout = max(3600, min(21600, config.table_size * 60 + 1800))
            _run_tidb_tiup_stage(
                config,
                "prepare",
                threads=config.table_count,
                timeout_seconds=prepare_timeout,
                log_callback=log_callback,
                cancel_callback=cancel_callback,
            )

        if config.warmup_seconds:
            warmup_threads = max(sorted(set(config.threads_values)))
            _emit_log(log_callback, f"TiDB TPC-C 预热开始: threads={warmup_threads}, time={config.warmup_seconds}s。")
            _run_tidb_tiup_stage(
                config,
                "run",
                threads=warmup_threads,
                duration_seconds=config.warmup_seconds,
                timeout_seconds=config.warmup_seconds + 600,
                log_callback=log_callback,
                cancel_callback=cancel_callback,
            )

        for concurrency in sorted(set(config.threads_values)):
            _check_cancel(cancel_callback)
            _emit_log(log_callback, f"并发 {concurrency}: 开始执行 TiDB tiup bench TPC-C run...")
            output = _run_tidb_tiup_stage(
                config,
                "run",
                threads=concurrency,
                duration_seconds=config.duration_seconds,
                timeout_seconds=config.duration_seconds + 600,
                log_callback=log_callback,
                cancel_callback=cancel_callback,
            )
            result = _parse_tidb_tiup_tpcc_output(concurrency, output)
            results.append(result)
            _emit_log(log_callback, f"并发 {concurrency}: TiDB TPC-C 完成，tpmC={result.qps}，P95={result.p95_latency_ms}ms")
    finally:
        if config.auto_cleanup:
            _run_tidb_tiup_stage(
                config,
                "cleanup",
                timeout_seconds=1800,
                allow_failure=True,
                log_callback=log_callback,
                cancel_callback=cancel_callback,
            )

    _emit_log(log_callback, "所有 TiDB TPC-C 压测线程已执行完成，正在整理结果。")
    return BenchmarkSummary(
        report_title=config.report_title,
        executed_at=_beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
        target={
            "db_type": config.db_type,
            "host": config.host,
            "port": config.port,
            "database": config.database,
            "user": config.user,
            "collection_name": "",
        },
        parameters={
            "benchmark_engine": "tiup-bench-tpcc",
            "tidb_engine": config.tidb_engine,
            "workload": "tprocc",
            "duration_seconds": config.duration_seconds,
            "warmup_seconds": config.warmup_seconds,
            "report_interval": 0,
            "table_count": config.table_count,
            "table_size": config.table_size,
            "operation_count": 0,
            "auth_database": "",
            "collection_name": "",
            "ssl_enabled": config.ssl_enabled,
            "ssl_verify": config.ssl_verify,
            "auto_prepare": config.auto_prepare,
            "auto_cleanup": config.auto_cleanup,
            "threads_values": sorted(set(config.threads_values)),
        },
        results=results,
        recommended_baseline=_select_recommended_baseline(results),
    )


def _run_mysql_benchmark(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> BenchmarkSummary:
    _emit_log(log_callback, f"已接收测试任务: {config.db_type} / {config.host}:{config.port}/{config.database}")
    _emit_log(
        log_callback,
        f"线程配置: {','.join(str(value) for value in sorted(set(config.threads_values)))} | 时长: {config.duration_seconds}s",
    )

    results: List[ConcurrencyResult] = []
    try:
        _check_cancel(cancel_callback)
        _check_mysql_connectivity(config, log_callback=log_callback)

        dataset_state = _mysql_dataset_cache_state(config)
        if config.auto_prepare:
            if dataset_state.get("prepared"):
                prepared_at = dataset_state.get("prepared_at", "-")
                _emit_log(log_callback, f"检测到当前库的压测数据已准备完成（{prepared_at}），跳过重复 prepare。")
            else:
                _run_prepare(config, log_callback=log_callback, cancel_callback=cancel_callback)
                dataset_state = _mysql_dataset_cache_state(config)

        if config.warmup_seconds:
            warmed_seconds = _safe_int(dataset_state.get("warmup_seconds"))
            if warmed_seconds >= config.warmup_seconds:
                warmed_at = dataset_state.get("warmed_at", "-")
                _emit_log(
                    log_callback,
                    f"检测到当前库已完成 {warmed_seconds} 秒数据预热（{warmed_at}），跳过重复预热，直接进入正式测试。",
                )
            else:
                warmup_threads = max(sorted(set(config.threads_values)))
                _run_mysql_warmup(
                    config,
                    warmup_threads,
                    log_callback=log_callback,
                    cancel_callback=cancel_callback,
                )

        for concurrency in sorted(set(config.threads_values)):
            _check_cancel(cancel_callback)
            test_phase = _mysql_test_phase_label(config.workload)
            command = _common_command(config, log_callback=log_callback, workload=config.workload) + [
                f"--threads={concurrency}",
                f"--time={config.duration_seconds}",
                "--events=0",
                f"--report-interval={config.report_interval}",
                "run",
            ]
            _emit_log(log_callback, f"线程 {concurrency}: 开始执行 {test_phase} 测试...")
            output = _run_command(
                command,
                timeout_seconds=config.duration_seconds + 600,
                log_callback=log_callback,
                log_prefix=f"[sysbench:{concurrency}] ",
                log_label=f"{concurrency}线程{test_phase}测试",
                secrets=[config.password],
                cancel_callback=cancel_callback,
                env=_mysql_sysbench_env(log_callback),
            )
            result = _parse_sysbench_output(concurrency, output)
            results.append(result)
            _emit_log(
                log_callback,
                f"线程 {concurrency}: {test_phase}测试完成，{_workload_metric_log_text(config.workload, result)}，P95={result.p95_latency_ms}ms，成功率={result.success_rate}%",
            )
    finally:
        if config.auto_cleanup:
            _run_cleanup(config, log_callback=log_callback, cancel_callback=cancel_callback)

    _emit_log(log_callback, "所有压测线程已执行完成，正在整理结果。")

    return BenchmarkSummary(
        report_title=config.report_title,
        executed_at=_beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
        target={
            "db_type": config.db_type,
            "host": config.host,
            "port": config.port,
            "database": config.database,
            "user": config.user,
            "collection_name": "",
        },
        parameters={
            "benchmark_engine": "sysbench",
            "workload": config.workload,
            "duration_seconds": config.duration_seconds,
            "warmup_seconds": config.warmup_seconds,
            "report_interval": config.report_interval,
            "table_count": config.table_count,
            "table_size": config.table_size,
            "operation_count": 0,
            "auth_database": "",
            "collection_name": "",
            "ssl_enabled": config.ssl_enabled,
            "ssl_verify": config.ssl_verify,
            "auto_prepare": config.auto_prepare,
            "auto_cleanup": config.auto_cleanup,
            "threads_values": sorted(set(config.threads_values)),
        },
        results=results,
        recommended_baseline=_select_recommended_baseline(results),
    )






def _find_kafka_home() -> Path:
    candidates = [
        Path(os.environ.get("KAFKA_HOME", "")),
        BASE_DIR / "vendor" / "kafka",
        Path("/app/tools/kafka"),
        Path("/opt/kafka"),
        Path("/usr/local/kafka"),
    ]
    for candidate in candidates:
        if not str(candidate):
            continue
        if (candidate / "bin" / "kafka-producer-perf-test.sh").exists() and (candidate / "bin" / "kafka-consumer-perf-test.sh").exists():
            return candidate
    raise BenchmarkValidationError("未找到 Kafka 官方压测工具，请在镜像内设置 KAFKA_HOME 或安装 /app/tools/kafka。")


def _kafka_tool(name: str) -> str:
    path = _find_kafka_home() / "bin" / name
    if not path.exists():
        raise BenchmarkValidationError(f"未找到 Kafka 工具: {path}")
    return str(path)


def _kafka_bootstrap_servers(config: BenchmarkConfig) -> str:
    raw = str(config.kafka_bootstrap_servers or "").strip()
    if raw:
        return raw
    host = str(config.host or "").strip()
    port = int(config.port or 9092)
    if "," in host:
        values: List[str] = []
        for item in host.split(","):
            item = item.strip()
            if not item:
                continue
            values.append(item if ":" in item else f"{item}:{port}")
        return ",".join(values)
    return host if ":" in host else f"{host}:{port}"


def _kafka_extra_args(config: BenchmarkConfig) -> List[str]:
    return shlex.split(config.extra_options) if str(config.extra_options or "").strip() else []


def _kafka_java_env() -> Dict[str, str]:
    env: Dict[str, str] = {}
    for candidate in ("/usr/lib/jvm/java-11-openjdk", "/usr/lib/jvm/java-11", "/usr/lib/jvm/jre-11"):
        if Path(candidate).exists():
            env["JAVA_HOME"] = candidate
            break
    return env


def _kafka_topic_command(config: BenchmarkConfig, action: str) -> List[str]:
    command = [
        _kafka_tool("kafka-topics.sh"),
        "--bootstrap-server",
        _kafka_bootstrap_servers(config),
        action,
        "--topic",
        config.database,
    ]
    if action == "--create":
        command.extend([
            "--if-not-exists",
            "--partitions",
            str(config.table_count),
            "--replication-factor",
            str(config.kafka_replication_factor),
        ])
    elif action == "--delete":
        command.append("--if-exists")
    return command


def _kafka_create_topic(config: BenchmarkConfig, log_callback: Optional[LogCallback], cancel_callback: Optional[CancelCallback] = None) -> None:
    _run_command(
        _kafka_topic_command(config, "--create"),
        timeout_seconds=90,
        allow_failure=False,
        log_callback=log_callback,
        log_label="kafka-topic-create",
        cancel_callback=cancel_callback,
        env=_kafka_java_env(),
    )


def _kafka_delete_topic(config: BenchmarkConfig, log_callback: Optional[LogCallback], cancel_callback: Optional[CancelCallback] = None) -> None:
    _run_command(
        _kafka_topic_command(config, "--delete"),
        timeout_seconds=90,
        allow_failure=True,
        log_callback=log_callback,
        log_label="kafka-topic-delete",
        cancel_callback=cancel_callback,
        env=_kafka_java_env(),
    )


def _check_kafka_connectivity(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    _emit_log(log_callback, "开始检查 Kafka 连接性...")
    _run_command(
        [_kafka_tool("kafka-topics.sh"), "--bootstrap-server", _kafka_bootstrap_servers(config), "--list"],
        timeout_seconds=30,
        allow_failure=False,
        log_callback=log_callback,
        log_label="kafka-topics-list",
        env=_kafka_java_env(),
    )
    _emit_log(log_callback, f"Kafka 连接检测通过: {_kafka_bootstrap_servers(config)}")


def _kafka_producer_command(config: BenchmarkConfig, records: int) -> List[str]:
    command = [
        _kafka_tool("kafka-producer-perf-test.sh"),
        "--topic",
        config.database,
        "--num-records",
        str(max(1, int(records))),
        "--record-size",
        str(config.table_size),
        "--throughput",
        str(config.kafka_throughput),
        "--producer-props",
        f"bootstrap.servers={_kafka_bootstrap_servers(config)}",
        f"acks={config.kafka_acks or '1'}",
    ]
    command.extend(_kafka_extra_args(config))
    return command


def _kafka_consumer_command(config: BenchmarkConfig, concurrency: int) -> List[str]:
    command = [
        _kafka_tool("kafka-consumer-perf-test.sh"),
        "--bootstrap-server",
        _kafka_bootstrap_servers(config),
        "--topic",
        config.database,
        "--messages",
        str(config.operation_count),
        "--threads",
        str(concurrency),
        "--group",
        f"qdbmark-{os.getpid()}-{concurrency}-{int(monotonic() * 1000)}",
        "--reporting-interval",
        str(max(1, config.report_interval) * 1000),
    ]
    command.extend(_kafka_extra_args(config))
    return command


def _run_parallel_kafka_producers(
    config: BenchmarkConfig,
    concurrency: int,
    log_callback: Optional[LogCallback],
    cancel_callback: Optional[CancelCallback],
) -> tuple[str, float]:
    records_total = int(config.operation_count)
    base_records = records_total // concurrency
    remainder = records_total % concurrency
    commands = [_kafka_producer_command(config, base_records + (1 if index < remainder else 0)) for index in range(concurrency)]
    secrets = [config.password] if config.password else []
    for index, command in enumerate(commands, start=1):
        _emit_log(log_callback, f"kafka-producer[{index}/{concurrency}]启动: {_masked_command(command, secrets)}")
    started = monotonic()
    processes = [
        subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env={**os.environ, **_kafka_java_env()})
        for command in commands
    ]
    output_parts: List[str] = []
    deadline = started + max(120, int(config.duration_seconds or 0) + int(config.warmup_seconds or 0) + 1800)
    while True:
        if cancel_callback and cancel_callback():
            for process in processes:
                if process.poll() is None:
                    process.kill()
            raise BenchmarkCancelledError("测试已终止。")
        if monotonic() > deadline:
            for process in processes:
                if process.poll() is None:
                    process.kill()
            raise BenchmarkValidationError("Kafka producer perf 执行超时。")
        if all(process.poll() is not None for process in processes):
            break
        sleep(0.5)
    failed_outputs: List[str] = []
    for index, process in enumerate(processes, start=1):
        out, _ = process.communicate(timeout=5)
        out = (out or "").strip()
        if out:
            output_parts.append(out)
            for line in out.splitlines()[-8:]:
                if line.strip():
                    _emit_log(log_callback, f"[kafka-producer:{index}] {line}")
        if process.returncode != 0:
            failed_outputs.append(out or f"producer {index} exit {process.returncode}")
    if failed_outputs:
        raise BenchmarkValidationError("\n".join(failed_outputs[-3:]))
    return "\n".join(output_parts), monotonic() - started


def _parse_kafka_producer_result(output: str, concurrency: int, elapsed_seconds: float) -> ConcurrencyResult:
    pattern = re.compile(
        r"(?P<records>[\d,]+)\s+records sent,\s+"
        r"(?P<records_sec>[\d.]+)\s+records/sec\s+\((?P<mb_sec>[\d.]+)\s+MB/sec\).*?"
        r"(?P<avg>[\d.]+)\s+ms avg latency,\s+(?P<max>[\d.]+)\s+ms max latency.*?"
        r"(?P<p95>[\d.]+)\s+ms 95th",
        re.IGNORECASE,
    )
    matches = list(pattern.finditer(output or ""))
    if not matches:
        raise BenchmarkValidationError("未能解析 Kafka producer perf 输出，请检查工具版本或命令参数。")
    total_events = 0
    records_sec = 0.0
    mb_sec = 0.0
    avg_values: List[float] = []
    max_latency = 0.0
    p95_latency = 0.0
    for match in matches[-concurrency:]:
        total_events += int(match.group("records").replace(",", ""))
        records_sec += float(match.group("records_sec"))
        mb_sec += float(match.group("mb_sec"))
        avg_values.append(float(match.group("avg")))
        max_latency = max(max_latency, float(match.group("max")))
        p95_latency = max(p95_latency, float(match.group("p95")))
    avg_latency = sum(avg_values) / len(avg_values) if avg_values else 0.0
    return ConcurrencyResult(
        concurrency=concurrency,
        elapsed_seconds=round(elapsed_seconds, 2),
        total_transactions=0,
        total_queries=total_events,
        total_events=total_events,
        read_queries=0,
        write_queries=total_events,
        other_queries=0,
        failed_requests=0,
        qps=round(records_sec, 2),
        tps=round(mb_sec, 2),
        avg_latency_ms=round(avg_latency, 2),
        p95_latency_ms=round(p95_latency, 2),
        min_latency_ms=0.0,
        max_latency_ms=round(max_latency, 2),
        success_rate=100.0,
        baseline_label="推荐基线",
        sample_errors=[],
        raw_output=output,
    )


def _parse_kafka_consumer_result(output: str, concurrency: int, elapsed_seconds: float) -> ConcurrencyResult:
    header: List[str] = []
    rows: List[List[str]] = []
    for row in csv.reader((output or "").splitlines()):
        if not row:
            continue
        values = [item.strip() for item in row]
        if "MB.sec" in values and "nMsg.sec" in values:
            header = values
            continue
        if header and len(values) >= len(header):
            rows.append(values)
    if not header or not rows:
        raise BenchmarkValidationError("未能解析 Kafka consumer perf 输出，请检查 Topic 中是否有可消费消息。")
    data = dict(zip(header, rows[-1]))
    total_events = int(float(data.get("data.consumed.in.nMsg", data.get("fetch.nMsg", 0)) or 0))
    records_sec = float(data.get("nMsg.sec", data.get("fetch.nMsg.sec", 0)) or 0)
    mb_sec = float(data.get("MB.sec", data.get("fetch.MB.sec", 0)) or 0)
    fetch_ms = float(data.get("fetch.time.ms", 0) or 0)
    return ConcurrencyResult(
        concurrency=concurrency,
        elapsed_seconds=round(elapsed_seconds, 2),
        total_transactions=0,
        total_queries=total_events,
        total_events=total_events,
        read_queries=total_events,
        write_queries=0,
        other_queries=0,
        failed_requests=0,
        qps=round(records_sec, 2),
        tps=round(mb_sec, 2),
        avg_latency_ms=0.0,
        p95_latency_ms=0.0,
        min_latency_ms=0.0,
        max_latency_ms=round(fetch_ms, 2),
        success_rate=100.0,
        baseline_label="推荐基线",
        sample_errors=[],
        raw_output=output,
    )


def _run_kafka_benchmark(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> BenchmarkSummary:
    config.validate()
    _emit_log(log_callback, f"Kafka 官方压测启动: workload={config.workload}, bootstrap={_kafka_bootstrap_servers(config)}, topic={config.database}")
    if config.auto_prepare:
        _emit_log(log_callback, "Kafka 测试前准备: 创建 Topic。")
        _kafka_create_topic(config, log_callback, cancel_callback)
        if config.workload == "consumer":
            _emit_log(log_callback, "Kafka consumer 压测需要 Topic 中已有消息，正在先写入测试消息。")
            prepare_output, prepare_elapsed = _run_parallel_kafka_producers(config, 1, log_callback, cancel_callback)
            _parse_kafka_producer_result(prepare_output, 1, prepare_elapsed)
    if config.warmup_seconds > 0:
        _emit_log(log_callback, f"Kafka 预热等待 {config.warmup_seconds} 秒。")
        deadline = monotonic() + config.warmup_seconds
        while monotonic() < deadline:
            _check_cancel(cancel_callback)
            sleep(min(1.0, deadline - monotonic()))

    results: List[ConcurrencyResult] = []
    for concurrency in sorted(set(config.threads_values)):
        _check_cancel(cancel_callback)
        if config.workload == "producer":
            _emit_log(log_callback, f"Kafka producer perf 开始: 并发客户端={concurrency}, 消息数={config.operation_count}, 消息大小={config.table_size} bytes")
            output, elapsed = _run_parallel_kafka_producers(config, concurrency, log_callback, cancel_callback)
            result = _parse_kafka_producer_result(output, concurrency, elapsed)
        else:
            command = _kafka_consumer_command(config, concurrency)
            started = monotonic()
            output = _run_command(
                command,
                timeout_seconds=max(120, int(config.duration_seconds or 0) + int(config.warmup_seconds or 0) + 1800),
                allow_failure=False,
                log_callback=log_callback,
                log_label=f"kafka-consumer-perf-{concurrency}",
                cancel_callback=cancel_callback,
                env=_kafka_java_env(),
            )
            result = _parse_kafka_consumer_result(output, concurrency, monotonic() - started)
        results.append(result)
        _emit_log(log_callback, f"Kafka {config.workload} 结果: 并发={concurrency}, records/s={result.qps}, MB/s={result.tps}")

    if config.auto_cleanup:
        _emit_log(log_callback, "Kafka 测试后清理: 删除测试 Topic。")
        _kafka_delete_topic(config, log_callback, cancel_callback)

    return BenchmarkSummary(
        report_title=config.report_title,
        executed_at=_beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
        target={
            "db_type": "Kafka",
            "host": _kafka_bootstrap_servers(config),
            "port": config.port,
            "database": config.database,
            "user": "-",
            "collection_name": config.database,
        },
        parameters={
            "benchmark_engine": "kafka-producer-perf-test" if config.workload == "producer" else "kafka-consumer-perf-test",
            "workload": config.workload,
            "duration_seconds": config.duration_seconds,
            "warmup_seconds": config.warmup_seconds,
            "report_interval": config.report_interval,
            "table_count": config.table_count,
            "table_size": config.table_size,
            "operation_count": config.operation_count,
            "auth_database": "",
            "collection_name": config.database,
            "ssl_enabled": False,
            "ssl_verify": False,
            "auto_prepare": config.auto_prepare,
            "auto_cleanup": config.auto_cleanup,
            "threads_values": sorted(set(config.threads_values)),
            "kafka_bootstrap_servers": _kafka_bootstrap_servers(config),
            "kafka_acks": config.kafka_acks,
            "kafka_throughput": config.kafka_throughput,
            "kafka_replication_factor": config.kafka_replication_factor,
        },
        results=results,
        recommended_baseline=_select_recommended_baseline(results),
    )


def _run_kafka_cleanup(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> None:
    config.validate()
    _kafka_delete_topic(config, log_callback, cancel_callback)


def _find_rocketmq_home() -> Path:
    candidates: List[Path] = []
    env_home = os.environ.get("ROCKETMQ_HOME")
    if env_home:
        candidates.append(Path(env_home))
    candidates.extend([
        BASE_DIR / "vendor" / "rocketmq",
        BASE_DIR.parent / "tools" / "rocketmq",
        Path("/app/tools/rocketmq"),
        Path("/opt/rocketmq"),
        Path("/usr/local/rocketmq"),
    ])
    for candidate in candidates:
        tools_sh = candidate / "bin" / "tools.sh"
        mqadmin = candidate / "bin" / "mqadmin"
        if tools_sh.exists() and mqadmin.exists():
            return candidate
    raise BenchmarkValidationError("未找到 RocketMQ 官方工具，请设置 ROCKETMQ_HOME，或将 rocketmq binary 解压到 /app/tools/rocketmq。")


def _rocketmq_task_home(config: Optional[BenchmarkConfig] = None) -> Path:
    base = _find_rocketmq_home()
    if config is None:
        return base
    task_home = Path(tempfile.mkdtemp(prefix="qdbmark-rocketmq-"))
    shutil.copytree(base / "bin", task_home / "bin", dirs_exist_ok=True)
    if (base / "lib").exists():
        os.symlink(base / "lib", task_home / "lib", target_is_directory=True)
    shutil.copytree(base / "conf", task_home / "conf", dirs_exist_ok=True)
    tools_yml = task_home / "conf" / "tools.yml"
    if config.user and config.password:
        tools_yml.write_text(f"accessKey: {config.user}\nsecretKey: {config.password}\n", encoding="utf-8")
    elif tools_yml.exists():
        tools_yml.unlink()
    return task_home


def _rocketmq_tool(name: str, home: Optional[Path] = None) -> str:
    path = (home or _find_rocketmq_home()) / "bin" / name
    if not path.exists():
        raise BenchmarkValidationError(f"RocketMQ 工具不存在: {path}")
    return str(path)


def _rocketmq_namesrv(config: BenchmarkConfig) -> str:
    raw = str(config.kafka_bootstrap_servers or config.host or "").strip()
    if not raw:
        raise BenchmarkValidationError("RocketMQ NameServer 地址不能为空。")
    nodes = [item.strip() for item in raw.replace(";", ",").split(",") if item.strip()]
    if not nodes:
        raise BenchmarkValidationError("RocketMQ NameServer 地址不能为空。")
    result: List[str] = []
    for node in nodes:
        result.append(node if ":" in node else f"{node}:{config.port or 9876}")
    return ";".join(result)


def _rocketmq_java_env(home: Optional[Path] = None) -> Dict[str, str]:
    env = _kafka_java_env()
    rocketmq_home = home or _find_rocketmq_home()
    env.update({
        "ROCKETMQ_HOME": str(rocketmq_home),
        "NAMESRV_ADDR": "",
        "JAVA_OPT_EXT": os.environ.get("JAVA_OPT_EXT", "-Xms256m -Xmx512m"),
    })
    return env


def _rocketmq_acl_args(config: BenchmarkConfig) -> List[str]:
    if config.user and config.password:
        return ["-a", "true", "-ak", config.user, "-sk", config.password]
    return []


def _rocketmq_error_summary(output: str) -> str:
    text = str(output or "")
    if not text.strip():
        return ""
    acl_match = re.search(r"AclException:\s*([^\n]+)", text, flags=re.IGNORECASE)
    if acl_match:
        detail = acl_match.group(1).strip()
        return f"RocketMQ ACL 认证失败: {detail}。请确认页面已填写正确的 AccessKey/SecretKey，或关闭集群 ACL 后重试。"
    broker_match = re.search(r"MQBrokerException:\s*CODE:\s*([^\n]+)", text, flags=re.IGNORECASE)
    if broker_match:
        return f"RocketMQ Broker 返回异常: {broker_match.group(1).strip()}"
    command_match = re.search(r"SubCommandException:\s*([^\n]+)", text, flags=re.IGNORECASE)
    if command_match:
        return f"RocketMQ 管理命令执行失败: {command_match.group(1).strip()}"
    if re.search(r"\b(Exception|ERROR|No route info|connect to .* failed|timeout)\b", text, flags=re.IGNORECASE):
        for line in text.splitlines():
            clean = line.strip()
            if re.search(r"Exception|ERROR|No route info|failed|timeout", clean, flags=re.IGNORECASE):
                return f"RocketMQ 连接失败: {clean}"
    return ""


def _rocketmq_recent_client_error(topic: str = "", access_key: str = "") -> str:
    log_dir = Path("/root/logs/rocketmqlogs")
    candidates = [
        log_dir / "broker_default.log",
        log_dir / "broker.log",
        log_dir / "tools_default.log",
        log_dir / "client.log",
    ]
    wanted = ["AclException", "MQBrokerException", "MQClientException", "No route info", "Send Exception"]
    for log_path in candidates:
        if not log_path.exists():
            continue
        try:
            size = log_path.stat().st_size
            with log_path.open("rb") as handle:
                handle.seek(max(0, size - 16 * 1024 * 1024))
                text = handle.read().decode("utf-8", "replace")
        except OSError:
            continue
        lines = [line.strip() for line in text.splitlines() if any(marker in line for marker in wanted)]
        if access_key:
            keyed = [line for line in lines if access_key in line]
            if not keyed:
                continue
            lines = keyed
        if topic:
            topic_lines = [line for line in lines if topic in line]
            if topic_lines:
                lines = topic_lines + lines
        summary = _rocketmq_error_summary("\n".join(lines[-80:]))
        if summary:
            return summary
    return ""


def _rocketmq_benchmark_failure_detail(output: str, config: Optional[BenchmarkConfig], operation: str, failed: int = 0) -> str:
    detail = _rocketmq_error_summary(output)
    if not detail and config is not None:
        detail = _rocketmq_recent_client_error(config.database, config.user or "")
    if detail:
        return f"RocketMQ {operation} 失败: {detail}"
    if failed > 0:
        return f"RocketMQ {operation} 失败: benchmark 返回失败数 {failed}，请检查 Topic 权限、Broker ACL、NameServer 路由和 Broker 状态。"
    return ""


def _check_rocketmq_connectivity(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    namesrv = _rocketmq_namesrv(config)
    task_home = _rocketmq_task_home(config)
    _emit_log(log_callback, "开始检查 RocketMQ 连接性...")
    _emit_log(log_callback, f"连接检测启动: mqadmin clusterList -> namesrv={namesrv}")
    command = [_rocketmq_tool("mqadmin", task_home), "clusterList", "-n", namesrv]
    output = _run_command(
        command,
        timeout_seconds=30,
        allow_failure=False,
        log_callback=log_callback,
        log_label="rocketmq-clusterList",
        secrets=[config.password],
        env=_rocketmq_java_env(task_home),
    )
    rocketmq_error = _rocketmq_error_summary(output)
    if rocketmq_error:
        raise BenchmarkValidationError(rocketmq_error)
    _emit_log(log_callback, "RocketMQ 连接检测通过。")


def _rocketmq_report_interval_ms(config: BenchmarkConfig) -> int:
    return max(1000, int(config.report_interval or 1) * 1000)


def _rocketmq_producer_command(config: BenchmarkConfig, concurrency: int, quantity: Optional[int] = None) -> List[str]:
    task_home = _rocketmq_task_home(config)
    return [
        _rocketmq_tool("tools.sh", task_home),
        "org.apache.rocketmq.example.benchmark.Producer",
        "-n", _rocketmq_namesrv(config),
        "-t", config.database,
        "-w", str(concurrency),
        "-s", str(config.table_size),
        "-q", str(quantity or config.operation_count),
        "-ri", str(_rocketmq_report_interval_ms(config)),
        *_rocketmq_acl_args(config),
    ]


def _rocketmq_consumer_command(config: BenchmarkConfig, concurrency: int) -> List[str]:
    task_home = _rocketmq_task_home(config)
    group = f"qdbmark-{re.sub(r'[^A-Za-z0-9_-]+', '-', config.database)[:48]}"
    return [
        _rocketmq_tool("tools.sh", task_home),
        "org.apache.rocketmq.example.benchmark.Consumer",
        "-n", _rocketmq_namesrv(config),
        "-t", config.database,
        "-w", str(concurrency),
        "-g", group,
        "-ri", str(_rocketmq_report_interval_ms(config)),
        *_rocketmq_acl_args(config),
    ]


def _run_rocketmq_command_for_duration(
    command: List[str],
    duration_seconds: int,
    log_callback: Optional[LogCallback],
    log_label: str,
    cancel_callback: Optional[CancelCallback],
    secrets: Optional[List[str]] = None,
) -> Tuple[str, float]:
    started = monotonic()
    output_lines: List[str] = []
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        env={**os.environ, **_rocketmq_java_env()},
    )
    deadline = started + max(1, duration_seconds)
    try:
        assert process.stdout is not None
        while True:
            _check_cancel(cancel_callback)
            if process.poll() is not None:
                break
            line = process.stdout.readline()
            if line:
                line = line.rstrip("\n")
                output_lines.append(line)
                _emit_log(log_callback, f"[{log_label}] {_mask_secrets(line, secrets or [])}")
            if monotonic() >= deadline:
                break
            if not line:
                sleep(0.2)
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
        remainder = process.stdout.read() if process.stdout else ""
        if remainder:
            for line in remainder.splitlines():
                output_lines.append(line)
                _emit_log(log_callback, f"[{log_label}] {_mask_secrets(line, secrets or [])}")
    finally:
        if process.poll() is None:
            process.kill()
    return "\n".join(output_lines), monotonic() - started


def _parse_rocketmq_producer_result(output: str, concurrency: int, elapsed_seconds: float, message_size: int, config: Optional[BenchmarkConfig] = None) -> ConcurrencyResult:
    complete_matches = list(re.finditer(r"\[Complete\].*?Send\s+Total\s*:?\s*(?P<total>[\d.]+).*?Send\s+TPS\s*:?\s*(?P<tps>[\d.]+)", output or "", flags=re.IGNORECASE))
    current_matches = list(re.finditer(r"Send\s+TPS\s*:?\s*(?P<tps>[\d.]+).*?Max\s+RT\(ms\)\s*:?\s*(?P<max>[\d.]+).*?Average\s+RT\(ms\)\s*:?\s*(?P<avg>[\d.]+).*?Send\s+Failed\s*:?\s*(?P<failed>\d+).*?Response\s+Failed\s*:?\s*(?P<response_failed>\d+)", output or "", flags=re.IGNORECASE))
    if complete_matches:
        last = complete_matches[-1]
        total_events = int(float(last.group("total")))
        records_sec = float(last.group("tps"))
    elif current_matches:
        last = current_matches[-1]
        records_sec = float(last.group("tps"))
        total_events = int(round(records_sec * max(elapsed_seconds, 0)))
    else:
        detail = _rocketmq_benchmark_failure_detail(output, config, "Producer")
        raise BenchmarkValidationError(detail or "未能解析 RocketMQ Producer benchmark 输出，请检查 NameServer、Topic 和 ACL。")
    avg_latency = float(current_matches[-1].group("avg")) if current_matches else 0.0
    max_latency = float(current_matches[-1].group("max")) if current_matches else 0.0
    failed = 0
    if current_matches:
        last = current_matches[-1]
        failed = int(last.group("failed")) + int(last.group("response_failed"))
    if failed > 0:
        detail = _rocketmq_benchmark_failure_detail(output, config, "Producer", failed)
        raise BenchmarkValidationError(detail)
    success_rate = 100.0 if total_events <= 0 else round(max(0.0, (total_events - failed) / total_events * 100.0), 2)
    mb_sec = records_sec * max(message_size, 1) / 1024 / 1024
    return ConcurrencyResult(concurrency=concurrency, elapsed_seconds=round(elapsed_seconds, 2), total_transactions=total_events, total_queries=total_events, total_events=total_events, read_queries=0, write_queries=total_events, other_queries=0, failed_requests=failed, qps=round(records_sec, 2), tps=round(mb_sec, 2), avg_latency_ms=round(avg_latency, 2), p95_latency_ms=0.0, min_latency_ms=0.0, max_latency_ms=round(max_latency, 2), success_rate=success_rate, baseline_label=_classify_baseline(success_rate, 0.0), sample_errors=_extract_sample_errors(output), raw_output=output)


def _parse_rocketmq_consumer_result(output: str, concurrency: int, elapsed_seconds: float, message_size: int, config: Optional[BenchmarkConfig] = None) -> ConcurrencyResult:
    matches = list(re.finditer(r"Consume\s+TPS\s*:?\s*(?P<tps>[\d.]+).*?AVG\(B2C\)\s+RT\(ms\)\s*:?\s*(?P<avg_b2c>[\d.]+).*?MAX\(B2C\)\s+RT\(ms\)\s*:?\s*(?P<max_b2c>[\d.]+).*?AVG\(S2C\)\s+RT\(ms\)\s*:?\s*(?P<avg_s2c>[\d.]+).*?MAX\(S2C\)\s+RT\(ms\)\s*:?\s*(?P<max_s2c>[\d.]+).*?Consume\s+Fail\s*:?\s*(?P<failed>\d+)", output or "", flags=re.IGNORECASE))
    if not matches:
        detail = _rocketmq_benchmark_failure_detail(output, config, "Consumer")
        raise BenchmarkValidationError(detail or "未能解析 RocketMQ Consumer benchmark 输出，请确认 Topic 中有可消费消息。")
    last = matches[-1]
    records_sec = float(last.group("tps"))
    total_events = int(round(records_sec * max(elapsed_seconds, 0)))
    failed = int(last.group("failed"))
    if failed > 0:
        detail = _rocketmq_benchmark_failure_detail(output, config, "Consumer", failed)
        raise BenchmarkValidationError(detail)
    success_rate = 100.0 if total_events <= 0 else round(max(0.0, (total_events - failed) / total_events * 100.0), 2)
    avg_latency = float(last.group("avg_b2c"))
    max_latency = max(float(last.group("max_b2c")), float(last.group("max_s2c")))
    mb_sec = records_sec * max(message_size, 1) / 1024 / 1024
    return ConcurrencyResult(concurrency=concurrency, elapsed_seconds=round(elapsed_seconds, 2), total_transactions=total_events, total_queries=total_events, total_events=total_events, read_queries=total_events, write_queries=0, other_queries=0, failed_requests=failed, qps=round(records_sec, 2), tps=round(mb_sec, 2), avg_latency_ms=round(avg_latency, 2), p95_latency_ms=0.0, min_latency_ms=0.0, max_latency_ms=round(max_latency, 2), success_rate=success_rate, baseline_label=_classify_baseline(success_rate, 0.0), sample_errors=_extract_sample_errors(output), raw_output=output)


def _run_rocketmq_benchmark(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None, cancel_callback: Optional[CancelCallback] = None) -> BenchmarkSummary:
    config.validate()
    namesrv = _rocketmq_namesrv(config)
    _emit_log(log_callback, f"RocketMQ 官方 benchmark 启动: workload={config.workload}, namesrv={namesrv}, topic={config.database}")
    _check_cancel(cancel_callback)
    _check_rocketmq_connectivity(config, log_callback=log_callback)
    if config.workload == "consumer" and config.auto_prepare:
        _emit_log(log_callback, "RocketMQ consumer 压测需要 Topic 中已有消息，正在先写入测试消息。")
        prepare_output = _run_command(_rocketmq_producer_command(config, 1, quantity=config.operation_count), timeout_seconds=max(1800, int(config.operation_count / 1000) + 120), allow_failure=False, log_callback=log_callback, log_label="rocketmq-producer-prepare", secrets=[config.password], cancel_callback=cancel_callback, env=_rocketmq_java_env())
        _parse_rocketmq_producer_result(prepare_output, 1, max(1, config.duration_seconds), config.table_size, config)
    if config.warmup_seconds > 0:
        _emit_log(log_callback, f"RocketMQ 预热等待 {config.warmup_seconds} 秒。")
        deadline = monotonic() + config.warmup_seconds
        while monotonic() < deadline:
            _check_cancel(cancel_callback)
            sleep(min(1.0, deadline - monotonic()))
    results: List[ConcurrencyResult] = []
    for concurrency in sorted(set(config.threads_values)):
        _check_cancel(cancel_callback)
        if config.workload == "producer":
            _emit_log(log_callback, f"RocketMQ Producer 开始: 线程={concurrency}, 消息数={config.operation_count}, 消息大小={config.table_size} bytes")
            started = monotonic()
            output = _run_command(_rocketmq_producer_command(config, concurrency), timeout_seconds=max(1800, int(config.duration_seconds or 0) + int(config.operation_count / 1000) + 120), allow_failure=False, log_callback=log_callback, log_label=f"rocketmq-producer-{concurrency}", secrets=[config.password], cancel_callback=cancel_callback, env=_rocketmq_java_env())
            result = _parse_rocketmq_producer_result(output, concurrency, monotonic() - started, config.table_size, config)
        else:
            _emit_log(log_callback, f"RocketMQ Consumer 开始: 线程={concurrency}, 持续={config.duration_seconds}s")
            output, elapsed = _run_rocketmq_command_for_duration(_rocketmq_consumer_command(config, concurrency), config.duration_seconds, log_callback, f"rocketmq-consumer-{concurrency}", cancel_callback, secrets=[config.password])
            result = _parse_rocketmq_consumer_result(output, concurrency, elapsed, config.table_size, config)
        results.append(result)
        _emit_log(log_callback, f"RocketMQ {config.workload} 结果: 并发={concurrency}, msg/s={result.qps}, MB/s={result.tps}")
    return BenchmarkSummary(report_title=config.report_title, executed_at=_beijing_now().strftime("%Y-%m-%d %H:%M:%S"), target={"db_type": "RocketMQ", "host": namesrv, "port": config.port, "database": config.database, "user": config.user or "-", "collection_name": config.database}, parameters={"benchmark_engine": "rocketmq-example-benchmark-producer" if config.workload == "producer" else "rocketmq-example-benchmark-consumer", "workload": config.workload, "duration_seconds": config.duration_seconds, "warmup_seconds": config.warmup_seconds, "report_interval": config.report_interval, "table_count": 0, "table_size": config.table_size, "operation_count": config.operation_count, "auth_database": "", "collection_name": config.database, "ssl_enabled": False, "ssl_verify": False, "auto_prepare": config.auto_prepare, "auto_cleanup": False, "threads_values": sorted(set(config.threads_values)), "rocketmq_namesrv": namesrv}, results=results, recommended_baseline=_select_recommended_baseline(results))


def _run_rocketmq_cleanup(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None, cancel_callback: Optional[CancelCallback] = None) -> None:
    config.validate()
    _check_cancel(cancel_callback)
    _emit_log(log_callback, "RocketMQ 官方 benchmark 不自动删除 Topic；如需清理，请在 RocketMQ 管理端删除测试 Topic 或消费组。")



def _rabbitmq_error_summary(output: str) -> str:
    text = str(output or "")
    reply_match = re.search(r"reply-text=([^,\n\)]+)", text, flags=re.IGNORECASE)
    if reply_match:
        detail = reply_match.group(1).strip()
        if "vhost" in detail.lower() and ("refused" in detail.lower() or "not found" in detail.lower()):
            return f"RabbitMQ 连接失败: {detail}。请确认用户拥有该 vhost 权限，或在其他参数设置中填写 --rabbitmq-vhost <实际vhost>。"
        return f"RabbitMQ 连接失败: {detail}"
    parsing_match = re.search(r"Parsing failed\. Reason:\s*([^\n]+)", text, flags=re.IGNORECASE)
    if parsing_match:
        return f"RabbitMQ PerfTest 参数错误: {parsing_match.group(1).strip()}"
    access_match = re.search(r"(ACCESS_REFUSED[^\n]+)", text, flags=re.IGNORECASE)
    if access_match:
        return f"RabbitMQ 认证失败: {access_match.group(1).strip()}"
    stopped_match = re.search(r"(test stopped[^\n]*)", text, flags=re.IGNORECASE)
    if stopped_match:
        return f"RabbitMQ PerfTest 执行失败: {stopped_match.group(1).strip()}"
    return ""

def _find_rabbitmq_perf_test_jar() -> Path:
    candidates = [
        Path(os.environ.get("RABBITMQ_PERF_TEST_JAR", "")),
        BASE_DIR / "vendor" / "rabbitmq-perf-test" / "perf-test.jar",
        BASE_DIR.parent / "tools" / "rabbitmq-perf-test" / "perf-test.jar",
        Path("/app/tools/rabbitmq-perf-test/perf-test.jar"),
        Path("/opt/rabbitmq-perf-test/perf-test.jar"),
    ]
    for candidate in candidates:
        if not str(candidate):
            continue
        if candidate.exists() and candidate.is_file():
            return candidate
    raise BenchmarkValidationError("未找到 RabbitMQ 官方 PerfTest，请设置 RABBITMQ_PERF_TEST_JAR，或将 perf-test.jar 放到 /app/tools/rabbitmq-perf-test/perf-test.jar。")


def _rabbitmq_broker(config: BenchmarkConfig) -> str:
    host = str(config.host or "").strip()
    port = int(config.port or 5672)
    if "," in host or host.startswith("amqp://") or host.startswith("amqps://"):
        return host
    return host if ":" in host else f"{host}:{port}"


def _rabbitmq_extra_tokens(config: BenchmarkConfig) -> List[str]:
    return shlex.split(str(config.extra_options or "").strip()) if str(config.extra_options or "").strip() else []


def _rabbitmq_vhost(config: BenchmarkConfig) -> str:
    tokens = _rabbitmq_extra_tokens(config)
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token.startswith("--rabbitmq-vhost=") or token.startswith("--rabbitmq_vhost="):
            return token.split("=", 1)[1] or "/"
        if token in {"--rabbitmq-vhost", "--rabbitmq_vhost"}:
            if index + 1 < len(tokens):
                return tokens[index + 1] or "/"
            return "/"
        index += 1
    return "/"


def _rabbitmq_passthrough_extra_args(config: BenchmarkConfig) -> List[str]:
    tokens = _rabbitmq_extra_tokens(config)
    filtered: List[str] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token.startswith("--rabbitmq-vhost=") or token.startswith("--rabbitmq_vhost="):
            index += 1
            continue
        if token in {"--rabbitmq-vhost", "--rabbitmq_vhost"}:
            index += 2
            continue
        filtered.append(token)
        index += 1
    return filtered


def _rabbitmq_uri(config: BenchmarkConfig) -> str:
    raw_host = str(config.host or "").strip()
    if raw_host.startswith("amqp://") or raw_host.startswith("amqps://"):
        return raw_host
    scheme = "amqps" if config.ssl_enabled else "amqp"
    user = quote(str(config.user or ""), safe="")
    password = quote(str(config.password or ""), safe="")
    host = raw_host.strip("[]")
    port = int(config.port or 5672)
    vhost = quote(_rabbitmq_vhost(config), safe="") or "%2F"
    return f"{scheme}://{user}:{password}@{host}:{port}/{vhost}"


def _rabbitmq_perf_command(config: BenchmarkConfig, concurrency: int, workload: str, *, queue_name: Optional[str] = None, duration_seconds: Optional[int] = None, message_count: Optional[int] = None, auto_delete: bool = False, predeclared: bool = False) -> List[str]:
    workload = (workload or "producer").strip().lower()
    producers = concurrency if workload in {"producer", "mixed"} else 0
    consumers = concurrency if workload in {"consumer", "mixed"} else 0
    command = [
        "java",
        "-jar", str(_find_rabbitmq_perf_test_jar()),
        "--uri", _rabbitmq_uri(config),
        "-u", queue_name or config.database,
        "-x", str(producers),
        "-y", str(consumers),
        "-s", str(config.table_size),
        "-z", str(duration_seconds if duration_seconds is not None else config.duration_seconds),
        "-i", str(config.report_interval),
    ]
    if message_count is not None and message_count > 0:
        if producers > 0:
            command.extend(["-C", str(message_count)])
        if consumers > 0:
            command.extend(["-D", str(message_count)])
    if auto_delete or config.auto_cleanup:
        command.extend(["--auto-delete", "true"])
    if predeclared:
        command.append("--predeclared")
    command.extend(_rabbitmq_passthrough_extra_args(config))
    return command


def _check_rabbitmq_connectivity(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None) -> None:
    _emit_log(log_callback, f"开始检查 RabbitMQ 连接性: broker={_rabbitmq_broker(config)} queue={config.database} vhost={_rabbitmq_vhost(config)}")
    probe_queue = f"qdbmark-connectivity-{os.getpid()}-{int(monotonic() * 1000)}"
    output = _run_command(_rabbitmq_perf_command(config, 1, "mixed", queue_name=probe_queue, duration_seconds=1, message_count=10, auto_delete=True), timeout_seconds=60, allow_failure=False, log_callback=log_callback, log_label="rabbitmq-connectivity", secrets=[config.password])
    _parse_rabbitmq_result(output, 1, 1.0, config.table_size, "mixed")
    _emit_log(log_callback, f"RabbitMQ 连接检测通过: {_rabbitmq_broker(config)}")


def _rabbitmq_rate_values(label: str, output: str) -> List[float]:
    text = output or ""
    label_key = (label or "").lower()
    if label_key == "sent":
        aliases = r"(?:sent|sending(?:\s+rate\s+avg)?)"
    elif label_key == "received":
        aliases = r"(?:received|receiving(?:\s+rate\s+avg)?)"
    else:
        aliases = re.escape(label or "")
    patterns = [
        rf"{aliases}:\s*([0-9]+(?:\.[0-9]+)?)\s*msg/s",
        rf"{aliases}[^\n]*?([0-9]+(?:\.[0-9]+)?)\s*msg/s",
    ]
    values: List[float] = []
    for pattern in patterns:
        values.extend(float(match.group(1)) for match in re.finditer(pattern, text, flags=re.IGNORECASE))
    return values


def _rabbitmq_latency_ms(output: str, token: str) -> float:
    pattern = rf"{token}[^0-9]*([0-9]+(?:\.[0-9]+)?)\s*(ms|us|µs)?"
    matches = list(re.finditer(pattern, output or "", flags=re.IGNORECASE))
    if not matches:
        return 0.0
    value = float(matches[-1].group(1))
    unit = (matches[-1].group(2) or "ms").lower()
    if unit in {"us", "µs"}:
        value = value / 1000.0
    return round(value, 2)


def _parse_rabbitmq_result(output: str, concurrency: int, elapsed_seconds: float, message_size: int, workload: str) -> ConcurrencyResult:
    sent_rates = _rabbitmq_rate_values("sent", output)
    received_rates = _rabbitmq_rate_values("received", output)
    workload = (workload or "producer").lower()
    if workload == "consumer":
        msg_sec = received_rates[-1] if received_rates else 0.0
    elif workload == "mixed":
        msg_sec = received_rates[-1] if received_rates else (sent_rates[-1] if sent_rates else 0.0)
    else:
        msg_sec = sent_rates[-1] if sent_rates else 0.0
    if msg_sec <= 0 and not (sent_rates or received_rates):
        raise BenchmarkValidationError("未能解析 RabbitMQ PerfTest 输出，请检查 Broker、Queue、账号权限和工具版本。")
    total_events = int(round(msg_sec * max(elapsed_seconds, 1.0)))
    avg_latency = _rabbitmq_latency_ms(output, "median") or _rabbitmq_latency_ms(output, "avg")
    p95_latency = _rabbitmq_latency_ms(output, "95th")
    max_latency = _rabbitmq_latency_ms(output, "max")
    mb_sec = msg_sec * max(message_size, 1) / 1024 / 1024
    errors = _extract_sample_errors(output)
    failed = len(errors)
    success_rate = 100.0 if total_events <= 0 else round(max(0.0, (total_events - failed) / total_events * 100.0), 2)
    return ConcurrencyResult(concurrency=concurrency, elapsed_seconds=round(elapsed_seconds, 2), total_transactions=total_events, total_queries=total_events, total_events=total_events, read_queries=total_events if workload == "consumer" else 0, write_queries=total_events if workload == "producer" else 0, other_queries=total_events if workload == "mixed" else 0, failed_requests=failed, qps=round(msg_sec, 2), tps=round(mb_sec, 2), avg_latency_ms=round(avg_latency, 2), p95_latency_ms=round(p95_latency, 2), min_latency_ms=0.0, max_latency_ms=round(max_latency, 2), success_rate=success_rate, baseline_label=_classify_baseline(success_rate, p95_latency), sample_errors=errors, raw_output=output)


def _is_rabbitmq_durable_mismatch_error(message: str) -> bool:
    text = str(message or "").lower()
    return "precondition_failed" in text and "inequivalent arg" in text and "durable" in text


def _run_rabbitmq_perf_command(
    config: BenchmarkConfig,
    concurrency: int,
    workload: str,
    *,
    timeout_seconds: int,
    log_callback: Optional[LogCallback],
    log_label: str,
    cancel_callback: Optional[CancelCallback],
    queue_name: Optional[str] = None,
    duration_seconds: Optional[int] = None,
    message_count: Optional[int] = None,
    auto_delete: bool = False,
) -> str:
    try:
        return _run_command(
            _rabbitmq_perf_command(
                config,
                concurrency,
                workload,
                queue_name=queue_name,
                duration_seconds=duration_seconds,
                message_count=message_count,
                auto_delete=auto_delete,
            ),
            timeout_seconds=timeout_seconds,
            allow_failure=False,
            log_callback=log_callback,
            log_label=log_label,
            secrets=[config.password],
            cancel_callback=cancel_callback,
        )
    except BenchmarkValidationError as exc:
        if not _is_rabbitmq_durable_mismatch_error(str(exc)):
            raise
        _emit_log(
            log_callback,
            "检测到 RabbitMQ Queue 已存在且 durable 属性与 PerfTest 默认声明不一致，改用 --predeclared 复用现有 Queue 后重试。",
        )
        return _run_command(
            _rabbitmq_perf_command(
                config,
                concurrency,
                workload,
                queue_name=queue_name,
                duration_seconds=duration_seconds,
                message_count=message_count,
                auto_delete=auto_delete,
                predeclared=True,
            ),
            timeout_seconds=timeout_seconds,
            allow_failure=False,
            log_callback=log_callback,
            log_label=f"{log_label}-predeclared",
            secrets=[config.password],
            cancel_callback=cancel_callback,
        )


def _run_rabbitmq_benchmark(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None, cancel_callback: Optional[CancelCallback] = None) -> BenchmarkSummary:
    config.validate()
    broker = _rabbitmq_broker(config)
    _emit_log(log_callback, f"RabbitMQ 官方 PerfTest 启动: workload={config.workload}, broker={broker}, queue={config.database}, vhost={_rabbitmq_vhost(config)}")
    _check_cancel(cancel_callback)
    _check_rabbitmq_connectivity(config, log_callback=log_callback)
    if config.workload == "consumer" and config.auto_prepare:
        _emit_log(log_callback, "RabbitMQ consumer 压测需要 Queue 中已有消息，正在先写入测试消息。")
        prepare_output = _run_rabbitmq_perf_command(config, 1, "producer", duration_seconds=max(1, min(config.duration_seconds, 60)), message_count=config.operation_count, timeout_seconds=max(120, min(config.operation_count, 1000000) // 1000 + 120), log_callback=log_callback, log_label="rabbitmq-producer-prepare", cancel_callback=cancel_callback)
        _parse_rabbitmq_result(prepare_output, 1, max(1, config.duration_seconds), config.table_size, "producer")
    if config.warmup_seconds > 0:
        _emit_log(log_callback, f"RabbitMQ 预热等待 {config.warmup_seconds} 秒。")
        deadline = monotonic() + config.warmup_seconds
        while monotonic() < deadline:
            _check_cancel(cancel_callback)
            sleep(min(1.0, deadline - monotonic()))
    results: List[ConcurrencyResult] = []
    for concurrency in sorted(set(config.threads_values)):
        _check_cancel(cancel_callback)
        _emit_log(log_callback, f"RabbitMQ PerfTest 开始: 模型={config.workload}, 并发={concurrency}, 时长={config.duration_seconds}s, 消息大小={config.table_size} bytes")
        started = monotonic()
        output = _run_rabbitmq_perf_command(config, concurrency, config.workload, message_count=config.operation_count, timeout_seconds=max(120, int(config.duration_seconds) + 120), log_callback=log_callback, log_label=f"rabbitmq-perftest-{config.workload}-{concurrency}", cancel_callback=cancel_callback)
        result = _parse_rabbitmq_result(output, concurrency, monotonic() - started, config.table_size, config.workload)
        results.append(result)
        _emit_log(log_callback, f"RabbitMQ {config.workload} 结果: 并发={concurrency}, msg/s={result.qps}, MB/s={result.tps}, P95={result.p95_latency_ms}ms")
    return BenchmarkSummary(report_title=config.report_title, executed_at=_beijing_now().strftime("%Y-%m-%d %H:%M:%S"), target={"db_type": "RabbitMQ", "host": broker, "port": config.port, "database": config.database, "user": config.user or "-", "collection_name": config.database}, parameters={"benchmark_engine": "rabbitmq-perf-test", "workload": config.workload, "duration_seconds": config.duration_seconds, "warmup_seconds": config.warmup_seconds, "report_interval": config.report_interval, "table_count": 0, "table_size": config.table_size, "operation_count": config.operation_count, "auth_database": _rabbitmq_vhost(config), "collection_name": config.database, "ssl_enabled": config.ssl_enabled, "ssl_verify": config.ssl_verify, "auto_prepare": config.auto_prepare, "auto_cleanup": config.auto_cleanup, "threads_values": sorted(set(config.threads_values)), "rabbitmq_broker": broker, "rabbitmq_vhost": _rabbitmq_vhost(config)}, results=results, recommended_baseline=_select_recommended_baseline(results))


def _run_rabbitmq_cleanup(config: BenchmarkConfig, log_callback: Optional[LogCallback] = None, cancel_callback: Optional[CancelCallback] = None) -> None:
    config.validate()
    _check_cancel(cancel_callback)
    _emit_log(log_callback, "RabbitMQ PerfTest 仅在本次命令使用 --auto-delete 创建的 Queue 会随连接释放；如测试 Queue 已存在，请在 RabbitMQ 管理端清理。")


def _run_redis_benchmark(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> BenchmarkSummary:
    _emit_log(log_callback, f"已接收测试任务: Redis / {config.host}:{config.port}")
    _emit_log(
        log_callback,
        "redis-benchmark 参数: "
        f"命令={config.workload} | 并发={','.join(str(value) for value in sorted(set(config.threads_values)))} "
        f"| value大小={config.redis_data_size} bytes | 请求数={config.operation_count} | key空间={config.table_size}",
    )

    results: List[ConcurrencyResult] = []
    _check_cancel(cancel_callback)
    _check_redis_connectivity(config, log_callback=log_callback)

    if config.auto_prepare:
        prepare_concurrency = max(sorted(set(config.threads_values)))
        _emit_log(log_callback, f"开始执行 Redis 数据准备与预热: 并发={prepare_concurrency}")
        _run_command(
            _redis_benchmark_command(config, prepare_concurrency),
            timeout_seconds=1800,
            allow_failure=False,
            log_callback=log_callback,
            log_prefix="[redis-prepare] ",
            log_label="redis-benchmark-prepare",
            secrets=[config.password],
            cancel_callback=cancel_callback,
        )

    for concurrency in sorted(set(config.threads_values)):
        _check_cancel(cancel_callback)
        _emit_log(log_callback, f"并发 {concurrency}: 开始执行 Redis redis-benchmark {config.workload} 测试...")
        output = _run_command(
            _redis_benchmark_command(config, concurrency),
            timeout_seconds=1800,
            log_callback=log_callback,
            log_prefix=f"[redis:{concurrency}] ",
            log_label=f"{concurrency}并发Redis测试",
            secrets=[config.password],
            cancel_callback=cancel_callback,
        )
        result = _parse_redis_benchmark_output(config, concurrency, output)
        results.append(result)
        _emit_log(
            log_callback,
            f"并发 {concurrency}: Redis 测试完成，QPS={result.qps}，平均延迟={result.avg_latency_ms}ms，P95={result.p95_latency_ms}ms。",
        )

    _emit_log(log_callback, "所有 Redis 压测并发已执行完成，正在整理结果。")
    return BenchmarkSummary(
        report_title=config.report_title,
        executed_at=_beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
        target={
            "db_type": "Redis",
            "host": config.host,
            "port": config.port,
            "database": config.database or "-",
            "user": config.user or "-",
            "collection_name": "",
        },
        parameters={
            "benchmark_engine": "redis-benchmark",
            "workload": config.workload,
            "duration_seconds": 0,
            "warmup_seconds": config.warmup_seconds,
            "report_interval": 0,
            "table_count": 0,
            "table_size": config.table_size,
            "operation_count": config.operation_count,
            "redis_data_size": config.redis_data_size,
            "auth_database": "",
            "collection_name": "",
            "ssl_enabled": False,
            "ssl_verify": False,
            "auto_prepare": config.auto_prepare,
            "auto_cleanup": False,
            "threads_values": sorted(set(config.threads_values)),
        },
        results=results,
        recommended_baseline=_select_recommended_baseline(results),
    )


def _run_oracle_benchmark(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> BenchmarkSummary:
    _emit_log(log_callback, f"已接收测试任务: Oracle / {config.host}:{config.port}/{config.database}")
    _emit_log(
        log_callback,
        f"压测工具: {config.oracle_engine} | 并发配置: {','.join(str(value) for value in sorted(set(config.threads_values)))} | 时长: {config.duration_seconds}s",
    )
    _check_cancel(cancel_callback)
    _check_oracle_connectivity(config, log_callback=log_callback)
    _apply_oracle_common_configuration(config, log_callback=log_callback, cancel_callback=cancel_callback)

    if config.auto_prepare and config.oracle_engine == "hammerdb":
        _ensure_oracle_hammerdb_tablespace(config, log_callback=log_callback)
        if _oracle_tpcc_schema_exists(config, log_callback=log_callback):
            if config.auto_cleanup:
                _emit_log(log_callback, "检测到 Oracle TPCC schema 已存在，开始清理旧 schema...")
                _run_hammerdb_script(
                    _hammerdb_oracle_deleteschema_script(config),
                    timeout_seconds=3600,
                    log_callback=log_callback,
                    log_label="hammerdb-oracle-deleteschema",
                    secrets=[config.password],
                    cancel_callback=cancel_callback,
                    artifact_dir=config.artifact_dir,
                )
            else:
                raise BenchmarkValidationError(
                    '检测到 Oracle TPCC schema 已存在业务表，重复 buildschema 会触发唯一键冲突。'
                    ' 请开启“自动清理”，或手动清理旧 schema 后再执行自动准备，或关闭“自动准备”直接复用现有数据。'
                )
        _emit_log(log_callback, "开始执行 Oracle HammerDB TPROC-C 建模...")
        try:
            _run_hammerdb_script(
                _hammerdb_oracle_script(
                    config,
                    max(sorted(set(config.threads_values))),
                    build_schema=True,
                ),
                timeout_seconds=7200,
                log_callback=log_callback,
                log_label="hammerdb-oracle-buildschema",
                secrets=[config.password],
                cancel_callback=cancel_callback,
                ignored_ora_codes={"ORA-01920"},
                artifact_dir=config.artifact_dir,
            )
        except BenchmarkValidationError as exc:
            message = str(exc)
            if 'ORA-01031' in message:
                raise BenchmarkValidationError(
                    'Oracle HammerDB 建模失败: 当前页面用户名/密码不具备 create user、grant、quota 等建模权限。'
                    ' 请改用具备管理员权限的 Oracle 账号，或关闭“自动准备”并使用已存在的 TPCC 用户/对象。'
                    f' 原始错误: {message}'
                ) from exc
            if 'ORA-01017' in message:
                raise BenchmarkValidationError(
                    'Oracle HammerDB 执行失败: TPCC 用户认证失败。通常是 buildschema 未成功创建 tpcc 用户，或 TPCC 用户/密码与实际不一致。'
                    f' 原始错误: {message}'
                ) from exc
            if 'ORA-00001' in message:
                raise BenchmarkValidationError(
                    'Oracle HammerDB 建模失败: 检测到唯一键冲突，通常是旧的 TPCC 数据未清理干净后重复建模。'
                    ' 请开启“自动清理”，或先手动删除旧 TPCC schema/对象后再执行。'
                    f' 原始错误: {message}'
                ) from exc
            raise
        _check_oracle_tpcc_connectivity(config, log_callback=log_callback)
        _validate_oracle_tpcc_schema_ready(config, log_callback=log_callback)
    elif config.auto_prepare and config.oracle_engine == "swingbench":
        _emit_log(log_callback, "开始执行 Oracle Swingbench SOE 建模...")
        _run_swingbench_build(config, log_callback=log_callback, cancel_callback=cancel_callback)
    elif config.oracle_engine == "hammerdb":
        _check_oracle_tpcc_connectivity(config, log_callback=log_callback)
        _validate_oracle_tpcc_schema_ready(config, log_callback=log_callback)

    if config.oracle_engine == "hammerdb":
        _grant_oracle_awr_permissions(config, log_callback=log_callback)

    results: List[ConcurrencyResult] = []
    for concurrency in sorted(set(config.threads_values)):
        _check_cancel(cancel_callback)
        if config.oracle_engine == "swingbench":
            output = _run_swingbench(config, concurrency, log_callback=log_callback, cancel_callback=cancel_callback)
            result = _parse_swingbench_oracle_output(config, concurrency, output)
        else:
            before_next_order_sum = _oracle_tpcc_next_order_sum(config)
            output = _run_hammerdb_oracle_runload(
                config,
                concurrency,
                timeout_seconds=config.duration_seconds + config.warmup_seconds + 900,
                log_callback=log_callback,
                cancel_callback=cancel_callback,
            )
            after_next_order_sum = _oracle_tpcc_next_order_sum(config)
            if after_next_order_sum <= before_next_order_sum:
                raise BenchmarkValidationError(
                    "Oracle HammerDB TPROC-C 压测未产生有效 New Order 增量: "
                    f"before_d_next_o_id_sum={before_next_order_sum}, after_d_next_o_id_sum={after_next_order_sum}。"
                    " 这类结果会导致 NOPM=0，已判定为无效压测。"
                )
            _emit_log(
                log_callback,
                "Oracle TPROC-C New Order 增量校验通过: "
                f"{after_next_order_sum - before_next_order_sum} / {config.duration_seconds}s",
            )
            result = _parse_hammerdb_oracle_output(config, concurrency, output)
        results.append(result)
        _emit_log(log_callback, f"并发 {concurrency}: Oracle {config.oracle_engine} 完成，TPS/TPM={result.tps}，QPS/NOPM={result.qps}")

    return BenchmarkSummary(
        report_title=config.report_title,
        executed_at=_beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
        target={
            "db_type": "Oracle",
            "host": config.host,
            "port": config.port,
            "database": config.database,
            "user": config.user,
            "collection_name": "",
        },
        parameters={
            "benchmark_engine": config.oracle_engine,
            "workload": "SOE" if config.oracle_engine == "swingbench" else "TPROC-C",
            "duration_seconds": config.duration_seconds,
            "warmup_seconds": config.warmup_seconds,
            "report_interval": config.report_interval,
            "table_count": 0,
            "table_size": config.table_size,
            "operation_count": 0,
            "auth_database": "",
            "collection_name": "",
            "ssl_enabled": False,
            "ssl_verify": False,
            "auto_prepare": config.auto_prepare,
            "auto_cleanup": False,
            "threads_values": sorted(set(config.threads_values)),
        },
        results=results,
        recommended_baseline=_select_recommended_baseline(results),
    )


def _run_sqlserver_cleanup(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
    *,
    allow_failure: bool = False,
) -> None:
    _emit_log(log_callback, "开始清理 SQL Server HammerDB TPC-C 测试数据...")
    try:
        _run_hammerdb_script(
            _hammerdb_sqlserver_deleteschema_script(config),
            timeout_seconds=1800,
            log_callback=log_callback,
            log_label="hammerdb-sqlserver-deleteschema",
            secrets=[config.password],
            cancel_callback=cancel_callback,
            artifact_dir=config.artifact_dir,
        )
    except BenchmarkValidationError as exc:
        message = (
            "SQL Server 测试数据清理失败。请确认当前账号在目标数据库内具备删除 HammerDB TPC-C 对象的权限"
            "（建议 db_owner，至少需要 DROP/ALTER 相关权限）。"
            f" 详细信息: {exc}"
        )
        if allow_failure:
            _emit_log(log_callback, message)
            return
        raise BenchmarkValidationError(message) from exc
    _emit_log(log_callback, "SQL Server HammerDB TPC-C 测试数据清理完成。")


def _run_sqlserver_benchmark(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> BenchmarkSummary:
    _emit_log(log_callback, f"已接收测试任务: SQL Server / {config.host}:{config.port}/{config.database}")
    _emit_log(
        log_callback,
        f"压测工具: HammerDB TPC-C | 并发配置: {','.join(str(value) for value in sorted(set(config.threads_values)))} | 时长: {config.duration_seconds}s",
    )
    _check_cancel(cancel_callback)
    _check_sqlserver_connectivity(config, log_callback=log_callback)

    if config.auto_prepare:
        _emit_log(log_callback, "SQL Server 自动准备开启，先清理旧 HammerDB TPC-C 数据，再执行建模。")
        _run_sqlserver_cleanup(config, log_callback=log_callback, cancel_callback=cancel_callback, allow_failure=True)
        _emit_log(log_callback, "开始执行 SQL Server HammerDB TPC-C 建模...")
        _run_hammerdb_script(
            _hammerdb_sqlserver_script(config, max(sorted(set(config.threads_values))), build_schema=True),
            timeout_seconds=7200,
            log_callback=log_callback,
            log_label="hammerdb-sqlserver-buildschema",
            secrets=[config.password],
            cancel_callback=cancel_callback,
            artifact_dir=config.artifact_dir,
        )

    results: List[ConcurrencyResult] = []
    for concurrency in sorted(set(config.threads_values)):
        _check_cancel(cancel_callback)
        output = _run_hammerdb_script(
            _hammerdb_sqlserver_script(config, concurrency, build_schema=False),
            timeout_seconds=config.duration_seconds + config.warmup_seconds + 900,
            log_callback=log_callback,
            log_label=f"{concurrency}并发SQLServer HammerDB测试",
            secrets=[config.password],
            cancel_callback=cancel_callback,
            artifact_dir=config.artifact_dir,
        )
        result = _parse_hammerdb_sqlserver_output(config, concurrency, output)
        results.append(result)
        _emit_log(log_callback, f"并发 {concurrency}: SQL Server HammerDB 完成，TPM={result.tps}，NOPM={result.qps}")

    if config.auto_cleanup:
        _run_sqlserver_cleanup(config, log_callback=log_callback, cancel_callback=cancel_callback, allow_failure=True)

    return BenchmarkSummary(
        report_title=config.report_title,
        executed_at=_beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
        target={
            "db_type": "SQL Server",
            "host": config.host,
            "port": config.port,
            "database": config.database,
            "user": config.user,
            "collection_name": "",
        },
        parameters={
            "benchmark_engine": "hammerdb",
            "workload": "TPC-C",
            "duration_seconds": config.duration_seconds,
            "warmup_seconds": config.warmup_seconds,
            "report_interval": 0,
            "table_count": 0,
            "table_size": config.table_size,
            "operation_count": 0,
            "auth_database": "",
            "collection_name": "",
            "ssl_enabled": False,
            "ssl_verify": False,
            "auto_prepare": config.auto_prepare,
            "auto_cleanup": config.auto_cleanup,
            "threads_values": sorted(set(config.threads_values)),
        },
        results=results,
        recommended_baseline=_select_recommended_baseline(results),
    )


def _run_mongodb_benchmark(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> BenchmarkSummary:
    _emit_log(log_callback, f"已接收测试任务: {config.db_type} / {_mongo_display_target(config)}")
    _emit_log(
        log_callback,
        f"拓扑: {config.mongo_topology} | 标准引擎: YCSB | 线程配置: {','.join(str(value) for value in sorted(set(config.threads_values)))} | recordcount: {config.table_size} | operationcount: {config.operation_count}",
    )

    results: List[ConcurrencyResult] = []
    try:
        _check_cancel(cancel_callback)
        _check_mongodb_connectivity(config, log_callback=log_callback)

        if config.auto_prepare:
            _run_mongo_prepare(config, log_callback=log_callback, cancel_callback=cancel_callback)
        readiness = _check_mongodb_ycsb_readiness(config, log_callback=log_callback, require_data=True)
        _emit_log(
            log_callback,
            f"MongoDB 数据集就绪: {config.database}.{config.collection_name}，预计记录数 {readiness['document_count']}。",
        )

        if config.warmup_seconds > 0:
            _emit_log(
                log_callback,
                f"MongoDB 当前按标准 YCSB load/run 流程执行，预热时长 {config.warmup_seconds}s 不再单独执行。",
            )

        for concurrency in sorted(set(config.threads_values)):
            _check_cancel(cancel_callback)
            result = _run_ycsb_stage(
                config,
                "run",
                concurrency,
                timeout_seconds=_estimate_ycsb_timeout(config.operation_count, concurrency),
                log_callback=log_callback,
                cancel_callback=cancel_callback,
            )
            results.append(result)
            _emit_log(
                log_callback,
                f"线程 {concurrency}: YCSB run 完成，OPS={result.qps}，P95={result.p95_latency_ms}ms，成功率={result.success_rate}%，读={result.read_queries}，写={result.write_queries}",
            )
    finally:
        if config.auto_cleanup:
            _run_mongo_cleanup(config, log_callback=log_callback)

    _emit_log(log_callback, "所有压测线程已执行完成，正在整理结果。")

    return BenchmarkSummary(
        report_title=config.report_title,
        executed_at=_beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
        target={
            "db_type": config.db_type,
            "host": config.host,
            "port": config.port,
            "hosts": _mongo_connection_hosts(config),
            "database": config.database,
            "user": config.user or "-",
            "collection_name": config.collection_name,
            "mongo_topology": config.mongo_topology,
            "mongo_replica_set": config.mongo_replica_set,
            "mongo_read_preference": config.mongo_read_preference,
            "mongo_shard_key": config.mongo_shard_key,
        },
        parameters={
            "benchmark_engine": "ycsb",
            "workload": config.workload,
            "duration_seconds": 0,
            "warmup_seconds": config.warmup_seconds,
            "report_interval": 0,
            "table_count": 0,
            "table_size": config.table_size,
            "operation_count": config.operation_count,
            "mongo_topology": config.mongo_topology,
            "mongo_hosts": _mongo_connection_hosts(config),
            "mongo_replica_set": config.mongo_replica_set,
            "mongo_read_preference": config.mongo_read_preference,
            "mongo_shard_key": config.mongo_shard_key,
            "auth_database": config.auth_database or config.database,
            "collection_name": config.collection_name,
            "ssl_enabled": config.ssl_enabled,
            "ssl_verify": config.ssl_verify,
            "auto_prepare": config.auto_prepare,
            "auto_cleanup": config.auto_cleanup,
            "threads_values": sorted(set(config.threads_values)),
        },
        results=results,
        recommended_baseline=_select_recommended_baseline(results),
    )


def _run_postgresql_benchmark(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> BenchmarkSummary:
    _emit_log(log_callback, f"已接收测试任务: {config.db_type} / {config.host}:{config.port}/{config.database}")
    _emit_log(
        log_callback,
        f"压测引擎: {config.postgres_engine} | 线程配置: {','.join(str(value) for value in sorted(set(config.threads_values)))} | 时长: {config.duration_seconds}s",
    )

    results: List[ConcurrencyResult] = []
    try:
        _check_cancel(cancel_callback)
        _check_postgresql_connectivity(config, log_callback=log_callback)

        if config.postgres_engine == "sysbench":
            if config.auto_prepare:
                _run_postgres_sysbench_prepare(config, log_callback=log_callback, cancel_callback=cancel_callback)
            if config.warmup_seconds > 0:
                _emit_log(
                    log_callback,
                    f"{config.db_type} sysbench 当前不单独执行预热阶段，预热时长 {config.warmup_seconds}s 仅保留在报告参数中。",
                )
            for concurrency in sorted(set(config.threads_values)):
                _check_cancel(cancel_callback)
                command = _build_postgres_sysbench_command(config, workload=config.workload) + [
                    f"--threads={concurrency}",
                    f"--time={config.duration_seconds}",
                    "--events=0",
                    f"--report-interval={config.report_interval}",
                    "run",
                ]
                _emit_log(log_callback, f"线程 {concurrency}: 开始执行 {config.db_type} sysbench 压测...")
                output = _run_command(
                    command,
                    timeout_seconds=config.duration_seconds + 600,
                    log_callback=log_callback,
                    log_prefix=f"[pgsysbench:{concurrency}] ",
                    log_label=f"{concurrency}线程{config.db_type} sysbench测试",
                    secrets=[config.password],
                    cancel_callback=cancel_callback,
                    env=_postgres_command_env(config),
                )
                result = _parse_sysbench_output(concurrency, output)
                results.append(result)
                _emit_log(
                    log_callback,
                    f"线程 {concurrency}: sysbench 完成，{_workload_metric_log_text(config.workload, result)}，P95={result.p95_latency_ms}ms，成功率={result.success_rate}%",
                )
        else:
            if config.auto_prepare:
                _run_pgbench_prepare(config, log_callback=log_callback, cancel_callback=cancel_callback)
            if config.warmup_seconds > 0:
                _emit_log(
                    log_callback,
                    f"pgbench 当前按 initialize/run 原生流程执行，不单独执行 {config.warmup_seconds}s 预热。",
                )
            for concurrency in sorted(set(config.threads_values)):
                _check_cancel(cancel_callback)
                output = _run_command(
                    _build_pgbench_run_command(config, concurrency),
                    timeout_seconds=config.duration_seconds + 600,
                    log_callback=log_callback,
                    log_prefix=f"[pgbench:{concurrency}] ",
                    log_label=f"{concurrency}客户端pgbench测试",
                    cancel_callback=cancel_callback,
                    env=_postgres_command_env(config),
                )
                result = _parse_pgbench_output(concurrency, output, config.workload)
                results.append(result)
                _emit_log(
                    log_callback,
                    f"线程 {concurrency}: pgbench 完成，{_workload_metric_log_text(config.workload, result)}，平均延迟={result.avg_latency_ms}ms，成功率={result.success_rate}%",
                )
    finally:
        if config.auto_cleanup:
            if config.postgres_engine == "sysbench":
                _run_postgres_sysbench_cleanup(config, log_callback=log_callback, cancel_callback=cancel_callback)
            else:
                _run_pgbench_cleanup(config, log_callback=log_callback, cancel_callback=cancel_callback)

    _emit_log(log_callback, "所有压测线程已执行完成，正在整理结果。")

    return BenchmarkSummary(
        report_title=config.report_title,
        executed_at=_beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
        target={
            "db_type": config.db_type,
            "host": config.host,
            "port": config.port,
            "database": config.database,
            "schema": config.postgres_schema,
            "user": config.user,
            "collection_name": "",
        },
        parameters={
            "benchmark_engine": config.postgres_engine,
            "workload": config.workload,
            "duration_seconds": config.duration_seconds,
            "warmup_seconds": config.warmup_seconds,
            "report_interval": config.report_interval,
            "table_count": config.table_count,
            "table_size": config.table_size,
            "operation_count": 0,
            "auth_database": "",
            "collection_name": "",
            "ssl_enabled": config.ssl_enabled,
            "ssl_verify": config.ssl_verify,
            "auto_prepare": config.auto_prepare,
            "auto_cleanup": config.auto_cleanup,
            "threads_values": sorted(set(config.threads_values)),
            "postgres_engine": config.postgres_engine,
            "postgres_schema": config.postgres_schema,
            "pgbench_scale": config.pgbench_scale,
            "pgbench_jobs": config.pgbench_jobs,
            "pgbench_fillfactor": config.pgbench_fillfactor,
            "pgbench_latency_limit": config.pgbench_latency_limit,
        },
        results=results,
        recommended_baseline=_select_recommended_baseline(results),
    )


def _run_fio_benchmark(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> BenchmarkSummary:
    _emit_log(log_callback, f"已接收测试任务: {config.db_type} / {_fio_display_target(config)}")
    _emit_log(
        log_callback,
        f"并发配置: {','.join(str(value) for value in sorted(set(config.threads_values)))} | 模式: {config.workload} | 文件大小: {config.fio_size_mb}MB",
    )

    results: List[ConcurrencyResult] = []
    try:
        _check_cancel(cancel_callback)
        _check_fio_connectivity(config, log_callback=log_callback)

        for concurrency in sorted(set(config.threads_values)):
            _check_cancel(cancel_callback)
            command = _build_fio_command(config, concurrency)
            _emit_log(
                log_callback,
                f"线程 {concurrency}: 开始执行 {'远程' if config.fio_ssh_enabled else '本地'} FIO 测试...",
            )
            try:
                if config.fio_ssh_enabled:
                    output = _run_remote_command(
                        config,
                        f"sh -lc {shlex.quote(shlex.join(['fio'] + command[1:]))}",
                        timeout_seconds=config.duration_seconds + config.warmup_seconds + 600,
                        log_callback=log_callback,
                        log_prefix=f"[fio:{concurrency}] ",
                        log_label=f"{concurrency}线程压测",
                        cancel_callback=cancel_callback,
                    )
                else:
                    output = _run_command(
                        command,
                        timeout_seconds=config.duration_seconds + config.warmup_seconds + 600,
                        log_callback=log_callback,
                        log_prefix=f"[fio:{concurrency}] ",
                        log_label=f"{concurrency}线程压测",
                        cancel_callback=cancel_callback,
                    )
            except BenchmarkValidationError as exc:
                raise BenchmarkValidationError(_friendly_fio_failure_message(config, str(exc))) from exc
            result = _parse_fio_output(concurrency, output)
            if result.failed_requests or result.total_events <= 0:
                raise BenchmarkValidationError(_friendly_fio_failure_message(config, output))
            results.append(result)
            _emit_log(
                log_callback,
                f"线程 {concurrency}: 完成，带宽={result.tps}MB/s，IOPS={result.qps}，P95={result.p95_latency_ms}ms，成功率={result.success_rate}%",
            )
    finally:
        if config.auto_cleanup:
            _run_fio_cleanup(config, log_callback=log_callback, cancel_callback=cancel_callback)

    _emit_log(log_callback, "所有 FIO 测试已执行完成，正在整理结果。")

    return BenchmarkSummary(
        report_title=config.report_title,
        executed_at=_beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
        target={
            "db_type": config.db_type,
            "host": "-",
            "port": 0,
            "database": "-",
            "user": "-",
            "collection_name": "",
            "target_path": config.fio_target_path,
            "ssh_enabled": config.fio_ssh_enabled,
            "ssh_host": config.fio_ssh_host,
            "ssh_port": config.fio_ssh_port,
            "ssh_user": config.fio_ssh_user,
        },
        parameters={
            "benchmark_engine": "fio",
            "workload": config.workload,
            "duration_seconds": config.duration_seconds,
            "warmup_seconds": config.warmup_seconds,
            "report_interval": 0,
            "table_count": 0,
            "table_size": 0,
            "operation_count": 0,
            "auth_database": "",
            "collection_name": "",
            "ssl_enabled": False,
            "ssl_verify": False,
            "auto_prepare": False,
            "auto_cleanup": config.auto_cleanup,
            "threads_values": sorted(set(config.threads_values)),
            "fio_target_path": config.fio_target_path,
            "fio_ssh_enabled": config.fio_ssh_enabled,
            "fio_ssh_host": config.fio_ssh_host,
            "fio_ssh_port": config.fio_ssh_port,
            "fio_ssh_user": config.fio_ssh_user,
            "fio_block_size": config.fio_block_size,
            "fio_iodepth": config.fio_iodepth,
            "fio_size_mb": config.fio_size_mb,
            "fio_direct": config.fio_direct,
            "fio_read_percent": config.fio_read_percent,
        },
        results=results,
        recommended_baseline=_select_recommended_baseline(results),
    )


def run_benchmark(
    config: BenchmarkConfig,
    log_callback: Optional[LogCallback] = None,
    cancel_callback: Optional[CancelCallback] = None,
) -> BenchmarkSummary:
    config.validate()
    if config.db_type == "TiDB" and config.tidb_engine == "tiup_tpcc":
        return _run_tidb_tiup_tpcc_benchmark(config, log_callback=log_callback, cancel_callback=cancel_callback)
    if config.db_type in {"MySQL", "TiDB"}:
        return _run_mysql_benchmark(config, log_callback=log_callback, cancel_callback=cancel_callback)
    if config.db_type == "MongoDB":
        return _run_mongodb_benchmark(config, log_callback=log_callback, cancel_callback=cancel_callback)
    if config.db_type == "Redis":
        return _run_redis_benchmark(config, log_callback=log_callback, cancel_callback=cancel_callback)
    if config.db_type == "ElasticSearch":
        return _run_elasticsearch_benchmark(config, log_callback=log_callback, cancel_callback=cancel_callback)
    if config.db_type == "ClickHouse":
        return _run_clickhouse_benchmark(config, log_callback=log_callback, cancel_callback=cancel_callback)
    if config.db_type == "Kafka":
        return _run_kafka_benchmark(config, log_callback=log_callback, cancel_callback=cancel_callback)
    if config.db_type == "RocketMQ":
        return _run_rocketmq_benchmark(config, log_callback=log_callback, cancel_callback=cancel_callback)
    if config.db_type == "RabbitMQ":
        return _run_rabbitmq_benchmark(config, log_callback=log_callback, cancel_callback=cancel_callback)
    if config.db_type == "Oracle":
        return _run_oracle_benchmark(config, log_callback=log_callback, cancel_callback=cancel_callback)
    if config.db_type == "SQL Server":
        return _run_sqlserver_benchmark(config, log_callback=log_callback, cancel_callback=cancel_callback)
    if config.db_type == "DM":
        return _run_dm_benchmark(config, log_callback=log_callback, cancel_callback=cancel_callback)
    if config.db_type == "GaussDB":
        return _run_gaussdb_benchmark(config, log_callback=log_callback, cancel_callback=cancel_callback)
    if config.db_type in POSTGRES_DB_TYPES:
        return _run_postgresql_benchmark(config, log_callback=log_callback, cancel_callback=cancel_callback)
    if config.db_type == "FIO":
        return _run_fio_benchmark(config, log_callback=log_callback, cancel_callback=cancel_callback)
    raise BenchmarkValidationError(
        f"当前版本仅支持 MySQL / TiDB / SQL Server / Oracle / PostgreSQL / GaussDB / OpenGauss / DM / OceanBase / KingBase / Vastbase / MongoDB / Redis / ElasticSearch / ClickHouse / Kafka / RocketMQ / RabbitMQ / FIO 压测执行，已选择 {config.db_type}。"
    )
