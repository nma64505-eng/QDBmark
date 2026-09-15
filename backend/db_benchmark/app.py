from __future__ import annotations

import json
import os
import re
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict
from zoneinfo import ZoneInfo

from flask import Flask, jsonify, render_template, request, send_from_directory
from werkzeug.utils import secure_filename

from db_benchmark.benchmark import (
    BenchmarkConfig,
    BenchmarkCancelledError,
    BenchmarkSummary,
    BenchmarkValidationError,
    inspect_mysql_target_schema,
)
from db_benchmark.configs import DM_TUNING_EXTRA_OPTIONS, OCEANBASE_APPLY_TUNING_OPTIONS, OCEANBASE_BOOLEAN_EXTRA_OPTIONS, OCEANBASE_GATHER_STATS_OPTIONS, OCEANBASE_WAIT_MAJOR_FREEZE_OPTIONS, build_config_from_payload, cli_option_enabled, strip_extra_options
from db_benchmark.databases.dispatcher import (
    cleanup_benchmark_data,
    collect_instance_info,
    collect_runtime_sample,
    format_runtime_metrics,
    run_benchmark,
    run_connectivity_check,
)
from db_benchmark.report import generate_batch_report, generate_report


PACKAGE_DIR = Path(__file__).resolve().parent
BACKEND_DIR = PACKAGE_DIR.parent
PROJECT_ROOT = BACKEND_DIR.parent
FRONTEND_DIR = PROJECT_ROOT / "frontend"
RESOURCES_DIR = FRONTEND_DIR
DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
HISTORY_FILE = REPORTS_DIR / "benchmark_history.json"
UPLOADS_DIR = DATA_DIR / "uploads"
SSL_CERTS_DIR = UPLOADS_DIR / "ssl"
SSL_CERTS_DIR.mkdir(parents=True, exist_ok=True)
ARTIFACTS_DIR = DATA_DIR / "artifacts"
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
SETTINGS_DIR = DATA_DIR / "settings"
SETTINGS_DIR.mkdir(parents=True, exist_ok=True)
PROMETHEUS_SETTINGS_FILE = SETTINGS_DIR / "prometheus.json"
SYSTEM_STATUS_SETTINGS_FILE = SETTINGS_DIR / "system_status.json"

app = Flask(
    __name__,
    template_folder=str(RESOURCES_DIR / "templates"),
    static_folder=str(RESOURCES_DIR / "static"),
)
BEIJING_TZ = ZoneInfo("Asia/Shanghai")
CONTAINER_METRICS_LOCK = threading.Lock()
CONTAINER_METRICS_PREVIOUS: Dict[str, Any] = {}


def _beijing_now() -> datetime:
    return datetime.now(BEIJING_TZ)


@dataclass
class BenchmarkJob:
    job_id: str
    kind: str = "draft"
    db_type: str = "-"
    target_display: str = "-"
    title: str = "-"
    created_at: str = field(default_factory=lambda: _beijing_now().strftime("%Y-%m-%d %H:%M:%S"))
    updated_at: str = field(default_factory=lambda: _beijing_now().strftime("%Y-%m-%d %H:%M:%S"))
    status: str = "draft"
    logs: list[str] = field(default_factory=list)
    summary: Dict[str, Any] | None = None
    report: Dict[str, str] | None = None
    instance_info: Dict[str, Any] | None = None
    runtime_metrics: Dict[str, Any] | None = None
    runtime_history: list[Dict[str, Any]] = field(default_factory=list)
    error: str | None = None
    stop_requested: bool = False
    payload: Dict[str, Any] = field(default_factory=dict, repr=False)
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def append_log(self, message: str) -> None:
        timestamp = _beijing_now().strftime("%H:%M:%S")
        with self.lock:
            self.logs.append(f"[{timestamp}] {message}")
            self.updated_at = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")

    def snapshot(self, offset: int = 0) -> Dict[str, Any]:
        with self.lock:
            logs = self.logs[offset:]
            payload: Dict[str, Any] = {
                "ok": True,
                "job_id": self.job_id,
                "kind": self.kind,
                "db_type": self.db_type,
                "target_display": self.target_display,
                "title": self.title,
                "created_at": self.created_at,
                "updated_at": self.updated_at,
                "status": self.status,
                "logs": logs,
                "next_offset": len(self.logs),
            }
            if self.summary is not None:
                payload["summary"] = self.summary
            if self.report is not None:
                payload["report"] = self.report
            if self.instance_info is not None:
                payload["instance_info"] = self.instance_info
            if self.runtime_metrics is not None:
                payload["runtime_metrics"] = self.runtime_metrics
            if self.runtime_history:
                payload["runtime_history"] = list(self.runtime_history)
            if self.error is not None:
                payload["error"] = self.error
            try:
                payload["editable_config"] = _editable_config_from_payload(dict(self.payload))
            except Exception:
                payload["editable_config"] = {}
            payload["connection_info"] = _connection_info_from_payload(dict(self.payload))
            return payload

    def request_stop(self) -> None:
        with self.lock:
            self.stop_requested = True
            self.updated_at = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")

    def should_stop(self) -> bool:
        with self.lock:
            return self.stop_requested

    def set_instance_info(self, instance_info: Dict[str, Any] | None) -> None:
        with self.lock:
            self.instance_info = instance_info
            self.updated_at = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")

    def set_runtime_metrics(self, runtime_metrics: Dict[str, Any] | None) -> None:
        with self.lock:
            self.runtime_metrics = runtime_metrics
            self.updated_at = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")

    def append_runtime_history(self, runtime_metrics: Dict[str, Any]) -> None:
        with self.lock:
            self.runtime_history.append(runtime_metrics)
            if len(self.runtime_history) > 120:
                self.runtime_history = self.runtime_history[-120:]
            self.updated_at = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")

    def compact(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "job_id": self.job_id,
                "kind": self.kind,
                "db_type": self.db_type,
                "target_display": self.target_display,
                "title": self.title,
                "status": self.status,
                "created_at": self.created_at,
                "updated_at": self.updated_at,
                "has_runtime": bool(self.runtime_history or self.runtime_metrics),
                "has_report": bool(self.report and self.report.get("url")),
                "has_summary": bool(self.summary),
                "error": self.error,
                "editable_config": _editable_config_from_payload(dict(self.payload)),
                "connection_info": _connection_info_from_payload(dict(self.payload)),
            }


JOBS: dict[str, BenchmarkJob] = {}
JOBS_LOCK = threading.Lock()
HISTORY_LOCK = threading.Lock()
MAX_TASKS = 3


def _load_history() -> list[Dict[str, Any]]:
    if not HISTORY_FILE.exists():
        return []
    try:
        entries = json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
        if not isinstance(entries, list):
            return []
        mutated = False
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            if not entry.get("id"):
                entry["id"] = uuid.uuid4().hex
                mutated = True
        if mutated:
            HISTORY_FILE.write_text(json.dumps(entries, ensure_ascii=False, indent=2), encoding="utf-8")
        return entries
    except Exception:
        return []


REPORT_HISTORY: list[Dict[str, Any]] = _load_history()


def _parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _load_prometheus_settings() -> Dict[str, str]:
    try:
        payload = json.loads(PROMETHEUS_SETTINGS_FILE.read_text(encoding="utf-8"))
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    return {
        "prometheus_url": str(payload.get("prometheus_url", "")).strip(),
        "prometheus_namespace": str(payload.get("prometheus_namespace", "")).strip() or "qfusion-admin",
    }


def _save_prometheus_settings(payload: Dict[str, Any]) -> Dict[str, str]:
    settings = {
        "prometheus_url": str(payload.get("prometheus_url", "")).strip(),
        "prometheus_namespace": str(payload.get("prometheus_namespace", "")).strip() or "qfusion-admin",
    }
    PROMETHEUS_SETTINGS_FILE.write_text(json.dumps(settings, ensure_ascii=False, indent=2), encoding="utf-8")
    return settings


def _format_bytes(value: Any) -> str:
    try:
        size = float(value)
    except (TypeError, ValueError):
        return "-"
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    unit_index = 0
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    if unit_index <= 1:
        return f"{size:.0f} {units[unit_index]}"
    return f"{size:.2f} {units[unit_index]}"


def _run_remote_system_status(host: str, port: int, user: str, password: str) -> Dict[str, str]:
    remote_script = """
set +e
cpu_cores="$(getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || grep -c '^processor' /proc/cpuinfo 2>/dev/null)"
cpu_model="$(lscpu 2>/dev/null | awk -F: '/Model name|型号名称/ {gsub(/^[ \\t]+/, "", $2); print $2; exit}')"
[ -n "$cpu_model" ] || cpu_model="$(awk -F: '/model name|Hardware|Processor/ {gsub(/^[ \\t]+/, "", $2); print $2; exit}' /proc/cpuinfo 2>/dev/null)"
hostname_value="$(hostname 2>/dev/null || uname -n 2>/dev/null)"
arch="$(uname -m 2>/dev/null)"
kernel="$(uname -sr 2>/dev/null)"
mem_kb="$(awk '/MemTotal/ {print $2}' /proc/meminfo 2>/dev/null)"
case "$mem_kb" in
  ''|*[!0-9]*) mem_bytes="" ;;
  *) mem_bytes=$((mem_kb * 1024)) ;;
esac
k8s_nodes=""
k8s_version=""
k8s_context=""
if command -v kubectl >/dev/null 2>&1; then
  k8s_nodes="$(kubectl get nodes --no-headers 2>/dev/null | awk 'NF {count++} END {print count+0}')"
  k8s_version="$(kubectl version --short 2>/dev/null | awk -F: '/Server Version/ {gsub(/^[ \\t]+/, "", $2); print $2; exit}')"
  [ -n "$k8s_version" ] || k8s_version="$(kubectl version -o json 2>/dev/null | awk '/"serverVersion"/ {flag=1} flag && /"gitVersion"/ {gsub(/[",]/, "", $2); print $2; exit}')"
  k8s_context="$(kubectl config current-context 2>/dev/null)"
fi
printf 'cpu_cores=%s\\n' "$cpu_cores"
printf 'cpu_model=%s\\n' "$cpu_model"
printf 'hostname=%s\\n' "$hostname_value"
printf 'arch=%s\\n' "$arch"
printf 'kernel=%s\\n' "$kernel"
printf 'memory_bytes=%s\\n' "$mem_bytes"
printf 'k8s_nodes=%s\\n' "$k8s_nodes"
printf 'k8s_version=%s\\n' "$k8s_version"
printf 'k8s_context=%s\\n' "$k8s_context"
"""
    ssh_target = f"{user}@{host}"
    base_args = [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "ConnectTimeout=8",
        "-p", str(port),
        ssh_target,
        "sh -s",
    ]
    if password:
        args = ["sshpass", "-p", password, *base_args]
    else:
        args = [*base_args[:1], "-o", "BatchMode=yes", *base_args[1:]]
    try:
        completed = subprocess.run(
            args,
            input=remote_script,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=25,
            check=False,
        )
    except FileNotFoundError as exc:
        binary = "sshpass" if password else "ssh"
        raise RuntimeError(f"当前容器缺少 {binary} 命令，无法发起 SSH 采集。") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("SSH 采集超时，请确认节点地址、端口和账号可用。") from exc
    if completed.returncode != 0:
        error_text = (completed.stderr or completed.stdout or "SSH 连接失败").strip().splitlines()
        raise RuntimeError(error_text[-1] if error_text else "SSH 连接失败")
    values: Dict[str, str] = {}
    for line in completed.stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _normalize_system_host_entries(raw_hosts: str, default_port: int) -> list[Dict[str, Any]]:
    entries: list[Dict[str, Any]] = []
    seen: set[tuple[str, int]] = set()
    for token in re.split(r"[\n,;\s]+", raw_hosts or ""):
        item = token.strip()
        if not item:
            continue
        if "@" in item:
            item = item.rsplit("@", 1)[1]
        host = item
        port = default_port
        if item.count(":") == 1:
            candidate_host, candidate_port = item.rsplit(":", 1)
            if candidate_host and candidate_port.isdigit():
                host = candidate_host
                port = int(candidate_port)
        key = (host, port)
        if host and key not in seen:
            entries.append({"host": host, "port": port})
            seen.add(key)
    return entries


def _system_status_payload(values: Dict[str, str], host: str) -> Dict[str, Any]:
    memory_text = _format_bytes(values.get("memory_bytes"))
    k8s_nodes = values.get("k8s_nodes") or "-"
    if k8s_nodes == "0":
        k8s_nodes = "未获取到"
    cards = [
        {"label": "主机名", "value": values.get("hostname") or "-"},
        {"label": "CPU 核数", "value": values.get("cpu_cores") or "-"},
        {"label": "CPU 型号", "value": values.get("cpu_model") or "-"},
        {"label": "服务器架构", "value": values.get("arch") or "-"},
        {"label": "内存容量", "value": memory_text},
        {"label": "K8s Node 节点", "value": k8s_nodes},
        {"label": "K8s Server 版本", "value": values.get("k8s_version") or "未获取到"},
    ]
    details = [
        {"label": "SSH 节点", "value": host},
        {"label": "内核版本", "value": values.get("kernel") or "-"},
        {"label": "K8s Context", "value": values.get("k8s_context") or "-"},
    ]
    return {
        "cards": cards,
        "details": details,
        "captured_at": _beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
        "node": {
            "host": host,
            "hostname": values.get("hostname") or "-",
            "cpu_cores": values.get("cpu_cores") or "-",
            "cpu_model": values.get("cpu_model") or "-",
            "arch": values.get("arch") or "-",
            "kernel": values.get("kernel") or "-",
            "memory_bytes": values.get("memory_bytes") or "",
            "memory": memory_text,
            "k8s_nodes": k8s_nodes,
            "k8s_version": values.get("k8s_version") or "未获取到",
            "k8s_context": values.get("k8s_context") or "-",
            "status": "ok",
        },
    }


def _system_status_multi_payload(nodes: list[Dict[str, Any]], failures: list[Dict[str, str]]) -> Dict[str, Any]:
    captured_at = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")
    if len(nodes) == 1 and not failures:
        single = dict(nodes[0].get("payload", {}))
        single["nodes"] = [nodes[0]["node"]]
        single["failures"] = []
        single["captured_at"] = captured_at
        return single

    total_cpu = 0
    total_memory = 0.0
    for item in nodes:
        node = item.get("node", {})
        try:
            total_cpu += int(str(node.get("cpu_cores", "0")).strip() or "0")
        except ValueError:
            pass
        try:
            total_memory += float(str(node.get("memory_bytes", "0")).strip() or "0")
        except ValueError:
            pass
    node_rows = [item["node"] for item in nodes]
    cards = [
        {"label": "SSH 节点数", "value": str(len(node_rows))},
        {"label": "采集成功", "value": str(len(node_rows))},
        {"label": "采集失败", "value": str(len(failures))},
        {"label": "CPU 总核数", "value": str(total_cpu) if total_cpu else "-"},
        {"label": "内存总量", "value": _format_bytes(total_memory) if total_memory else "-"},
        {"label": "节点地址", "value": " / ".join(str(item.get("host", "-")) for item in node_rows) or "-"},
        {"label": "主机名", "value": " / ".join(str(item.get("hostname", "-")) for item in node_rows) or "-"},
    ]
    details: list[Dict[str, str]] = []
    for index, node in enumerate(node_rows, start=1):
        prefix = f"节点 {index} {node.get('host', '-')}"
        details.extend([
            {"label": prefix, "value": "采集成功"},
            {"label": f"{prefix} 主机名", "value": str(node.get("hostname", "-"))},
            {"label": f"{prefix} CPU 核数", "value": str(node.get("cpu_cores", "-"))},
            {"label": f"{prefix} CPU 型号", "value": str(node.get("cpu_model", "-"))},
            {"label": f"{prefix} 内存容量", "value": str(node.get("memory", "-"))},
            {"label": f"{prefix} 内核版本", "value": str(node.get("kernel", "-"))},
            {"label": f"{prefix} K8s Node 节点", "value": str(node.get("k8s_nodes", "-"))},
            {"label": f"{prefix} K8s Server 版本", "value": str(node.get("k8s_version", "-"))},
        ])
    for failure in failures:
        details.append({"label": f"节点 {failure.get('host', '-')} 采集失败", "value": failure.get("error", "-")})
    return {
        "cards": cards,
        "details": details,
        "nodes": node_rows,
        "failures": failures,
        "captured_at": captured_at,
    }


def _empty_system_status_payload() -> Dict[str, Any]:
    return {
        "cards": [
            {"label": "主机名", "value": "未采集"},
            {"label": "CPU 核数", "value": "未采集"},
            {"label": "CPU 型号", "value": "未采集"},
            {"label": "服务器架构", "value": "未采集"},
            {"label": "内存容量", "value": "未采集"},
            {"label": "K8s Node 节点", "value": "未采集"},
            {"label": "K8s Server 版本", "value": "未采集"},
        ],
        "details": [
            {"label": "SSH 节点", "value": "未采集"},
            {"label": "内核版本", "value": "未采集"},
            {"label": "K8s Context", "value": "未采集"},
        ],
        "captured_at": "未采集",
        "nodes": [],
        "failures": [],
    }


def _load_system_status_settings() -> Dict[str, Any]:
    try:
        payload = json.loads(SYSTEM_STATUS_SETTINGS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return _empty_system_status_payload()
    if not isinstance(payload, dict):
        return _empty_system_status_payload()
    cards = payload.get("cards") if isinstance(payload.get("cards"), list) else []
    details = payload.get("details") if isinstance(payload.get("details"), list) else []
    if not cards and not details:
        return _empty_system_status_payload()
    nodes = payload.get("nodes") if isinstance(payload.get("nodes"), list) else []
    failures = payload.get("failures") if isinstance(payload.get("failures"), list) else []
    return {
        "cards": cards,
        "details": details,
        "captured_at": str(payload.get("captured_at", "")).strip() or "未记录",
        "nodes": nodes,
        "failures": failures,
    }


def _save_system_status_settings(payload: Dict[str, Any]) -> Dict[str, Any]:
    status = {
        "cards": payload.get("cards") if isinstance(payload.get("cards"), list) else [],
        "details": payload.get("details") if isinstance(payload.get("details"), list) else [],
        "captured_at": str(payload.get("captured_at", "")).strip() or _beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
        "nodes": payload.get("nodes") if isinstance(payload.get("nodes"), list) else [],
        "failures": payload.get("failures") if isinstance(payload.get("failures"), list) else [],
    }
    SYSTEM_STATUS_SETTINGS_FILE.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    return status


def _apply_prometheus_settings_defaults(payload: Dict[str, Any]) -> Dict[str, Any]:
    settings = _load_prometheus_settings()
    if not str(payload.get("prometheus_url", "")).strip() and settings.get("prometheus_url"):
        payload["prometheus_url"] = settings["prometheus_url"]
    if not str(payload.get("prometheus_namespace", "")).strip():
        payload["prometheus_namespace"] = settings.get("prometheus_namespace") or "qfusion-admin"
    return payload


def _safe_artifact_name(value: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in value.strip())
    return safe.strip("._") or "unknown"


def _job_artifact_dir(job: BenchmarkJob, kind: str) -> Path:
    path = ARTIFACTS_DIR / _safe_artifact_name(job.db_type) / f"{_safe_artifact_name(kind)}-{job.job_id}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _extract_request_payload() -> Dict[str, Any]:
    payload = request.get_json(silent=True) or request.form.to_dict()
    if "ssl_ca_file_upload" in request.files:
        cert_file = request.files.get("ssl_ca_file_upload")
        if cert_file and cert_file.filename:
            safe_name = secure_filename(cert_file.filename)
            suffix = Path(safe_name).suffix or ".pem"
            target_path = SSL_CERTS_DIR / f"{uuid.uuid4().hex}{suffix}"
            cert_file.save(target_path)
            payload["ssl_ca_file"] = str(target_path)
    return _apply_prometheus_settings_defaults(payload)


def _parse_request_payload(payload: Dict[str, Any]) -> BenchmarkConfig:
    return build_config_from_payload(payload)


def _editable_extra_options(config: BenchmarkConfig) -> str:
    if config.db_type == "DM":
        return strip_extra_options(config.extra_options, DM_TUNING_EXTRA_OPTIONS)
    if config.db_type == "OceanBase":
        return strip_extra_options(config.extra_options, OCEANBASE_BOOLEAN_EXTRA_OPTIONS)
    return config.extra_options


def _editable_config_from_payload(payload: Dict[str, Any]) -> Dict[str, str]:
    config = _parse_request_payload(payload)
    table_size_value = config.pgbench_scale if config.db_type in {"PostgreSQL", "OpenGauss", "Vastbase"} and config.postgres_engine == "pgbench" else config.table_size
    return {
        "db_type": config.db_type,
        "workload": config.workload,
        "concurrency_values": ",".join(str(item) for item in config.threads_values),
        "postgres_engine": config.postgres_engine,
        "tidb_engine": config.tidb_engine,
        "duration_seconds": str(config.duration_seconds),
        "warmup_seconds": str(config.warmup_seconds),
        "report_interval": str(config.report_interval),
        "table_count": str(config.table_count),
        "table_size": str(table_size_value),
        "extra_options": _editable_extra_options(config),
        "auto_prepare": config.auto_prepare,
        "auto_cleanup": config.auto_cleanup,
        "dm_apply_tuning": cli_option_enabled(config.extra_options.split(), DM_TUNING_EXTRA_OPTIONS, False) if config.db_type == "DM" else False,
        "ob_apply_tuning": cli_option_enabled(config.extra_options.split(), OCEANBASE_APPLY_TUNING_OPTIONS, False) if config.db_type == "OceanBase" else False,
        "ob_wait_major_freeze": cli_option_enabled(config.extra_options.split(), OCEANBASE_WAIT_MAJOR_FREEZE_OPTIONS, False) if config.db_type == "OceanBase" else False,
        "ob_gather_stats": cli_option_enabled(config.extra_options.split(), OCEANBASE_GATHER_STATS_OPTIONS, False) if config.db_type == "OceanBase" else False,
        "operation_count": str(config.operation_count),
        "pgbench_scale": str(config.pgbench_scale),
        "pgbench_jobs": str(config.pgbench_jobs),
        "pgbench_fillfactor": str(config.pgbench_fillfactor),
        "pgbench_latency_limit": str(config.pgbench_latency_limit),
        "fio_block_size": config.fio_block_size,
        "fio_iodepth": str(config.fio_iodepth),
        "fio_size_mb": str(config.fio_size_mb),
        "fio_read_percent": str(config.fio_read_percent),
        "es_pipeline": config.es_pipeline,
        "es_challenge": config.es_challenge,
        "es_ingest_percentage": str(config.es_ingest_percentage),
        "es_track_params": config.es_track_params,
        "gaussdb_architecture": config.gaussdb_architecture,
        "gaussdb_cn_hosts": config.gaussdb_cn_hosts,
        "gaussdb_conn_params": config.gaussdb_conn_params,
        "ssl_enabled": config.ssl_enabled,
        "ssl_verify": config.ssl_verify,
        "ssl_ca_file": config.ssl_ca_file,
    }


def _editable_connection_from_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    config = _parse_request_payload(payload)
    return {
        "db_type": config.db_type,
        "db_host": config.host,
        "db_port": str(config.port),
        "db_user": config.user,
        "db_password": config.password,
        "db_name": config.database,
        "ob_tenant_name": config.oceanbase_tenant_name,
        "postgres_schema": config.postgres_schema,
        "gaussdb_cn_hosts": config.gaussdb_cn_hosts,
        "mongo_topology": config.mongo_topology,
        "mongo_hosts": config.mongo_hosts,
        "mongo_replica_set": config.mongo_replica_set,
        "mongo_read_preference": config.mongo_read_preference,
        "mongo_shard_key": config.mongo_shard_key,
        "auth_database": config.auth_database,
        "collection_name": config.collection_name,
        "fio_target_path": config.fio_target_path,
        "fio_ssh_enabled": config.fio_ssh_enabled,
        "fio_ssh_host": config.fio_ssh_host,
        "fio_ssh_port": str(config.fio_ssh_port),
        "fio_ssh_user": config.fio_ssh_user,
        "fio_ssh_password": config.fio_ssh_password,
        "ssl_enabled": config.ssl_enabled,
        "ssl_verify": config.ssl_verify,
        "prometheus_instance": config.prometheus_instance,
    }


CONNECTION_EDITABLE_FIELDS = {
    "db_host",
    "db_port",
    "db_user",
    "db_password",
    "db_name",
    "ob_tenant_name",
    "postgres_schema",
    "gaussdb_cn_hosts",
    "mongo_topology",
    "mongo_hosts",
    "mongo_replica_set",
    "mongo_read_preference",
    "mongo_shard_key",
    "auth_database",
    "collection_name",
    "fio_target_path",
    "fio_ssh_enabled",
    "fio_ssh_host",
    "fio_ssh_port",
    "fio_ssh_user",
    "fio_ssh_password",
    "ssl_enabled",
    "ssl_verify",
    "prometheus_instance",
}
CONNECTION_BOOLEAN_FIELDS = {"fio_ssh_enabled", "ssl_enabled", "ssl_verify"}


def _normalize_connection_update_value(field_name: str, value: Any) -> Any:
    if field_name in CONNECTION_BOOLEAN_FIELDS:
        return "true" if _parse_bool(value, False) else "false"
    return "" if value is None else str(value).strip()


def _payload_value(payload: Dict[str, Any], key: str) -> str:
    value = payload.get(key, "")
    if value is None:
        return ""
    return str(value).strip()


def _connection_info_from_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Readonly task connection snapshot for the UI."""
    db_type = _payload_value(payload, "db_type") or "-"
    fields: list[Dict[str, Any]] = []

    def add(label: str, value: Any, *, secret: bool = False) -> None:
        text = "" if value is None else str(value)
        if not text and secret:
            return
        if not text:
            text = "-"
        fields.append({"label": label, "value": text, "secret": bool(secret)})

    add("数据库类型", db_type)

    if db_type == "FIO":
        add("测试文件路径", _payload_value(payload, "fio_target_path"))
        add("远程 SSH", "启用" if payload.get("fio_ssh_enabled") in {True, "true", "on", "1", "yes"} else "未启用")
        add("SSH 节点地址", _payload_value(payload, "fio_ssh_host"))
        add("SSH 端口", _payload_value(payload, "fio_ssh_port") or "22")
        add("SSH 用户", _payload_value(payload, "fio_ssh_user"))
        add("SSH 密码", _payload_value(payload, "fio_ssh_password"), secret=True)
        return {"db_type": db_type, "fields": fields}

    if db_type == "ElasticSearch":
        add("ElasticSearch 节点", _payload_value(payload, "db_host"))
        add("端口", _payload_value(payload, "db_port"))
    elif db_type == "MongoDB" and _payload_value(payload, "mongo_topology") != "standalone":
        add("MongoDB 拓扑", _payload_value(payload, "mongo_topology"))
        add("节点列表", _payload_value(payload, "mongo_hosts"))
    else:
        add("数据库地址", _payload_value(payload, "db_host"))
        add("端口", _payload_value(payload, "db_port"))

    if db_type == "GaussDB":
        arch = (_payload_value(payload, "gaussdb_architecture") or "centralized").lower()
        add("GaussDB 架构", "分布式" if arch == "distributed" else "集中式")
        if arch == "distributed":
            add("CN 节点列表", _payload_value(payload, "gaussdb_cn_hosts"))

    if db_type == "OceanBase":
        tenant_name = _payload_value(payload, "ob_tenant_name")
        if not tenant_name:
            user_value = _payload_value(payload, "db_user")
            if "@" in user_value:
                tenant_name = user_value.split("@", 1)[1].split("#", 1)[0].split(":", 1)[0].strip()
        add("租户名", tenant_name)
        add("连接用户", _payload_value(payload, "db_user"))
    else:
        add("用户名", _payload_value(payload, "db_user"))
    add("密码", _payload_value(payload, "db_password"), secret=True)

    if db_type == "Oracle":
        add("Service Name", _payload_value(payload, "db_name"))
    elif db_type == "Redis":
        add("DB Index", _payload_value(payload, "db_name"))
    elif db_type == "ElasticSearch":
        add("Rally track", _payload_value(payload, "workload"))
        add("数据灌入比例", f"{_payload_value(payload, 'es_ingest_percentage') or '100'}%")
    else:
        add("数据库名", _payload_value(payload, "db_name"))

    if db_type in {"PostgreSQL", "OpenGauss", "Vastbase"}:
        add("Schema", _payload_value(payload, "postgres_schema"))
    if db_type == "MongoDB":
        add("认证库", _payload_value(payload, "auth_database"))
        add("集合名", _payload_value(payload, "collection_name"))
        add("replicaSet", _payload_value(payload, "mongo_replica_set"))
    if _payload_value(payload, "prometheus_instance"):
        add("当前主库name", _payload_value(payload, "prometheus_instance"))
    if _payload_value(payload, "prometheus_url"):
        add("Prometheus", _payload_value(payload, "prometheus_url"))
    if _payload_value(payload, "prometheus_namespace"):
        add("监控命名空间", _payload_value(payload, "prometheus_namespace"))

    return {"db_type": db_type, "fields": fields}


def _read_text_file(path: Path) -> str | None:
    try:
        return path.read_text(errors="ignore").strip()
    except OSError:
        return None


def _read_int_file(path: Path) -> int | None:
    raw = _read_text_file(path)
    if raw is None or raw == "" or raw == "max":
        return None
    try:
        return int(raw.split()[0])
    except (TypeError, ValueError):
        return None


def _format_bytes(value: Any) -> str:
    if value is None:
        return "-"
    try:
        size = float(str(value).strip().replace(",", ""))
    except (TypeError, ValueError):
        return "-"
    size = max(0.0, size)
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    unit = units[0]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            break
        size /= 1024
    if unit == "B":
        return f"{int(size)} B"
    if size >= 100:
        return f"{size:.0f} {unit}"
    if size >= 10:
        return f"{size:.1f} {unit}"
    return f"{size:.2f} {unit}"


def _format_rate(value: float | int | None) -> str:
    if value is None:
        return "-"
    return f"{_format_bytes(value)}/s"


def _safe_percent(value: float | int | None) -> float:
    if value is None:
        return 0.0
    return round(max(0.0, min(100.0, float(value))), 2)


def _cgroup_relative_path(controller_names: set[str]) -> str:
    raw = _read_text_file(Path("/proc/self/cgroup")) or ""
    for line in raw.splitlines():
        parts = line.split(":", 2)
        if len(parts) != 3:
            continue
        controllers = set(filter(None, parts[1].split(",")))
        if controllers & controller_names:
            return parts[2].lstrip("/")
    return ""


def _cgroup_controller_path(controller: str) -> Path:
    roots = {
        "cpu": ["/sys/fs/cgroup/cpu,cpuacct", "/sys/fs/cgroup/cpu", "/sys/fs/cgroup/cpuacct"],
        "cpuacct": ["/sys/fs/cgroup/cpu,cpuacct", "/sys/fs/cgroup/cpuacct", "/sys/fs/cgroup/cpu"],
        "memory": ["/sys/fs/cgroup/memory"],
        "pids": ["/sys/fs/cgroup/pids"],
        "blkio": ["/sys/fs/cgroup/blkio"],
    }
    controller_aliases = {
        "cpu": {"cpu", "cpuacct"},
        "cpuacct": {"cpu", "cpuacct"},
        "memory": {"memory"},
        "pids": {"pids"},
        "blkio": {"blkio"},
    }
    rel = _cgroup_relative_path(controller_aliases.get(controller, {controller}))
    for root_name in roots.get(controller, [f"/sys/fs/cgroup/{controller}"]):
        root = Path(root_name)
        if not root.exists():
            continue
        if rel:
            nested = root / rel
            if nested.exists():
                return nested
        return root
    return Path("/sys/fs/cgroup")


def _read_cpu_usage_ns() -> int | None:
    stat = _read_text_file(Path("/sys/fs/cgroup/cpu.stat"))
    if stat:
        for line in stat.splitlines():
            key, _, value = line.partition(" ")
            if key == "usage_usec":
                try:
                    return int(value) * 1000
                except ValueError:
                    return None
    return _read_int_file(_cgroup_controller_path("cpuacct") / "cpuacct.usage")


def _read_cpu_limit_cores() -> float:
    cpu_max = _read_text_file(Path("/sys/fs/cgroup/cpu.max"))
    if cpu_max:
        parts = cpu_max.split()
        if len(parts) >= 2 and parts[0] != "max":
            try:
                quota = float(parts[0])
                period = float(parts[1])
                if quota > 0 and period > 0:
                    return max(quota / period, 0.01)
            except ValueError:
                pass
    cpu_path = _cgroup_controller_path("cpu")
    quota = _read_int_file(cpu_path / "cpu.cfs_quota_us")
    period = _read_int_file(cpu_path / "cpu.cfs_period_us")
    if quota and period and quota > 0 and period > 0:
        return max(float(quota) / float(period), 0.01)
    return float(os.cpu_count() or 1)


def _read_memory_usage() -> tuple[int | None, int | None]:
    current = _read_int_file(Path("/sys/fs/cgroup/memory.current"))
    limit = _read_int_file(Path("/sys/fs/cgroup/memory.max"))
    if current is not None:
        return current, limit
    memory_path = _cgroup_controller_path("memory")
    usage = _read_int_file(memory_path / "memory.usage_in_bytes")
    limit = _read_int_file(memory_path / "memory.limit_in_bytes")
    stat = _read_text_file(memory_path / "memory.stat") or ""
    inactive_file = 0
    for line in stat.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0] in {"total_inactive_file", "inactive_file"}:
            try:
                inactive_file = max(inactive_file, int(parts[1]))
            except ValueError:
                pass
    if usage is not None and inactive_file:
        usage = max(0, usage - inactive_file)
    if limit is None or limit > 1 << 60:
        meminfo = _read_text_file(Path("/proc/meminfo")) or ""
        for line in meminfo.splitlines():
            if line.startswith("MemTotal:"):
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        limit = int(parts[1]) * 1024
                    except ValueError:
                        pass
                break
    return usage, limit


def _read_pids() -> tuple[int | None, int | None]:
    current = _read_int_file(Path("/sys/fs/cgroup/pids.current"))
    max_value = _read_int_file(Path("/sys/fs/cgroup/pids.max"))
    if current is not None:
        return current, max_value
    pids_path = _cgroup_controller_path("pids")
    return _read_int_file(pids_path / "pids.current"), _read_int_file(pids_path / "pids.max")


def _read_network_bytes() -> tuple[int, int]:
    rx_total = 0
    tx_total = 0
    raw = _read_text_file(Path("/proc/net/dev")) or ""
    for line in raw.splitlines()[2:]:
        if ":" not in line:
            continue
        name, data = line.split(":", 1)
        if name.strip() == "lo":
            continue
        parts = data.split()
        if len(parts) >= 16:
            try:
                rx_total += int(parts[0])
                tx_total += int(parts[8])
            except ValueError:
                continue
    return rx_total, tx_total


def _read_block_io_bytes() -> tuple[int, int]:
    blkio_path = _cgroup_controller_path("blkio")
    raw = _read_text_file(blkio_path / "blkio.throttle.io_service_bytes") or _read_text_file(blkio_path / "blkio.io_service_bytes") or ""
    read_total = 0
    write_total = 0
    for line in raw.splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        op = parts[-2].lower()
        try:
            value = int(parts[-1])
        except ValueError:
            continue
        if op == "read":
            read_total += value
        elif op == "write":
            write_total += value
    return read_total, write_total


def _sample_container_metric_counters() -> Dict[str, Any]:
    read_bytes, write_bytes = _read_block_io_bytes()
    rx_bytes, tx_bytes = _read_network_bytes()
    return {
        "time": time.monotonic(),
        "cpu_ns": _read_cpu_usage_ns(),
        "network_bytes": rx_bytes + tx_bytes,
        "block_bytes": read_bytes + write_bytes,
        "network_rx": rx_bytes,
        "network_tx": tx_bytes,
        "block_read": read_bytes,
        "block_write": write_bytes,
    }


def _delta_rate(current: Dict[str, Any], previous: Dict[str, Any] | None, key: str) -> float | None:
    if not previous:
        return None
    elapsed = float(current.get("time", 0) - previous.get("time", 0))
    if elapsed <= 0:
        return None
    old_value = previous.get(key)
    new_value = current.get(key)
    if old_value is None or new_value is None:
        return None
    return max(0.0, float(new_value - old_value) / elapsed)


def _build_container_metrics_payload() -> Dict[str, Any]:
    current = _sample_container_metric_counters()
    with CONTAINER_METRICS_LOCK:
        previous = dict(CONTAINER_METRICS_PREVIOUS) if CONTAINER_METRICS_PREVIOUS else None
        if previous is None:
            CONTAINER_METRICS_PREVIOUS.update(current)
            time.sleep(0.12)
            current = _sample_container_metric_counters()
            previous = dict(CONTAINER_METRICS_PREVIOUS)
        cpu_rate_ns = _delta_rate(current, previous, "cpu_ns")
        network_rate = _delta_rate(current, previous, "network_bytes")
        block_rate = _delta_rate(current, previous, "block_bytes")
        CONTAINER_METRICS_PREVIOUS.clear()
        CONTAINER_METRICS_PREVIOUS.update(current)

    cpu_limit_cores = _read_cpu_limit_cores()
    cpu_percent = None
    if cpu_rate_ns is not None and cpu_limit_cores > 0:
        cpu_percent = (cpu_rate_ns / 1_000_000_000.0) / cpu_limit_cores * 100.0

    memory_used, memory_limit = _read_memory_usage()
    memory_percent = None
    if memory_used is not None and memory_limit and memory_limit > 0:
        memory_percent = memory_used / memory_limit * 100.0

    pids_current, pids_max = _read_pids()
    if pids_max and pids_max > 0:
        pids_percent = pids_current / pids_max * 100.0 if pids_current is not None else 0.0
        pids_detail = f"上限 {pids_max}"
    else:
        pids_percent = (pids_current or 0) / 128.0 * 100.0
        pids_detail = "当前容器 PIDs / 线程数"

    network_cap = 100 * 1024 * 1024
    block_cap = 100 * 1024 * 1024
    metrics = [
        {
            "key": "cpu",
            "label": "CPU 使用率",
            "value": f"{(cpu_percent or 0):.2f}%",
            "percent": _safe_percent(cpu_percent),
            "detail": f"CPU 配额 {cpu_limit_cores:.1f} Core",
            "color": "#2f75ff",
        },
        {
            "key": "memory",
            "label": "内存使用率",
            "value": f"{(memory_percent or 0):.2f}%",
            "percent": _safe_percent(memory_percent),
            "detail": f"{_format_bytes(memory_used)} / {_format_bytes(memory_limit)}",
            "color": "#16a7b7",
        },
        {
            "key": "pids",
            "label": "线程 / PIDs",
            "value": str(pids_current or 0),
            "percent": _safe_percent(pids_percent),
            "detail": pids_detail,
            "color": "#7c3aed",
        },
        {
            "key": "network",
            "label": "网络 IO",
            "value": _format_rate(network_rate),
            "percent": _safe_percent((network_rate or 0) / network_cap * 100.0),
            "detail": f"收 {_format_bytes(current.get('network_rx'))} / 发 {_format_bytes(current.get('network_tx'))}",
            "color": "#22c55e",
        },
        {
            "key": "block",
            "label": "块设备 IO",
            "value": _format_rate(block_rate),
            "percent": _safe_percent((block_rate or 0) / block_cap * 100.0),
            "detail": f"读 {_format_bytes(current.get('block_read'))} / 写 {_format_bytes(current.get('block_write'))}",
            "color": "#ff8a1f",
        },
    ]
    return {
        "ok": True,
        "container": os.environ.get("HOSTNAME", "QDBmark"),
        "name": "QDBmark",
        "collected_at": _beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
        "metrics": metrics,
    }


@app.get("/")
def index() -> str:
    return render_template("index.html")


@app.get("/health")
def health() -> Any:
    return jsonify({"status": "ok"})




@app.get("/api/qdbmark-container-metrics")
def qdbmark_container_metrics() -> Any:
    try:
        return jsonify(_build_container_metrics_payload())
    except Exception as exc:
        return jsonify({"ok": False, "error": f"读取 QDBmark 容器指标失败: {exc}"}), 500


@app.get("/api/prometheus-settings")
def get_prometheus_settings() -> Any:
    return jsonify({"ok": True, **_load_prometheus_settings()})


@app.post("/api/system-status")
def collect_system_status() -> Any:
    payload = request.get_json(silent=True) or request.form.to_dict()
    if not isinstance(payload, dict):
        payload = {}
    raw_hosts = str(payload.get("ssh_hosts") or payload.get("ssh_host", "")).strip()
    user = str(payload.get("ssh_user", "root")).strip() or "root"
    password = str(payload.get("ssh_password", ""))
    try:
        default_port = int(str(payload.get("ssh_port", "22")).strip() or "22")
    except ValueError:
        return jsonify({"ok": False, "error": "SSH 端口必须是数字。"}), 400
    if default_port < 1 or default_port > 65535:
        return jsonify({"ok": False, "error": "SSH 端口必须在 1-65535 之间。"}), 400
    entries = _normalize_system_host_entries(raw_hosts, default_port)
    if not entries:
        return jsonify({"ok": False, "error": "请填写至少一个 SSH 节点地址。"}), 400
    nodes: list[Dict[str, Any]] = []
    failures: list[Dict[str, str]] = []
    for entry in entries:
        host = str(entry["host"])
        port = int(entry["port"])
        if port < 1 or port > 65535:
            failures.append({"host": f"{host}:{port}", "error": "SSH 端口必须在 1-65535 之间。"})
            continue
        try:
            values = _run_remote_system_status(host, port, user, password)
            payload_item = _system_status_payload(values, f"{host}:{port}")
            node = dict(payload_item.get("node", {}))
            node["host"] = host
            node["port"] = port
            nodes.append({"node": node, "payload": payload_item})
        except RuntimeError as exc:
            failures.append({"host": f"{host}:{port}", "error": str(exc)})
    if not nodes:
        message = failures[0]["error"] if len(failures) == 1 else "所有 SSH 节点采集失败。"
        return jsonify({"ok": False, "error": message, "failures": failures}), 400
    system_status = _save_system_status_settings(_system_status_multi_payload(nodes, failures))
    return jsonify({"ok": True, **system_status})


@app.post("/api/prometheus-settings")
def update_prometheus_settings() -> Any:
    payload = request.get_json(silent=True) or request.form.to_dict()
    if not isinstance(payload, dict):
        payload = {}
    settings = _save_prometheus_settings(payload)
    return jsonify({"ok": True, **settings})


@app.get("/reports/<path:filename>")
def download_report(filename: str) -> Any:
    safe_name = Path(filename).name
    return send_from_directory(REPORTS_DIR, safe_name, as_attachment=True)


def _save_history() -> None:
    HISTORY_FILE.write_text(json.dumps(REPORT_HISTORY, ensure_ascii=False, indent=2), encoding="utf-8")


def _append_history(summary: Dict[str, Any], report: Dict[str, str], runtime_history: list[Dict[str, Any]] | None = None) -> None:
    entry = {
        "id": uuid.uuid4().hex,
        "summary": summary,
        "report": report,
        "runtime_history": runtime_history or [],
    }
    with HISTORY_LOCK:
        REPORT_HISTORY.append(entry)
        _save_history()


def _get_history(db_type: str | None = None) -> list[Dict[str, Any]]:
    with HISTORY_LOCK:
        entries = list(REPORT_HISTORY)
    if db_type:
        entries = [entry for entry in entries if entry["summary"]["target"]["db_type"] == db_type]
    return sorted(
        entries,
        key=lambda entry: str(entry.get("summary", {}).get("executed_at", "")),
        reverse=True,
    )


def _get_history_by_ids(entry_ids: list[str]) -> list[Dict[str, Any]]:
    if not entry_ids:
        return []
    wanted = set(entry_ids)
    entries = _get_history()
    return [entry for entry in entries if str(entry.get("id", "")) in wanted]


def _delete_history_by_ids(entry_ids: list[str]) -> int:
    if not entry_ids:
        return 0

    wanted = set(entry_ids)
    with HISTORY_LOCK:
        to_delete = [entry for entry in REPORT_HISTORY if str(entry.get("id", "")) in wanted]
        if not to_delete:
            return 0

        remaining = [entry for entry in REPORT_HISTORY if str(entry.get("id", "")) not in wanted]
        remaining_report_names = {
            str(entry.get("report", {}).get("filename", "")).strip()
            for entry in remaining
            if str(entry.get("report", {}).get("filename", "")).strip()
        }
        removable_report_names = {
            str(entry.get("report", {}).get("filename", "")).strip()
            for entry in to_delete
            if str(entry.get("report", {}).get("filename", "")).strip()
            and str(entry.get("report", {}).get("filename", "")).strip() not in remaining_report_names
        }

        REPORT_HISTORY[:] = remaining
        _save_history()

    for filename in removable_report_names:
        report_path = REPORTS_DIR / secure_filename(filename)
        try:
            if report_path.exists():
                report_path.unlink()
        except Exception:
            pass

    return len(to_delete)


def _history_metric_columns(summary: Dict[str, Any]) -> list[tuple[str, str]]:
    target = dict(summary.get("target", {}))
    parameters = dict(summary.get("parameters", {}))
    db_type = str(target.get("db_type", ""))
    workload = str(parameters.get("workload", "")).strip().lower()
    engine = str(parameters.get("benchmark_engine", "")).strip().lower()
    if db_type == "FIO":
        return [("带宽(MB/s)", "tps"), ("IOPS", "qps")]
    if db_type == "MongoDB":
        return [("OPS", "qps")]
    if db_type == "Redis":
        return [("QPS", "qps")]
    if db_type == "ElasticSearch":
        return [("吞吐(ops/s)", "qps")]
    if db_type in {"Oracle", "SQL Server"}:
        return [("TPM", "tps"), ("NOPM", "qps")]
    if db_type in {"DM", "OceanBase", "KingBase", "GaussDB"}:
        return [("tpmTOTAL", "tps"), ("tpmC", "qps")]
    if db_type == "TiDB" and engine == "tiup-bench-tpcc":
        return [("tpmC", "qps")]
    read_workloads = {"oltp_read_only", "oltp_point_select", "select_only", "read", "randread"}
    write_workloads = {"oltp_read_write", "oltp_update_index", "oltp_update_non_index", "tpcb_like", "write", "randwrite", "randrw"}
    if db_type in {"MySQL", "TiDB"} or engine == "sysbench":
        if workload in read_workloads or workload.endswith("read_only"):
            return [("QPS", "qps")]
        if workload in write_workloads or any(token in workload for token in ("read_write", "update", "insert", "delete", "write")):
            return [("TPS", "tps")]
    if db_type in {"PostgreSQL", "OpenGauss", "Vastbase"} and engine == "pgbench":
        return [("QPS", "qps")] if workload in read_workloads or workload.endswith("read_only") else [("TPS", "tps")]
    return [("TPS", "tps"), ("QPS", "qps")]


def _history_metric_text(summary: Dict[str, Any], result: Dict[str, Any]) -> str:
    if not result:
        return "-"
    return " / ".join(f"{label} {result.get(attr, '-')}" for label, attr in _history_metric_columns(summary))


def _history_digest(entry: Dict[str, Any]) -> Dict[str, Any]:
    summary = dict(entry.get("summary", {}))
    target = dict(summary.get("target", {}))
    baseline = dict(summary.get("recommended_baseline") or {})
    return {
        "id": str(entry.get("id", "")),
        "executed_at": str(summary.get("executed_at", "-")),
        "report_title": str(summary.get("report_title", "-")),
        "db_type": str(target.get("db_type", "-")),
        "target_display": (
            target.get("target_path")
            if target.get("db_type") == "FIO"
            else (
                f"{target.get('hosts')}/{target.get('database')}"
                if target.get("db_type") == "MongoDB" and target.get("hosts")
                else f"{target.get('host', '-') }:{target.get('port', '-')}/{target.get('database', '-')}"
            )
        ),
        "workload": str(summary.get("parameters", {}).get("workload", "-")),
        "threads": ", ".join(str(item) for item in summary.get("parameters", {}).get("threads_values", [])),
        "recommended_concurrency": baseline.get("concurrency", "-"),
        "recommended_throughput": _history_metric_text(summary, baseline),
        "baseline_label": baseline.get("baseline_label", "-"),
        "report": dict(entry.get("report", {})),
    }


def _job_target_display(payload: Dict[str, Any]) -> str:
    db_type = str(payload.get("db_type", "MySQL")).strip() or "MySQL"
    if db_type == "FIO":
        target_path = str(payload.get("fio_target_path", "")).strip() or "-"
        if _parse_bool(payload.get("fio_ssh_enabled"), False):
            ssh_user = str(payload.get("fio_ssh_user", "")).strip()
            ssh_host = str(payload.get("fio_ssh_host", "")).strip()
            return f"{ssh_user + '@' if ssh_user else ''}{ssh_host}:{target_path}"
        return target_path
    if db_type == "ElasticSearch":
        host = str(payload.get("db_host", "")).strip() or "-"
        port = str(payload.get("db_port", "")).strip() or "9200"
        if "," in host:
            return host
        return host if ":" in host else f"{host}:{port}"
    if db_type == "MongoDB":
        topology = str(payload.get("mongo_topology", "standalone")).strip() or "standalone"
        database = str(payload.get("db_name", "")).strip() or "-"
        if topology == "standalone":
            host = str(payload.get("db_host", "")).strip() or "-"
            port = str(payload.get("db_port", "")).strip() or "-"
            return f"{host}:{port}/{database}"
        hosts = str(payload.get("mongo_hosts", "")).strip() or "-"
        return f"{hosts}/{database}"
    if db_type == "OceanBase":
        host = str(payload.get("db_host", "")).strip() or "-"
        port = str(payload.get("db_port", "")).strip() or "-"
        database = str(payload.get("db_name", "")).strip() or "-"
        tenant = str(payload.get("ob_tenant_name", "")).strip()
        if not tenant:
            user = str(payload.get("db_user", "")).strip()
            if "@" in user:
                tenant = user.split("@", 1)[1].split("#", 1)[0].split(":", 1)[0].strip()
        suffix = f"@{tenant}" if tenant else ""
        return f"{host}:{port}/{database}{suffix}"
    host = str(payload.get("db_host", "")).strip() or "-"
    port = str(payload.get("db_port", "")).strip() or "-"
    database = str(payload.get("db_name", "")).strip() or "-"
    return f"{host}:{port}/{database}"


def _job_title(db_type: str, kind: str) -> str:
    if kind == "connectivity":
        return f"{db_type} 连通性测试"
    if kind == "benchmark":
        return f"{db_type} 性能测试"
    if kind == "cleanup":
        return f"{db_type} 删除测试数据"
    return f"{db_type} 测试任务"


def _create_job(payload: Dict[str, Any], config: BenchmarkConfig) -> BenchmarkJob:
    db_type = config.db_type
    target_display = _job_target_display(payload)
    job_id = uuid.uuid4().hex
    job_payload = dict(payload)
    job_payload["qdbmark_job_id"] = job_id
    job = BenchmarkJob(
        job_id=job_id,
        kind="draft",
        db_type=db_type,
        target_display=target_display,
        title=_job_title(db_type, "draft"),
        payload=job_payload,
    )
    job.append_log("任务已创建，可在任务详情中发起连通性测试或性能测试。")
    with JOBS_LOCK:
        JOBS[job.job_id] = job
    return job


def _prepare_job_for_run(job: BenchmarkJob, kind: str) -> None:
    with job.lock:
        job.kind = kind
        job.title = _job_title(job.db_type, kind)
        job.status = "queued"
        job.logs = []
        job.summary = None
        job.report = None
        job.instance_info = None
        job.runtime_metrics = None
        job.runtime_history = []
        job.error = None
        job.stop_requested = False
        job.updated_at = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")


def _start_benchmark_job(job: BenchmarkJob) -> None:
    payload = dict(job.payload)
    _prepare_job_for_run(job, kind="benchmark")
    artifact_dir = _job_artifact_dir(job, "benchmark")
    payload["artifact_dir"] = str(artifact_dir)
    job.append_log("性能测试任务已创建，等待开始执行。")
    job.append_log(f"任务运行产物目录: {artifact_dir}")

    def _worker() -> None:
        monitor_stop_event: threading.Event | None = None
        monitor_thread: threading.Thread | None = None
        try:
            with job.lock:
                job.status = "running"
                job.updated_at = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")
            job.append_log("开始解析测试参数。")
            config = _parse_request_payload(payload)
            job.append_log(f"已选择数据库类型: {config.db_type}")
            _populate_instance_info(job, config)
            monitor_stop_event, monitor_thread = _start_runtime_monitor(job, config)
            summary = run_benchmark(config, log_callback=job.append_log, cancel_callback=job.should_stop)
            if job.should_stop():
                raise BenchmarkCancelledError("测试已终止。")
            job.append_log("压测完成，开始生成测试报告。")
            report_path = generate_report(
                summary,
                REPORTS_DIR,
                runtime_history=job.runtime_history,
                customer_name=str(payload.get("customer_name", "")).strip(),
                tester_name=str(payload.get("tester_name", "")).strip(),
                system_info=_load_system_status_settings(),
            )
            with job.lock:
                job.summary = summary.to_dict()
                job.report = {
                    "filename": report_path.name,
                    "url": f"/reports/{report_path.name}",
                }
                job.status = "succeeded"
                job.updated_at = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")
            _append_history(job.summary, job.report, runtime_history=job.runtime_history)
            job.append_log(f"测试报告已生成: {report_path.name}")
        except BenchmarkCancelledError as exc:
            with job.lock:
                job.status = "stopped"
                job.error = str(exc)
                job.updated_at = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")
            job.append_log(str(exc))
        except BenchmarkValidationError as exc:
            with job.lock:
                job.status = "failed"
                job.error = str(exc)
                job.updated_at = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")
            job.append_log(f"测试失败: {exc}")
        except Exception as exc:  # pragma: no cover
            with job.lock:
                job.status = "failed"
                job.error = f"执行测试失败: {exc}"
                job.updated_at = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")
            job.append_log(f"测试失败: {exc}")
        finally:
            if monitor_stop_event is not None:
                monitor_stop_event.set()
            if monitor_thread is not None:
                monitor_thread.join(timeout=1.0)

    threading.Thread(target=_worker, daemon=True).start()


def _start_connectivity_job(job: BenchmarkJob) -> None:
    payload = dict(job.payload)
    _prepare_job_for_run(job, kind="connectivity")
    artifact_dir = _job_artifact_dir(job, "connectivity")
    payload["artifact_dir"] = str(artifact_dir)
    job.append_log("连通性测试任务已创建，等待开始执行。")
    job.append_log(f"任务运行产物目录: {artifact_dir}")

    def _worker() -> None:
        try:
            with job.lock:
                job.status = "running"
                job.updated_at = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")
            job.append_log("开始解析连接测试参数。")
            config = _parse_request_payload(payload)
            job.append_log(f"已选择数据库类型: {config.db_type}")
            if job.should_stop():
                raise BenchmarkCancelledError("测试已终止。")
            run_connectivity_check(config, log_callback=job.append_log)
            with job.lock:
                job.status = "succeeded"
                job.updated_at = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")
            job.append_log("连通性测试完成，可以开始性能测试。")
            _populate_instance_info(job, config)
        except BenchmarkCancelledError as exc:
            with job.lock:
                job.status = "stopped"
                job.error = str(exc)
                job.updated_at = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")
            job.append_log(str(exc))
        except BenchmarkValidationError as exc:
            with job.lock:
                job.status = "failed"
                job.error = str(exc)
                job.updated_at = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")
            job.append_log(f"连接测试失败: {exc}")
        except Exception as exc:  # pragma: no cover
            with job.lock:
                job.status = "failed"
                job.error = f"执行连接测试失败: {exc}"
                job.updated_at = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")
            job.append_log(f"连接测试失败: {exc}")

    threading.Thread(target=_worker, daemon=True).start()


def _start_cleanup_job(job: BenchmarkJob) -> None:
    payload = dict(job.payload)
    _prepare_job_for_run(job, kind="cleanup")
    artifact_dir = _job_artifact_dir(job, "cleanup")
    payload["artifact_dir"] = str(artifact_dir)
    job.append_log("删除测试数据任务已创建，等待开始执行。")
    job.append_log(f"任务运行产物目录: {artifact_dir}")

    def _worker() -> None:
        try:
            with job.lock:
                job.status = "running"
                job.updated_at = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")
            job.append_log("开始解析删除测试数据参数。")
            config = _parse_request_payload(payload)
            job.append_log(f"已选择数据库类型: {config.db_type}")
            cleanup_benchmark_data(config, log_callback=job.append_log, cancel_callback=job.should_stop)
            if job.should_stop():
                raise BenchmarkCancelledError("删除测试数据已终止。")
            with job.lock:
                job.status = "succeeded"
                job.updated_at = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")
            job.append_log("删除测试数据完成，可以重新发起连通性测试或性能测试。")
        except BenchmarkCancelledError as exc:
            with job.lock:
                job.status = "stopped"
                job.error = str(exc)
                job.updated_at = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")
            job.append_log(str(exc))
        except BenchmarkValidationError as exc:
            with job.lock:
                job.status = "failed"
                job.error = str(exc)
                job.updated_at = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")
            job.append_log(f"删除测试数据失败: {exc}")
        except Exception as exc:  # pragma: no cover
            with job.lock:
                job.status = "failed"
                job.error = f"删除测试数据失败: {exc}"
                job.updated_at = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")
            job.append_log(f"删除测试数据失败: {exc}")

    threading.Thread(target=_worker, daemon=True).start()


def _job_sort_key(job: BenchmarkJob) -> tuple[str, str]:
    compact = job.compact()
    return (
        str(compact["created_at"]),
        compact["job_id"],
    )


def _list_jobs() -> list[Dict[str, Any]]:
    with JOBS_LOCK:
        jobs = list(JOBS.values())
    return [job.compact() for job in sorted(jobs, key=_job_sort_key, reverse=False)]


def _delete_jobs_by_ids(job_ids: list[str]) -> tuple[int, list[str]]:
    if not job_ids:
        return 0, []

    wanted = set(job_ids)
    undeletable_titles: list[str] = []
    deleted_count = 0

    with JOBS_LOCK:
        for job_id in list(wanted):
            job = JOBS.get(job_id)
            if job is None:
                continue
            compact = job.compact()
            if compact["status"] in {"queued", "running", "stopping"}:
                undeletable_titles.append(compact["title"] or compact["job_id"])
                continue
            JOBS.pop(job_id, None)
            deleted_count += 1

    return deleted_count, undeletable_titles


def _populate_instance_info(job: BenchmarkJob, config: BenchmarkConfig) -> None:
    try:
        instance_info = collect_instance_info(config)
        if instance_info:
            job.set_instance_info(instance_info)
            job.append_log("已采集实例规格信息。")
    except Exception as exc:
        job.append_log(f"实例规格采集失败，已按允许失败继续: {exc}")


def _start_runtime_monitor(job: BenchmarkJob, config: BenchmarkConfig) -> tuple[threading.Event, threading.Thread] | tuple[None, None]:
    if config.db_type not in {"MySQL", "TiDB", "Redis", "MongoDB", "ElasticSearch", "ClickHouse", "PostgreSQL", "GaussDB", "OpenGauss", "Vastbase", "DM", "SQL Server", "Oracle", "OceanBase", "KingBase", "RabbitMQ", "RocketMQ"}:
        return None, None

    if config.db_type in {"DM", "SQL Server", "Oracle", "OceanBase", "KingBase", "ClickHouse", "RabbitMQ", "RocketMQ"} and not str(config.prometheus_url or "").strip():
        job.append_log("实时监控未启用: 未配置 Prometheus 服务地址。请先在 Prometheus 配置页保存地址，再新建任务。")
        return None, None

    stop_event = threading.Event()

    def _monitor() -> None:
        previous_sample: Dict[str, Any] | None = None
        logged_error = False
        logged_empty = False
        while not stop_event.is_set():
            try:
                current_sample = collect_runtime_sample(config)
                runtime_metrics = format_runtime_metrics(config, current_sample, previous_sample)
                if runtime_metrics:
                    job.set_runtime_metrics(runtime_metrics)
                    job.append_runtime_history(runtime_metrics)
                elif config.prometheus_url and not logged_empty:
                    job.append_log("实时监控暂未返回指标: 请确认 Prometheus 地址、当前主库name、namespace 与指标标签匹配。")
                    logged_empty = True
                previous_sample = current_sample
            except Exception as exc:
                if not logged_error:
                    job.append_log(f"实时监控采集失败，已停止刷新监控指标: {exc}")
                    logged_error = True
                break
            stop_event.wait(2.0)

    monitor_thread = threading.Thread(target=_monitor, daemon=True)
    monitor_thread.start()
    return stop_event, monitor_thread


@app.post("/api/tasks")
def create_task() -> Any:
    try:
        with JOBS_LOCK:
            job_count = len(JOBS)
        if job_count >= MAX_TASKS:
            return jsonify({"ok": False, "error": f"测试任务已达上限（最多 {MAX_TASKS} 个），请先删除已有任务后再新增。"}), 400
        payload = _extract_request_payload()
        config = _parse_request_payload(payload)
        config.validate()
        if config.db_type == "OceanBase":
            payload["db_user"] = config.user
            payload["ob_tenant_name"] = config.oceanbase_tenant_name
        job = _create_job(payload, config)
        return jsonify({"ok": True, "job_id": job.job_id, "job": job.compact()})
    except BenchmarkValidationError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.post("/api/tasks/<job_id>/benchmark-precheck")
def benchmark_precheck(job_id: str) -> Any:
    job = _get_job_or_404(job_id)
    if job is None:
        return jsonify({"ok": False, "error": "未找到对应的测试任务。"}), 404

    try:
        config = _parse_request_payload(dict(job.payload))
        config.validate()
        if config.db_type not in {"MySQL", "TiDB"}:
            return jsonify(
                {
                    "ok": True,
                    "db_type": config.db_type,
                    "needs_confirmation": False,
                    "logs": ["当前任务不是 MySQL/TiDB，已跳过空库校验。"],
                }
            )

        logs: list[str] = []
        inspection = inspect_mysql_target_schema(config, log_callback=logs.append)
        return jsonify(
            {
                "ok": True,
                "db_type": config.db_type,
                "logs": logs,
                **inspection,
            }
        )
    except BenchmarkValidationError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.get("/api/tasks/<job_id>/config")
def get_task_config(job_id: str) -> Any:
    job = _get_job_or_404(job_id)
    if job is None:
        return jsonify({"ok": False, "error": "未找到对应的测试任务。"}), 404

    try:
        editable_config = _editable_config_from_payload(dict(job.payload))
        editable_connection = _editable_connection_from_payload(dict(job.payload))
        connection_info = _connection_info_from_payload(dict(job.payload))
        return jsonify(
            {
                "ok": True,
                "job_id": job.job_id,
                "editable_config": editable_config,
                "editable_connection": editable_connection,
                "connection_info": connection_info,
            }
        )
    except BenchmarkValidationError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.post("/api/tasks/<job_id>/connection")
def update_task_connection(job_id: str) -> Any:
    job = _get_job_or_404(job_id)
    if job is None:
        return jsonify({"ok": False, "error": "未找到对应的测试任务。"}), 404
    if job.status in {"queued", "running", "stopping"}:
        return jsonify({"ok": False, "error": "任务执行中，暂不支持修改连接信息，请先终止当前测试。"}), 400

    incoming = request.get_json(silent=True) or request.form.to_dict()
    if not isinstance(incoming, dict):
        incoming = {}

    updated_payload = dict(job.payload)
    changed_fields: list[str] = []
    for field_name in CONNECTION_EDITABLE_FIELDS:
        if field_name not in incoming:
            continue
        next_value = _normalize_connection_update_value(field_name, incoming.get(field_name))
        if str(updated_payload.get(field_name, "")) != str(next_value):
            changed_fields.append(field_name)
        updated_payload[field_name] = next_value

    if not changed_fields:
        return jsonify({"ok": False, "error": "没有检测到可更新的连接信息。"}), 400

    try:
        config = _parse_request_payload(updated_payload)
        config.validate()
    except BenchmarkValidationError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    if config.db_type == "OceanBase":
        updated_payload["db_user"] = config.user
        updated_payload["ob_tenant_name"] = config.oceanbase_tenant_name

    target_display = _job_target_display(updated_payload)
    with job.lock:
        job.payload = updated_payload
        job.target_display = target_display
        job.kind = "draft"
        job.title = _job_title(job.db_type, "draft")
        job.status = "draft"
        job.summary = None
        job.report = None
        job.instance_info = None
        job.runtime_metrics = None
        job.runtime_history = []
        job.error = None
        job.stop_requested = False
        job.updated_at = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")

    job.append_log(f"已更新连接信息: {target_display}。可重新发起连通性测试。")
    return jsonify(
        {
            "ok": True,
            "job_id": job.job_id,
            "editable_connection": _editable_connection_from_payload(updated_payload),
            "connection_info": _connection_info_from_payload(updated_payload),
            "job": job.compact(),
        }
    )


@app.post("/api/tasks/<job_id>/config")
def update_task_config(job_id: str) -> Any:
    job = _get_job_or_404(job_id)
    if job is None:
        return jsonify({"ok": False, "error": "未找到对应的测试任务。"}), 404
    if job.status in {"queued", "running", "stopping"}:
        return jsonify({"ok": False, "error": "任务执行中，暂不支持修改参数，请先终止当前测试。"}), 400

    incoming = request.get_json(silent=True) or request.form.to_dict()
    if not isinstance(incoming, dict):
        incoming = {}

    editable_fields = {
        "workload",
        "concurrency_values",
        "duration_seconds",
        "warmup_seconds",
        "report_interval",
        "table_count",
        "table_size",
        "extra_options",
        "auto_prepare",
        "auto_cleanup",
        "dm_apply_tuning",
        "ob_apply_tuning",
        "ob_wait_major_freeze",
        "ob_gather_stats",
        "operation_count",
        "tidb_engine",
        "postgres_engine",
        "pgbench_scale",
        "pgbench_jobs",
        "pgbench_fillfactor",
        "pgbench_latency_limit",
        "fio_block_size",
        "fio_iodepth",
        "fio_size_mb",
        "fio_read_percent",
        "es_pipeline",
        "es_challenge",
        "es_ingest_percentage",
        "es_track_params",
        "gaussdb_architecture",
        "gaussdb_cn_hosts",
        "gaussdb_conn_params",
        "ssl_enabled",
        "ssl_verify",
        "ssl_ca_file",
    }

    updated_payload = dict(job.payload)
    changed_fields: list[str] = []
    incoming_postgres_engine = str(incoming.get("postgres_engine", updated_payload.get("postgres_engine", ""))).strip()
    if incoming_postgres_engine == "pgbench" and "table_size" in incoming and "pgbench_scale" not in incoming:
        incoming["pgbench_scale"] = incoming.get("table_size", "")

    for field_name in editable_fields:
        if field_name not in incoming:
            continue
        updated_payload[field_name] = str(incoming.get(field_name, "")).strip()
        changed_fields.append(field_name)

    if not changed_fields:
        return jsonify({"ok": False, "error": "没有检测到可更新的参数。"}), 400

    try:
        config = _parse_request_payload(updated_payload)
        config.validate()
    except BenchmarkValidationError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    with job.lock:
        job.payload = updated_payload
        job.updated_at = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")

    job.append_log(
        "已更新任务参数: "
        f"模型={config.workload}, "
        f"线程列表={','.join(str(item) for item in config.threads_values)}"
        + (f", 测试时长={config.duration_seconds}s" if config.db_type in {"MySQL", "TiDB", "Oracle", "SQL Server", "PostgreSQL", "GaussDB", "OpenGauss", "Vastbase", "FIO", "DM", "OceanBase", "KingBase", "ElasticSearch", "ClickHouse"} else "")
        + (f", 预热={config.warmup_seconds}s" if config.warmup_seconds >= 0 else "")
    )
    return jsonify(
        {
            "ok": True,
            "job_id": job.job_id,
            "editable_config": _editable_config_from_payload(updated_payload),
            "job": job.compact(),
        }
    )


def _get_job_or_404(job_id: str) -> BenchmarkJob | None:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if job is None:
        return None
    return job


def _start_job(job_id: str, kind: str) -> Any:
    job = _get_job_or_404(job_id)
    if job is None:
        return jsonify({"ok": False, "error": "未找到对应的测试任务。"}), 404
    if job.status in {"queued", "running", "stopping"}:
        return jsonify({"ok": False, "error": "当前任务正在执行中，请稍后再试。"}), 400

    if kind == "connectivity":
        _start_connectivity_job(job)
    elif kind == "cleanup":
        _start_cleanup_job(job)
    else:
        _start_benchmark_job(job)
    return jsonify({"ok": True, "job_id": job.job_id, "job": job.compact()})


@app.post("/api/tasks/<job_id>/benchmark")
def run_task_benchmark(job_id: str) -> Any:
    return _start_job(job_id, kind="benchmark")


@app.post("/api/tasks/<job_id>/connectivity")
def run_task_connectivity(job_id: str) -> Any:
    return _start_job(job_id, kind="connectivity")


@app.post("/api/tasks/<job_id>/cleanup")
def run_task_cleanup(job_id: str) -> Any:
    return _start_job(job_id, kind="cleanup")


@app.get("/api/jobs")
def list_jobs() -> Any:
    return jsonify({"ok": True, "jobs": _list_jobs()})


@app.post("/api/tasks/delete")
def delete_jobs() -> Any:
    payload = request.get_json(silent=True) or request.form.to_dict()
    job_ids = payload.get("job_ids") or []
    if isinstance(job_ids, str):
        job_ids = [item.strip() for item in job_ids.split(",") if item.strip()]
    if not isinstance(job_ids, list):
        job_ids = []
    job_ids = [str(item).strip() for item in job_ids if str(item).strip()]
    if not job_ids:
        return jsonify({"ok": False, "error": "请至少选择一条任务。"}), 400

    deleted_count, undeletable_titles = _delete_jobs_by_ids(job_ids)
    if deleted_count == 0 and undeletable_titles:
        return jsonify(
            {
                "ok": False,
                "error": "所选任务正在执行中，请先终止后再删除。",
                "undeletable_titles": undeletable_titles,
            }
        ), 400
    if deleted_count == 0:
        return jsonify({"ok": False, "error": "未找到可删除的任务。"}), 404

    return jsonify(
        {
            "ok": True,
            "deleted_count": deleted_count,
            "undeletable_titles": undeletable_titles,
        }
    )


@app.get("/api/test/<job_id>")
def get_test_status(job_id: str) -> Any:
    job = _get_job_or_404(job_id)
    if job is None:
        return jsonify({"ok": False, "error": "未找到对应的测试任务。"}), 404

    try:
        offset = int(request.args.get("offset", "0"))
    except ValueError:
        offset = 0
    offset = max(offset, 0)
    return jsonify(job.snapshot(offset=offset))


@app.post("/api/test/<job_id>/stop")
def stop_test(job_id: str) -> Any:
    job = _get_job_or_404(job_id)
    if job is None:
        return jsonify({"ok": False, "error": "未找到对应的测试任务。"}), 404
    if job.status not in {"queued", "running"}:
        return jsonify({"ok": False, "error": "当前任务已经结束，无法终止。"}), 400
    with job.lock:
        job.status = "stopping"
        job.updated_at = _beijing_now().strftime("%Y-%m-%d %H:%M:%S")
    job.request_stop()
    job.append_log("已收到终止请求，正在停止当前测试...")
    return jsonify({"ok": True, "job_id": job_id, "job": job.compact()})


@app.get("/api/history")
def get_history() -> Any:
    db_type = str(request.args.get("db_type", "")).strip() or None
    entries = _get_history(db_type=db_type)
    return jsonify(
        {
            "ok": True,
            "entries": [_history_digest(entry) for entry in entries],
        }
    )


@app.post("/api/history/delete")
def delete_history() -> Any:
    payload = request.get_json(silent=True) or request.form.to_dict()
    entry_ids = payload.get("entry_ids") or []
    if isinstance(entry_ids, str):
        entry_ids = [item.strip() for item in entry_ids.split(",") if item.strip()]
    if not isinstance(entry_ids, list):
        entry_ids = []
    entry_ids = [str(item).strip() for item in entry_ids if str(item).strip()]
    if not entry_ids:
        return jsonify({"ok": False, "error": "请至少选择一条测试记录。"}), 400

    deleted_count = _delete_history_by_ids(entry_ids)
    if deleted_count == 0:
        return jsonify({"ok": False, "error": "未找到可删除的测试记录。"}), 404

    return jsonify({"ok": True, "deleted_count": deleted_count})


@app.post("/api/export-report")
def export_report() -> Any:
    payload = request.get_json(silent=True) or request.form.to_dict()
    mode = str(payload.get("mode", "all")).strip() or "all"
    db_type = str(payload.get("db_type", "")).strip() or None
    entry_ids = payload.get("entry_ids") or []
    if isinstance(entry_ids, str):
        entry_ids = [item.strip() for item in entry_ids.split(",") if item.strip()]
    if not isinstance(entry_ids, list):
        entry_ids = []

    if mode not in {"all", "db_type", "selected"}:
        return jsonify({"ok": False, "error": "导出模式不支持。"}), 400
    if mode == "db_type" and not db_type:
        return jsonify({"ok": False, "error": "按数据库类型导出时必须提供 db_type。"}), 400
    if mode == "selected" and not entry_ids:
        return jsonify({"ok": False, "error": "请至少选择一条测试记录。"}), 400

    if mode == "selected":
        entries = _get_history_by_ids([str(item) for item in entry_ids])
    else:
        entries = _get_history(db_type=db_type if mode == "db_type" else None)
    if not entries:
        return jsonify({"ok": False, "error": "当前没有可导出的测试记录。"}), 404

    summaries = [BenchmarkSummary.from_dict(entry["summary"]) for entry in entries]
    runtime_histories = [list(entry.get("runtime_history", [])) for entry in entries]
    requested_report_title = str(payload.get("report_title", "")).strip()
    if mode == "all":
        report_path = generate_batch_report(
            summaries,
            REPORTS_DIR,
            report_title=requested_report_title or "数据库性能测试汇总报告",
            file_prefix="database_benchmark_all",
            runtime_histories=runtime_histories,
            customer_name=str(payload.get("customer_name", "")).strip(),
            tester_name=str(payload.get("tester_name", "")).strip(),
            system_info=_load_system_status_settings(),
        )
    elif mode == "db_type":
        report_path = generate_batch_report(
            summaries,
            REPORTS_DIR,
            report_title=requested_report_title or f"{db_type} 数据库性能测试报告",
            file_prefix=f"database_benchmark_{secure_filename(db_type or 'db')}",
            runtime_histories=runtime_histories,
            customer_name=str(payload.get("customer_name", "")).strip(),
            tester_name=str(payload.get("tester_name", "")).strip(),
            system_info=_load_system_status_settings(),
        )
    else:
        report_path = generate_batch_report(
            summaries,
            REPORTS_DIR,
            report_title=requested_report_title or "所选数据库性能测试报告",
            file_prefix="database_benchmark_selected",
            runtime_histories=runtime_histories,
            customer_name=str(payload.get("customer_name", "")).strip(),
            tester_name=str(payload.get("tester_name", "")).strip(),
            system_info=_load_system_status_settings(),
        )

    return jsonify(
        {
            "ok": True,
            "report": {
                "filename": report_path.name,
                "url": f"/reports/{report_path.name}",
            },
            "count": len(summaries),
        }
    )


if __name__ == "__main__":
    port = int(os.getenv("PORT", "12365"))
    app.run(host="0.0.0.0", port=port, debug=False)
