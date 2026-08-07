from __future__ import annotations

import json
import re
import urllib.request
from datetime import datetime
from time import monotonic
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

BEIJING_TZ = ZoneInfo("Asia/Shanghai")


def _safe_float(value: Any, default: Optional[float] = 0.0) -> Optional[float]:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _clamp_percent(value: Any) -> Optional[float]:
    numeric = _safe_float(value, None)
    if numeric is None:
        return None
    return max(0.0, min(numeric, 100.0))


def _round_percent(value: Any) -> Optional[float]:
    numeric = _clamp_percent(value)
    return round(numeric, 2) if numeric is not None else None


def _prometheus_resource_percent_query(usage_query: str, limit_query: str) -> str:
    return f"clamp_max(({usage_query}) / ({limit_query}) * 100, 100)"


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


def _normalize_prometheus_url(url: str) -> str:
    normalized = str(url or "").strip()
    if not normalized:
        return ""
    if not normalized.startswith(("http://", "https://")):
        normalized = f"http://{normalized}"
    return normalized.rstrip("/")


def _prometheus_escape_label_value(value: Any) -> str:
    raw = str(value or "").strip()
    return raw.replace("\\", "\\\\").replace('"', '\\"')


def _http_get_json(url: str, timeout: float = 3.0) -> Any:
    request = urllib.request.Request(url, headers={"User-Agent": "qfusion-db-benchmark"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8", errors="replace"))
    except Exception:
        return None


def _prometheus_query(base_url: str, query: str) -> Any:
    normalized = _normalize_prometheus_url(base_url)
    if not normalized:
        return None
    url = f"{normalized}/api/v1/query?{urlencode({'query': query})}"
    payload = _http_get_json(url)
    if not isinstance(payload, dict):
        return None
    if payload.get("status") != "success":
        return None
    data = payload.get("data", {})
    if not isinstance(data, dict):
        return None
    result = data.get("result")
    return result if isinstance(result, list) else None


def _prometheus_query_range(base_url: str, query: str, start_ts: int, end_ts: int, step: str = "15s") -> Any:
    normalized = _normalize_prometheus_url(base_url)
    if not normalized:
        return None
    url = f"{normalized}/api/v1/query_range?{urlencode({'query': query, 'start': start_ts, 'end': end_ts, 'step': step})}"
    payload = _http_get_json(url)
    if not isinstance(payload, dict):
        return None
    if payload.get("status") != "success":
        return None
    data = payload.get("data", {})
    if not isinstance(data, dict):
        return None
    result = data.get("result")
    return result if isinstance(result, list) else None


def _prometheus_vector_value(result: Any) -> Optional[float]:
    if not isinstance(result, list) or not result:
        return None
    first = result[0]
    if not isinstance(first, dict):
        return None
    value = first.get("value")
    if not isinstance(value, list) or len(value) < 2:
        return None
    return _safe_float(value[1], None)


def _prometheus_range_last_value(result: Any) -> Optional[float]:
    if not isinstance(result, list) or not result:
        return None
    first = result[0]
    if not isinstance(first, dict):
        return None
    values = first.get("values")
    if not isinstance(values, list) or not values:
        return None
    for item in reversed(values):
        if isinstance(item, list) and len(item) >= 2:
            value = _safe_float(item[1], None)
            if value is not None:
                return value
    return None


def _prometheus_single_value(base_url: str, query: str) -> Optional[float]:
    instant_value = _prometheus_vector_value(_prometheus_query(base_url, query))
    if instant_value is not None:
        return instant_value

    now_ts = int(datetime.now(BEIJING_TZ).timestamp())
    range_value = _prometheus_range_last_value(
        _prometheus_query_range(base_url, query, start_ts=now_ts - 600, end_ts=now_ts, step="15s")
    )
    return range_value


def _build_prometheus_exact_matcher(labels: Dict[str, Any], regex_labels: Optional[set[str]] = None) -> str:
    matcher_parts: List[str] = []
    regex_labels = regex_labels or set()
    for key, value in labels.items():
        normalized = str(value or "").strip()
        if not normalized:
            continue
        operator = "=~" if key in regex_labels else "="
        matcher_parts.append(f'{key}{operator}"{_prometheus_escape_label_value(normalized)}"')
    return ",".join(matcher_parts)


def _prometheus_metric_for_candidates(base_url: str, candidates: List[str], query_templates: List[str]) -> Optional[float]:
    if not base_url or not candidates:
        return None
    label_keys = [
        "container",
        "container_name",
        "name",
        "container_label_com_docker_compose_service",
        "container_label_io_kubernetes_container_name",
        "pod",
        "pod_name",
    ]
    escaped = [_prometheus_label_regex(candidate, prefix_ok=False) for candidate in candidates if candidate]
    if not escaped:
        return None
    regex = "|".join(escaped)
    for label_key in label_keys:
        matcher = f'{label_key}=~".*({regex}).*"'
        for template in query_templates:
            result = _prometheus_query(base_url, template.format(matcher=matcher))
            value = _prometheus_vector_value(result)
            if value is not None:
                return value
    return None


def _prometheus_first_value(base_url: str, queries: List[str]) -> Optional[float]:
    for query in queries:
        if not query:
            continue
        value = _prometheus_single_value(base_url, query)
        if value is not None:
            return value
    return None


def _prometheus_container_runtime_stats(prometheus_url: str, *candidates: Any) -> Optional[Dict[str, Any]]:
    identifier_candidates = _container_candidates(*candidates)
    if not prometheus_url or not identifier_candidates:
        return None

    cpu_percent = _prometheus_metric_for_candidates(
        prometheus_url,
        identifier_candidates,
        [
            'clamp_max(sum(rate(container_cpu_usage_seconds_total{{{matcher}}}[1m])) * 100, 100)',
            'clamp_max(sum(rate(container_cpu_system_seconds_total{{{matcher}}}[1m]) + rate(container_cpu_user_seconds_total{{{matcher}}}[1m])) * 100, 100)',
        ],
    )
    memory_usage = _prometheus_metric_for_candidates(
        prometheus_url,
        identifier_candidates,
        [
            'sum(container_memory_working_set_bytes{{{matcher}}})',
            'sum(container_memory_usage_bytes{{{matcher}}})',
        ],
    )
    io_ops = _prometheus_metric_for_candidates(
        prometheus_url,
        identifier_candidates,
        [
            'sum(rate(container_fs_reads_total{{{matcher}}}[1m])) + sum(rate(container_fs_writes_total{{{matcher}}}[1m]))',
            'sum(rate(container_blkio_device_usage_total{{{matcher},operation=~"Read|Write"}}[1m]))',
        ],
    )

    if cpu_percent is None and memory_usage is None and io_ops is None:
        return None

    return {
        "container_name": identifier_candidates[0],
        "cpu_percent": _round_percent(cpu_percent),
        "memory_usage_bytes": memory_usage,
        "memory_limit_bytes": None,
        "io_ops_total": io_ops,
        "io_ops_rate": io_ops,
        "daemon_host": "prometheus",
    }


def _prometheus_mysql_label_context(config: "BenchmarkConfig") -> Dict[str, str]:
    default_container_names = {
        "MySQL": "mysql",
        "TiDB": "tidb",
        "MongoDB": "mongod",
        "FIO": "fio",
        "SQL Server": "mssql",
        "PostgreSQL": "postgres",
        "OpenGauss": "postgres",
        "DM": "dameng",
        "KingBase": "kingbase",
        "OceanBase": "oceanbase",
        "Redis": "redis",
        "RabbitMQ": "rabbitmq",
        "RocketMQ": "rocketmq",
        "Kafka": "kafka",
        "ElasticSearch": "elasticsearch",
        "ClickHouse": "clickhouse",
        "Oracle": "oracle",
        "Vastbase": "postgres",
    }
    pod_name = str(config.prometheus_instance or config.prometheus_pod or "").strip()
    instance = pod_name or (f"{config.host}:{config.port}" if config.host and config.port else "")
    container_name = str(config.prometheus_container or "").strip() or default_container_names.get(
        config.db_type,
        re.sub(r"[^a-z0-9]+", "", str(config.db_type or "").lower()),
    )
    return {
        "instance": instance,
        "namespace": str(config.prometheus_namespace or "").strip() or "qfusion-admin",
        "pod": pod_name,
        "container": container_name,
    }


def _prometheus_label_regex(value: str, *, prefix_ok: bool = True) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return ""
    if normalized.endswith((".*", ".+")) or any(token in normalized for token in ("|", "(", ")", "[", "]", "^", "$")):
        return normalized
    escaped = re.sub(r"([\\.^$*+?{}\[\]()])", r"\\\1", normalized)
    return f"{escaped}.*" if prefix_ok else escaped


def _prometheus_exporter_matchers(config: "BenchmarkConfig") -> List[str]:
    label_context = _prometheus_mysql_label_context(config)
    namespace = label_context["namespace"]
    raw_values = [
        config.prometheus_instance,
        config.prometheus_pod,
        label_context["pod"],
        f"{config.host}:{config.port}" if config.host and config.port else "",
    ]
    seen: set[str] = set()
    matchers: List[str] = []
    for raw in raw_values:
        value = str(raw or "").strip()
        if not value:
            continue
        regex = _prometheus_label_regex(value)
        key = f"{namespace}|{regex}"
        if key in seen:
            continue
        seen.add(key)
        matcher = _build_prometheus_exact_matcher(
            {"namespace": namespace, "instance": regex},
            regex_labels={"instance"},
        )
        if matcher:
            matchers.append(matcher)
    return matchers


def _prometheus_app_matchers(config: "BenchmarkConfig") -> List[str]:
    label_context = _prometheus_mysql_label_context(config)
    namespace = label_context["namespace"]
    raw_values = [config.prometheus_instance, config.prometheus_pod]
    if config.prometheus_instance:
        raw = str(config.prometheus_instance).strip()
        # Common QFusion pod suffixes: dameng-xxxx-0-0, mongo-xxxx-replica0-0-0,
        # mysql-xxxx00-0. AppName/alertingTargetName usually keeps only the cluster id.
        suffix_patterns = [
            r"(-replica\d+)?-\d+-\d+$",
            r"\d{2}-\d+$",
            r"-\d+$",
        ]
        for pattern in suffix_patterns:
            stripped = re.sub(pattern, "", raw)
            if stripped and stripped != raw:
                raw_values.append(stripped)
    seen: set[str] = set()
    matchers: List[str] = []
    for raw in raw_values:
        value = str(raw or "").strip()
        if not value:
            continue
        regex = _prometheus_label_regex(value)
        key = f"{namespace}|{regex}"
        if key in seen:
            continue
        seen.add(key)
        matcher = _build_prometheus_exact_matcher(
            {"namespace": namespace, "AppName": regex},
            regex_labels={"AppName"},
        )
        if matcher:
            matchers.append(matcher)
    return matchers


def _prometheus_cluster_name_values(config: "BenchmarkConfig") -> List[str]:
    raw_values = [config.prometheus_instance, config.prometheus_pod]
    seen: set[str] = set()
    values: List[str] = []
    suffix_patterns = (
        r"-server-\d+-\d+$",
        r"-nameserver-\d+-\d+$",
        r"-broker-\d+-\d+$",
        r"-replica\d+-\d+-\d+$",
        r"-replica\d+-\d+$",
        r"(-replica\d+)?-\d+-\d+$",
        r"\d{2}-\d+$",
        r"-\d+$",
    )
    for raw in raw_values:
        value = str(raw or "").strip()
        if not value:
            continue
        candidates = [value]
        for pattern in suffix_patterns:
            stripped = re.sub(pattern, "", value)
            if stripped and stripped != value:
                candidates.append(stripped)
        for candidate in candidates:
            if candidate and candidate not in seen:
                seen.add(candidate)
                values.append(candidate)
    return values


def _prometheus_target_matchers(config: "BenchmarkConfig", target_type: str) -> List[str]:
    label_context = _prometheus_mysql_label_context(config)
    namespace = label_context["namespace"]
    seen: set[str] = set()
    matchers: List[str] = []
    for value in _prometheus_cluster_name_values(config):
        regex = _prometheus_label_regex(value)
        key = f"{namespace}|{target_type}|{regex}"
        if key in seen:
            continue
        seen.add(key)
        matcher = _build_prometheus_exact_matcher(
            {"namespace": namespace, "alertingTargetType": target_type, "alertingTargetName": regex},
            regex_labels={"alertingTargetName"},
        )
        if matcher:
            matchers.append(matcher)
    return matchers


def _prometheus_resource_stats_for_matchers(
    config: "BenchmarkConfig",
    matcher_variants: List[tuple[str, str]],
    *,
    include_shmem: bool = False,
    vastbase_dynamic_memory: bool = False,
) -> Optional[Dict[str, Any]]:
    for matcher, scope in matcher_variants:
        if not matcher:
            continue
        cpu_limit_cores = _prometheus_single_value(
            config.prometheus_url,
            f"sum(container_spec_cpu_quota{{{matcher}}} / 100000)",
        )
        cpu_percent = _prometheus_single_value(
            config.prometheus_url,
            _prometheus_resource_percent_query(
                f"sum(rate(container_cpu_usage_seconds_total{{{matcher}}}[3m]))",
                f"sum(container_spec_cpu_quota{{{matcher}}} / 100000)",
            ),
        )
        memory_usage = None
        memory_limit = _prometheus_single_value(
            config.prometheus_url,
            f"sum(qfrds_container_spec_memory_limit_bytes{{{matcher}}})",
        )
        memory_percent = None
        if vastbase_dynamic_memory:
            memory_usage = _prometheus_single_value(
                config.prometheus_url,
                f"sum(pg_memory_detail_dynamic_used_memory{{{matcher}}})",
            )
            dynamic_limit = _prometheus_single_value(
                config.prometheus_url,
                f"sum(pg_memory_detail_max_dynamic_memory{{{matcher}}})",
            )
            if memory_usage is not None and dynamic_limit and dynamic_limit > 0:
                memory_limit = dynamic_limit
                memory_percent = min(memory_usage / dynamic_limit * 100, 100)
        if memory_percent is None:
            usage_expr = f"sum(container_memory_rss{{{matcher}}})"
            if include_shmem:
                usage_expr = f"sum(container_memory_rss{{{matcher}}}) + sum(container_memory_shmem{{{matcher}}})"
            memory_usage = _prometheus_single_value(config.prometheus_url, usage_expr)
            if memory_usage is not None and memory_limit and memory_limit > 0:
                memory_percent = min(memory_usage / memory_limit * 100, 100)
        iops = _prometheus_single_value(
            config.prometheus_url,
            f"sum(rate(container_fs_writes_total{{{matcher}}}[5m]) + rate(container_fs_reads_total{{{matcher}}}[5m])) by (pod)",
        )
        if cpu_percent is None and memory_percent is None and iops is None:
            continue
        return {
            "metrics_source": "prometheus",
            "prometheus_namespace": _prometheus_mysql_label_context(config)["namespace"],
            "prometheus_pod": _prometheus_mysql_label_context(config)["pod"],
            "prometheus_container": scope,
            "container_name": scope,
            "container_cpu_limit_cores": round(cpu_limit_cores, 2) if cpu_limit_cores is not None else None,
            "container_cpu_percent": _round_percent(cpu_percent),
            "container_memory_percent": _round_percent(memory_percent),
            "container_memory_usage_bytes": memory_usage,
            "container_memory_limit_bytes": memory_limit,
            "container_io_ops_rate": round(iops, 2) if iops is not None else None,
        }
    return None


def _prometheus_container_matcher_variants(
    config: "BenchmarkConfig",
    *,
    target_type: str = "",
    container_candidates: Optional[List[str]] = None,
) -> List[tuple[str, str]]:
    label_context = _prometheus_mysql_label_context(config)
    namespace = label_context["namespace"]
    pod_name = label_context["pod"]
    containers = [item for item in (container_candidates or []) if item]
    if label_context["container"] not in containers:
        containers.insert(0, label_context["container"])
    matcher_variants: List[tuple[str, str]] = []
    seen: set[str] = set()

    pod_patterns: List[str] = []
    if pod_name:
        pod_patterns.append(_prometheus_label_regex(pod_name))
        if not re.search(r"-\d+-\d+$", pod_name):
            pod_patterns.append(f"{_prometheus_label_regex(pod_name, prefix_ok=False)}(-[0-9]+)?")
    for pod_pattern in pod_patterns:
        for container_name in containers:
            matcher = _build_prometheus_exact_matcher(
                {"namespace": namespace, "pod": pod_pattern, "container": container_name},
                regex_labels={"pod"},
            )
            if matcher and matcher not in seen:
                seen.add(matcher)
                matcher_variants.append((matcher, f"pod=~{pod_pattern},container={container_name}"))

    for target_matcher in _prometheus_target_matchers(config, target_type) if target_type else []:
        for container_name in containers:
            matcher = f'{target_matcher},container="{_prometheus_escape_label_value(container_name)}"'
            if matcher and matcher not in seen:
                seen.add(matcher)
                matcher_variants.append((matcher, f"{target_matcher},container={container_name}"))

    return matcher_variants


def _prometheus_pod_patterns(pod_name: str) -> List[str]:
    normalized = str(pod_name or "").strip()
    if not normalized:
        return []
    patterns = [normalized]
    if not normalized.endswith("-[0-9]+"):
        patterns.append(f"{normalized}(-[0-9]+)?")
    return patterns


def _prometheus_alerting_matcher(config: "BenchmarkConfig", target_type: str) -> str:
    label_context = _prometheus_mysql_label_context(config)
    target_name = str(config.prometheus_instance or config.prometheus_pod or "").strip()
    return _build_prometheus_exact_matcher(
        {
            "alertingTargetName": target_name,
            "alertingTargetType": target_type,
            "namespace": label_context["namespace"],
        }
    )


def _prometheus_container_matcher(config: "BenchmarkConfig", *, pod_regex: bool = True) -> str:
    label_context = _prometheus_mysql_label_context(config)
    pod_name = label_context["pod"]
    pod_value = pod_name
    if pod_regex and pod_name:
        if pod_name.endswith((".*", ".+")) or any(token in pod_name for token in ("|", "(", ")", "[", "]", "^", "$")):
            pod_value = pod_name
        elif re.search(r"-[0-9]+$", pod_name):
            pod_value = _prometheus_label_regex(pod_name, prefix_ok=False)
        else:
            pod_value = f"{_prometheus_label_regex(pod_name, prefix_ok=False)}(-[0-9]+)?"
    return _build_prometheus_exact_matcher(
        {
            "namespace": label_context["namespace"],
            "pod": pod_value,
            "container": label_context["container"],
        },
        regex_labels={"pod"} if pod_regex else set(),
    )


def _prometheus_iops_for_matcher(base_url: str, matcher: str) -> Optional[float]:
    if not matcher:
        return None
    return _prometheus_single_value(
        base_url,
        f"sum(irate(container_fs_writes_total{{{matcher}}}[3m]) + irate(container_fs_reads_total{{{matcher}}}[3m]))",
    )


def _prometheus_join_matcher(base_matcher: str, *extra_labels: str) -> str:
    labels = [item for item in [base_matcher, *extra_labels] if str(item or "").strip()]
    return ",".join(labels)


def _prometheus_tidb_matchers(config: "BenchmarkConfig") -> List[str]:
    label_context = _prometheus_mysql_label_context(config)
    namespace = label_context["namespace"]
    raw_values = [config.prometheus_instance, config.prometheus_pod, label_context["pod"]]
    seen_values: set[str] = set()
    values: List[str] = []
    for raw in raw_values:
        value = str(raw or "").strip()
        if not value or value in seen_values:
            continue
        seen_values.add(value)
        values.append(value)
        if "-tidb-" in value:
            cluster_name = value.split("-tidb-", 1)[0]
            if cluster_name and cluster_name not in seen_values:
                seen_values.add(cluster_name)
                values.append(cluster_name)

    matchers: List[str] = []
    seen_matchers: set[str] = set()
    for value in values:
        regex = _prometheus_label_regex(value)
        label_sets = [
            {"kubernetes_namespace": namespace, "instance": regex},
            {"namespace": namespace, "instance": regex},
            {"kubernetes_namespace": namespace, "cluster": regex},
            {"namespace": namespace, "cluster": regex},
            {"kubernetes_namespace": namespace, "tidb_cluster": f"{namespace}-{regex}"},
            {"namespace": namespace, "tidb_cluster": f"{namespace}-{regex}"},
        ]
        for labels in label_sets:
            matcher = _build_prometheus_exact_matcher(
                labels,
                regex_labels={"instance", "cluster", "tidb_cluster"},
            )
            if matcher and matcher not in seen_matchers:
                seen_matchers.add(matcher)
                matchers.append(matcher)
    return matchers


def _prometheus_tidb_runtime_rates(config: "BenchmarkConfig") -> tuple[Optional[float], Optional[float]]:
    matchers = _prometheus_tidb_matchers(config)
    qps_queries: List[str] = []
    tps_queries: List[str] = []
    for matcher in matchers:
        server_query_matcher = _prometheus_join_matcher(matcher, 'result="OK"', 'type!="Sleep"')
        statement_query_matcher = _prometheus_join_matcher(
            matcher,
            'type!~"Use|Set|Show|Begin|Commit|Rollback"',
        )
        transaction_matcher = _prometheus_join_matcher(matcher, 'type=~"commit|rollback"')
        executor_transaction_matcher = _prometheus_join_matcher(matcher, 'type=~"Commit|Rollback"')
        qps_queries.extend(
            [
                f"sum(rate(tidb_server_query_total{{{server_query_matcher}}}[5m]))",
                f"sum(rate(tidb_executor_statement_total{{{statement_query_matcher}}}[5m]))",
            ]
        )
        tps_queries.extend(
            [
                f"sum(rate(tidb_session_transaction_duration_seconds_count{{{transaction_matcher}}}[5m]))",
                f"sum(rate(tidb_executor_statement_total{{{executor_transaction_matcher}}}[5m]))",
            ]
        )

    qps_queries.extend(
        [
            'sum(rate(tidb_server_query_total{result="OK",type!="Sleep"}[5m]))',
            'sum(rate(tidb_executor_statement_total{type!~"Use|Set|Show|Begin|Commit|Rollback"}[5m]))',
        ]
    )
    tps_queries.extend(
        [
            'sum(rate(tidb_session_transaction_duration_seconds_count{type=~"commit|rollback"}[5m]))',
            'sum(rate(tidb_executor_statement_total{type=~"Commit|Rollback"}[5m]))',
        ]
    )
    return (
        _prometheus_first_value(config.prometheus_url, qps_queries),
        _prometheus_first_value(config.prometheus_url, tps_queries),
    )


def _prometheus_redis_runtime_sample(config: "BenchmarkConfig") -> Optional[Dict[str, Any]]:
    if not config.prometheus_url:
        return None

    label_context = _prometheus_mysql_label_context(config)
    namespace = label_context["namespace"]
    pod_name = label_context["pod"]
    if not pod_name:
        return None

    pod_pattern = _prometheus_label_regex(pod_name)
    container_name = str(config.prometheus_container or "").strip() or "redis"
    container_matcher = _build_prometheus_exact_matcher(
        {"namespace": namespace, "pod": pod_pattern, "container": container_name},
        regex_labels={"pod"},
    )
    instance_matcher = _build_prometheus_exact_matcher({"instance": pod_name}, regex_labels={"instance"})

    cpu_limit_cores = _prometheus_single_value(
        config.prometheus_url,
        f"sum(container_spec_cpu_quota{{{container_matcher}}} / 100000)",
    )
    cpu_percent = _prometheus_single_value(
        config.prometheus_url,
        _prometheus_resource_percent_query(
            f"sum(irate(container_cpu_usage_seconds_total{{{container_matcher}}}[3m]))",
            f"sum(container_spec_cpu_quota{{{container_matcher}}} / 100000)",
        ),
    )
    memory_used = _prometheus_single_value(
        config.prometheus_url,
        f"max(redis_memory_used_bytes{{{container_matcher}}}) by (pod)",
    )
    memory_max = _prometheus_single_value(
        config.prometheus_url,
        f"max(redis_memory_max_bytes{{{container_matcher}}}) by (pod)",
    )
    if memory_used is None:
        memory_used = _prometheus_first_value(
            config.prometheus_url,
            [f"sum(redis_memory_used_bytes{{{matcher}}}) by (instance)" for matcher in _prometheus_exporter_matchers(config)],
        )
    if memory_max is None:
        memory_max = _prometheus_first_value(
            config.prometheus_url,
            [f"sum(redis_memory_max_bytes{{{matcher}}}) by (instance)" for matcher in _prometheus_exporter_matchers(config)],
        )
    memory_percent = None
    if memory_used is not None and memory_max and memory_max > 0:
        memory_percent = min(memory_used / memory_max * 100, 100)
    if memory_percent is None:
        memory_percent = _prometheus_single_value(
            config.prometheus_url,
            _prometheus_resource_percent_query(
                f"sum(container_memory_rss{{{container_matcher}}})",
                f"sum(qfrds_container_spec_memory_limit_bytes{{{container_matcher}}})",
            ),
        )
    key_count = (
        _prometheus_single_value(config.prometheus_url, f"sum(max_over_time(redis_db_keys{{{instance_matcher}}}[3m]))")
        if instance_matcher
        else None
    )
    qps = _prometheus_first_value(
        config.prometheus_url,
        [
            f"sum(redis_instantaneous_ops_per_sec{{{matcher}}}) by (instance)"
            for matcher in _prometheus_exporter_matchers(config)
        ]
        + [f"sum(redis_instantaneous_ops_per_sec{{{instance_matcher}}})" if instance_matcher else ""],
    )
    iops = _prometheus_iops_for_matcher(config.prometheus_url, container_matcher)

    if cpu_percent is None and memory_percent is None and key_count is None and qps is None and iops is None:
        return None
    return {
        "captured_at": monotonic(),
        "metrics_source": "prometheus",
        "prometheus_namespace": namespace,
        "prometheus_pod": pod_name,
        "prometheus_container": f"pod=~{pod_pattern},container={container_name}",
        "container_name": f"pod=~{pod_pattern},container={container_name}",
        "container_cpu_limit_cores": round(cpu_limit_cores, 2) if cpu_limit_cores is not None else None,
        "container_cpu_percent": _round_percent(cpu_percent),
        "container_memory_percent": _round_percent(memory_percent),
        "container_memory_usage_bytes": memory_used,
        "container_memory_limit_bytes": memory_max,
        "container_io_ops_rate": round(iops, 2) if iops is not None else None,
        "prometheus_qps": round(qps, 2) if qps is not None else None,
        "redis_key_count": round(key_count, 2) if key_count is not None else None,
    }


def _prometheus_oracle_runtime_sample(config: "BenchmarkConfig") -> Optional[Dict[str, Any]]:
    if not config.prometheus_url:
        return None

    label_context = _prometheus_mysql_label_context(config)
    namespace = label_context["namespace"]
    pod_name = label_context["pod"]
    if not pod_name:
        return None

    container_matcher = _prometheus_container_matcher(config, pod_regex=True)
    db_name = re.escape(str(config.database or "").strip().lower())
    group_pattern = f"ora.+_{db_name}.*$|oracle{db_name}.*" if db_name else "ora.+|oracle.+"

    cpu_percent = _prometheus_single_value(
        config.prometheus_url,
        "1e+07 * "
        f'sum by (node) (rate(namedprocess_namegroup_cpu_seconds_total{{groupname=~"{group_pattern}"}}[3m])) / '
        f"on (node) group_right () container_spec_cpu_quota{{{container_matcher}}}",
    )
    memory_usage = _prometheus_single_value(
        config.prometheus_url,
        f'sum by (node) (namedprocess_namegroup_memory_bytes{{groupname=~"{group_pattern}",memtype="proportionalResident"}})',
    )
    memory_limit = _prometheus_single_value(
        config.prometheus_url,
        f"max(qfrds_container_spec_memory_limit_bytes{{{container_matcher}}})",
    )
    memory_percent = min(memory_usage / memory_limit * 100, 100) if memory_usage is not None and memory_limit and memory_limit > 0 else None
    if cpu_percent is None:
        cpu_percent = _prometheus_single_value(
            config.prometheus_url,
            _prometheus_resource_percent_query(
                f"sum(irate(container_cpu_usage_seconds_total{{{container_matcher}}}[3m]))",
                f"sum(container_spec_cpu_quota{{{container_matcher}}} / 100000)",
            ),
        )
    if memory_percent is None:
        memory_percent = _prometheus_single_value(
            config.prometheus_url,
            _prometheus_resource_percent_query(
                f"sum(container_memory_rss{{{container_matcher}}})",
                f"sum(qfrds_container_spec_memory_limit_bytes{{{container_matcher}}})",
            ),
        )
    iops = _prometheus_iops_for_matcher(config.prometheus_url, container_matcher)
    if cpu_percent is None and memory_percent is None and iops is None:
        return None
    return {
        "captured_at": monotonic(),
        "metrics_source": "prometheus",
        "prometheus_namespace": namespace,
        "prometheus_pod": pod_name,
        "prometheus_container": container_matcher,
        "container_name": container_matcher,
        "container_cpu_percent": _round_percent(cpu_percent),
        "container_memory_percent": _round_percent(memory_percent),
        "container_memory_usage_bytes": memory_usage,
        "container_memory_limit_bytes": memory_limit,
        "container_io_ops_rate": round(iops, 2) if iops is not None else None,
    }


def _prometheus_namespace_cluster_matchers(
    config: "BenchmarkConfig",
    label_name: str,
    *,
    extra_labels: Optional[Dict[str, Any]] = None,
) -> List[str]:
    label_context = _prometheus_mysql_label_context(config)
    namespace = label_context["namespace"]
    matchers: List[str] = []
    seen: set[str] = set()
    for value in _prometheus_cluster_name_values(config):
        regex = _prometheus_label_regex(value)
        labels = {"namespace": namespace, label_name: regex}
        if extra_labels:
            labels.update(extra_labels)
        matcher = _build_prometheus_exact_matcher(labels, regex_labels={label_name})
        if matcher and matcher not in seen:
            seen.add(matcher)
            matchers.append(matcher)
    return matchers


def _prometheus_empty_resource_stats(config: "BenchmarkConfig", *, scope: str = "") -> Dict[str, Any]:
    label_context = _prometheus_mysql_label_context(config)
    return {
        "metrics_source": "prometheus",
        "prometheus_namespace": label_context["namespace"],
        "prometheus_pod": label_context["pod"],
        "prometheus_container": scope,
        "container_name": scope,
        "container_cpu_limit_cores": None,
        "container_cpu_percent": None,
        "container_memory_percent": None,
        "container_memory_usage_bytes": None,
        "container_memory_limit_bytes": None,
        "container_io_ops_rate": None,
    }


def _prometheus_clickhouse_runtime_sample(config: "BenchmarkConfig") -> Optional[Dict[str, Any]]:
    if not config.prometheus_url:
        return None

    label_context = _prometheus_mysql_label_context(config)
    chi_matchers = _prometheus_namespace_cluster_matchers(config, "chi")
    if not chi_matchers:
        return None

    qps = _prometheus_first_value(
        config.prometheus_url,
        [f"sum(rate(chi_clickhouse_event_Query{{{matcher}}}[3m]))" for matcher in chi_matchers],
    )
    select_qps = _prometheus_first_value(
        config.prometheus_url,
        [f"sum(rate(chi_clickhouse_event_SelectQuery{{{matcher}}}[3m]))" for matcher in chi_matchers],
    )
    insert_qps = _prometheus_first_value(
        config.prometheus_url,
        [f"sum(rate(chi_clickhouse_event_InsertQuery{{{matcher}}}[3m]))" for matcher in chi_matchers],
    )
    failed_qps = _prometheus_first_value(
        config.prometheus_url,
        [f"sum(rate(chi_clickhouse_event_FailedQuery{{{matcher}}}[3m]))" for matcher in chi_matchers],
    )
    selected_rows_rate = _prometheus_first_value(
        config.prometheus_url,
        [f"sum(rate(chi_clickhouse_event_SelectedRows{{{matcher}}}[3m]))" for matcher in chi_matchers],
    )
    inserted_rows_rate = _prometheus_first_value(
        config.prometheus_url,
        [f"sum(rate(chi_clickhouse_event_InsertedRows{{{matcher}}}[3m]))" for matcher in chi_matchers],
    )
    active_queries = _prometheus_first_value(
        config.prometheus_url,
        [f"sum(chi_clickhouse_metric_Query{{{matcher}}})" for matcher in chi_matchers],
    )

    resource_stats = _prometheus_resource_stats_for_matchers(
        config,
        _prometheus_container_matcher_variants(
            config,
            target_type="ClickHouseCluster",
            container_candidates=["clickhouse"],
        ),
    ) or _prometheus_empty_resource_stats(config, scope=chi_matchers[0])

    resource_stats.update(
        {
            "captured_at": monotonic(),
            "prometheus_namespace": label_context["namespace"],
            "prometheus_pod": label_context["pod"],
            "prometheus_qps": round(qps, 2) if qps is not None else None,
            "prometheus_tps": round(insert_qps, 2) if insert_qps is not None else None,
            "clickhouse_select_qps": round(select_qps, 2) if select_qps is not None else None,
            "clickhouse_insert_qps": round(insert_qps, 2) if insert_qps is not None else None,
            "clickhouse_failed_qps": round(failed_qps, 2) if failed_qps is not None else None,
            "clickhouse_selected_rows_rate": round(selected_rows_rate, 2) if selected_rows_rate is not None else None,
            "clickhouse_inserted_rows_rate": round(inserted_rows_rate, 2) if inserted_rows_rate is not None else None,
            "clickhouse_active_queries": round(active_queries, 2) if active_queries is not None else None,
        }
    )
    if all(
        resource_stats.get(key) is None
        for key in (
            "container_cpu_percent",
            "container_memory_percent",
            "container_io_ops_rate",
            "prometheus_qps",
            "prometheus_tps",
            "clickhouse_failed_qps",
            "clickhouse_active_queries",
        )
    ):
        return None
    return resource_stats


def _prometheus_rabbitmq_runtime_sample(config: "BenchmarkConfig") -> Optional[Dict[str, Any]]:
    if not config.prometheus_url:
        return None

    label_context = _prometheus_mysql_label_context(config)
    business_matchers = _prometheus_target_matchers(config, "RabbitMQCluster")
    if not business_matchers:
        business_matchers = _prometheus_namespace_cluster_matchers(config, "alertingTargetName", extra_labels={"alertingTargetType": "RabbitMQCluster"})
    if not business_matchers:
        return None

    qps = _prometheus_first_value(
        config.prometheus_url,
        [f"sum(rate(rabbitmq_global_messages_received_total{{{matcher}}}[3m]))" for matcher in business_matchers],
    )
    tps = _prometheus_first_value(
        config.prometheus_url,
        [f"sum(rate(rabbitmq_global_messages_delivered_total{{{matcher}}}[3m]))" for matcher in business_matchers]
        + [f"sum(rate(rabbitmq_global_messages_acknowledged_total{{{matcher}}}[3m]))" for matcher in business_matchers],
    )
    ready = _prometheus_first_value(
        config.prometheus_url,
        [f"sum(rabbitmq_queue_messages_ready{{{matcher}}})" for matcher in business_matchers]
        + [f"sum(rabbitmq_queue_messages{{{matcher}}})" for matcher in business_matchers],
    )
    unacked = _prometheus_first_value(
        config.prometheus_url,
        [f"sum(rabbitmq_queue_messages_unacked{{{matcher}}})" for matcher in business_matchers],
    )
    connections = _prometheus_first_value(
        config.prometheus_url,
        [f"sum(rabbitmq_connections{{{matcher}}})" for matcher in business_matchers],
    )
    channels = _prometheus_first_value(
        config.prometheus_url,
        [f"sum(rabbitmq_channels{{{matcher}}})" for matcher in business_matchers],
    )
    consumers = _prometheus_first_value(
        config.prometheus_url,
        [f"sum(rabbitmq_consumers{{{matcher}}})" for matcher in business_matchers],
    )

    resource_stats = _prometheus_resource_stats_for_matchers(
        config,
        _prometheus_container_matcher_variants(
            config,
            target_type="RabbitMQCluster",
            container_candidates=["rabbitmq"],
        ),
    ) or _prometheus_empty_resource_stats(config, scope=business_matchers[0])

    resource_stats.update(
        {
            "captured_at": monotonic(),
            "prometheus_namespace": label_context["namespace"],
            "prometheus_pod": label_context["pod"],
            "prometheus_qps": round(qps, 2) if qps is not None else None,
            "prometheus_tps": round(tps, 2) if tps is not None else None,
            "rabbitmq_messages_ready": round(ready, 2) if ready is not None else None,
            "rabbitmq_messages_unacked": round(unacked, 2) if unacked is not None else None,
            "rabbitmq_connections": round(connections, 2) if connections is not None else None,
            "rabbitmq_channels": round(channels, 2) if channels is not None else None,
            "rabbitmq_consumers": round(consumers, 2) if consumers is not None else None,
        }
    )
    if all(
        resource_stats.get(key) is None
        for key in (
            "container_cpu_percent",
            "container_memory_percent",
            "container_io_ops_rate",
            "prometheus_qps",
            "prometheus_tps",
            "rabbitmq_messages_ready",
            "rabbitmq_connections",
        )
    ):
        return None
    return resource_stats


def _prometheus_rocketmq_runtime_sample(config: "BenchmarkConfig") -> Optional[Dict[str, Any]]:
    if not config.prometheus_url:
        return None

    label_context = _prometheus_mysql_label_context(config)
    business_matchers = _prometheus_target_matchers(config, "RocketMqCluster")
    business_matchers.extend(_prometheus_namespace_cluster_matchers(config, "cluster"))
    unique_matchers: List[str] = []
    seen: set[str] = set()
    for matcher in business_matchers:
        if matcher and matcher not in seen:
            seen.add(matcher)
            unique_matchers.append(matcher)
    business_matchers = unique_matchers
    if not business_matchers:
        return None

    qps = _prometheus_first_value(
        config.prometheus_url,
        [f"sum(rocketmq_brokeruntime_put_tps10{{{matcher}}})" for matcher in business_matchers]
        + [f"sum(rate(rocketmq_brokeruntime_putmessage_times_total{{{matcher}}}[3m]))" for matcher in business_matchers],
    )
    tps = _prometheus_first_value(
        config.prometheus_url,
        [f"sum(rocketmq_brokeruntime_gettotal_tps10{{{matcher}}})" for matcher in business_matchers],
    )
    latency_p99 = _prometheus_first_value(
        config.prometheus_url,
        [f"max(rocketmq_brokeruntime_put_latency_99{{{matcher}}})" for matcher in business_matchers],
    )
    disk_percent = _prometheus_first_value(
        config.prometheus_url,
        [f"max(rocketmq_brokeruntime_commitlog_disk_ratio{{{matcher}}}) * 100" for matcher in business_matchers]
        + [f"sum(rocketmq_filesystem_used_bytes{{{matcher}}}) / sum(rocketmq_filesystem_size_bytes{{{matcher}}}) * 100" for matcher in business_matchers],
    )

    resource_stats = _prometheus_resource_stats_for_matchers(
        config,
        _prometheus_container_matcher_variants(
            config,
            target_type="RocketMqCluster",
            container_candidates=["rocketmq", "broker", "namesrv"],
        ),
    ) or _prometheus_empty_resource_stats(config, scope=business_matchers[0])

    resource_stats.update(
        {
            "captured_at": monotonic(),
            "prometheus_namespace": label_context["namespace"],
            "prometheus_pod": label_context["pod"],
            "prometheus_qps": round(qps, 2) if qps is not None else None,
            "prometheus_tps": round(tps, 2) if tps is not None else None,
            "rocketmq_put_latency_p99": round(latency_p99, 2) if latency_p99 is not None else None,
            "rocketmq_disk_percent": _round_percent(disk_percent),
        }
    )
    if all(
        resource_stats.get(key) is None
        for key in (
            "container_cpu_percent",
            "container_memory_percent",
            "container_io_ops_rate",
            "prometheus_qps",
            "prometheus_tps",
            "rocketmq_put_latency_p99",
            "rocketmq_disk_percent",
        )
    ):
        return None
    return resource_stats


def _prometheus_database_runtime_sample(config: "BenchmarkConfig") -> Optional[Dict[str, Any]]:
    if not config.prometheus_url:
        return None
    if config.db_type == "Redis":
        return _prometheus_redis_runtime_sample(config)
    if config.db_type == "Oracle":
        return _prometheus_oracle_runtime_sample(config)
    if config.db_type == "ClickHouse":
        return _prometheus_clickhouse_runtime_sample(config)
    if config.db_type == "RabbitMQ":
        return _prometheus_rabbitmq_runtime_sample(config)
    if config.db_type == "RocketMQ":
        return _prometheus_rocketmq_runtime_sample(config)
    if config.db_type in {"PostgreSQL", "OpenGauss", "Vastbase"}:
        return _prometheus_postgresql_container_metrics(config)

    target_types = {
        "DM": "DMDBCluster",
        "SQL Server": "MSSQLCluster",
        "MongoDB": "MongoDBCluster",
        "OceanBase": "OceanBaseCluster",
        "KingBase": "KingBaseCluster",
    }
    target_type = target_types.get(config.db_type)
    if not target_type:
        return None

    label_context = _prometheus_mysql_label_context(config)
    container_candidates = {
        "DM": ["dameng"],
        "SQL Server": ["mssql"],
        "MongoDB": ["mongod", "mongodb"],
        "OceanBase": ["oceanbase", "observer"],
        "KingBase": ["kingbase"],
    }.get(config.db_type, [label_context["container"]])
    matchers = _prometheus_container_matcher_variants(
        config,
        target_type=target_type,
        container_candidates=container_candidates,
    )
    if not matchers:
        return None

    resource_stats = _prometheus_resource_stats_for_matchers(config, matchers) or {
        "metrics_source": "prometheus",
        "prometheus_namespace": label_context["namespace"],
        "prometheus_pod": label_context["pod"],
        "prometheus_container": "",
        "container_name": "",
        "container_cpu_limit_cores": None,
        "container_cpu_percent": None,
        "container_memory_percent": None,
        "container_memory_usage_bytes": None,
        "container_memory_limit_bytes": None,
        "container_io_ops_rate": None,
    }

    exporter_matchers = _prometheus_exporter_matchers(config)
    app_matchers = _prometheus_app_matchers(config)
    qps = None
    tps = None
    if config.db_type == "DM":
        tps = _prometheus_first_value(
            config.prometheus_url,
            [f"sum(irate(dmdb_tps_count{{{matcher}}}[3m]))" for matcher in exporter_matchers],
        )
        qps = _prometheus_first_value(
            config.prometheus_url,
            [f"sum(irate(dmdb_ops_count{{{matcher}}}[3m]))" for matcher in exporter_matchers],
        )
    elif config.db_type == "MongoDB":
        qps = _prometheus_first_value(
            config.prometheus_url,
            [f'sum(irate(mongodb_ss_opcounters{{{matcher},legacy_op_type!="command"}}[5m]))' for matcher in exporter_matchers]
            + [f'sum(irate(mongodb_ss_opcounters{{{matcher},legacy_op_type!="command"}}[5m]))' for matcher in app_matchers],
        )
    elif config.db_type == "KingBase":
        tps = _prometheus_first_value(
            config.prometheus_url,
            [f'sum(rate(pg_stat_database_xact_commit{{{matcher},datname!=""}}[5m]))' for matcher in exporter_matchers],
        )
    elif config.db_type == "OceanBase":
        ob_matchers = app_matchers or exporter_matchers
        qps = _prometheus_first_value(
            config.prometheus_url,
            [
                f'sum(rate(ob_sysstat{{{matcher},tenant_name!="sys",stat_id=~"40002|40004|40006|40008|40000"}}[5m]))'
                for matcher in ob_matchers
            ]
            + [
                f'sum(rate(ob_sysstat{{{matcher},stat_id=~"40002|40004|40006|40008|40000"}}[5m]))'
                for matcher in ob_matchers
            ],
        )
        tps = _prometheus_first_value(
            config.prometheus_url,
            [
                f'sum(rate(ob_sysstat{{{matcher},tenant_name!="sys",stat_id="30005"}}[5m]))'
                for matcher in ob_matchers
            ]
            + [f'sum(rate(ob_sysstat{{{matcher},stat_id="30005"}}[5m]))' for matcher in ob_matchers],
        )
        ob_iops = _prometheus_first_value(
            config.prometheus_url,
            [
                f'sum(rate(ob_sysstat{{{matcher},tenant_name!="sys",stat_id=~"60000|60003"}}[5m]))'
                for matcher in ob_matchers
            ]
            + [f'sum(rate(ob_sysstat{{{matcher},stat_id=~"60000|60003"}}[5m]))' for matcher in ob_matchers],
        )
        if ob_iops is not None:
            resource_stats["container_io_ops_rate"] = round(ob_iops, 2)

    resource_stats.update(
        {
            "captured_at": monotonic(),
            "prometheus_namespace": label_context["namespace"],
            "prometheus_pod": label_context["pod"],
            "prometheus_qps": round(qps, 2) if qps is not None else None,
            "prometheus_tps": round(tps, 2) if tps is not None else None,
        }
    )
    if (
        resource_stats.get("container_cpu_percent") is None
        and resource_stats.get("container_memory_percent") is None
        and resource_stats.get("container_io_ops_rate") is None
        and resource_stats.get("prometheus_qps") is None
        and resource_stats.get("prometheus_tps") is None
    ):
        return None
    return resource_stats


def _prometheus_postgresql_container_metrics(config: "BenchmarkConfig") -> Optional[Dict[str, Any]]:
    if not config.prometheus_url:
        return None

    label_context = _prometheus_mysql_label_context(config)
    namespace = label_context["namespace"]
    pod_name = label_context["pod"]
    if not pod_name:
        return None

    pod_patterns = [pod_name]
    if not pod_name.endswith("-[0-9]+"):
        pod_patterns.append(f"{pod_name}(-[0-9]+)?")

    container_candidates: List[Optional[str]] = []
    configured_container = str(config.prometheus_container or "").strip()
    if configured_container:
        container_candidates.append(configured_container)
    if config.db_type == "Vastbase":
        container_candidates.append("vastbase")
    if config.db_type == "OpenGauss":
        container_candidates.append("opengauss")
    # QFusion PostgreSQL-compatible pods may expose the database container as "postgres".
    container_candidates.append("postgres")
    container_candidates.append(None)  # final fallback: pod-level aggregation

    matcher_variants: List[tuple[str, str, str]] = []
    seen: set[tuple[str, str]] = set()
    for pod_pattern in pod_patterns:
        for container_name in container_candidates:
            labels = {"namespace": namespace, "pod": pod_pattern}
            regex_labels = {"pod"}
            scope = f"pod=~{pod_pattern}"
            if container_name:
                labels["container"] = container_name
                scope += f",container={container_name}"
            key = (pod_pattern, container_name or "")
            if key in seen:
                continue
            seen.add(key)
            matcher = _build_prometheus_exact_matcher(labels, regex_labels=regex_labels)
            matcher_variants.append((matcher, matcher, scope))

    for exact_matcher, regex_matcher, scope in matcher_variants:
        if not exact_matcher or not regex_matcher:
            continue
        cpu_percent = _prometheus_single_value(
            config.prometheus_url,
            _prometheus_resource_percent_query(
                f"sum(rate(container_cpu_usage_seconds_total{{{exact_matcher}}}[3m]))",
                f"sum(container_spec_cpu_quota{{{regex_matcher}}} / 100000)",
            ),
        )
        memory_percent = _prometheus_single_value(
            config.prometheus_url,
            _prometheus_resource_percent_query(
                f"sum(container_memory_rss{{{regex_matcher}}}) + sum(container_memory_shmem{{{regex_matcher}}})",
                f"sum(qfrds_container_spec_memory_limit_bytes{{{regex_matcher}}})",
            ),
        ) if config.db_type != "Vastbase" else _prometheus_single_value(
            config.prometheus_url,
            _prometheus_resource_percent_query(
                f"sum(pg_memory_detail_dynamic_used_memory{{{regex_matcher}}})",
                f"sum(pg_memory_detail_max_dynamic_memory{{{regex_matcher}}})",
            ),
        )
        if memory_percent is None:
            memory_percent = _prometheus_single_value(
                config.prometheus_url,
                _prometheus_resource_percent_query(
                    f"sum(container_memory_rss{{{regex_matcher}}})",
                    f"sum(qfrds_container_spec_memory_limit_bytes{{{regex_matcher}}})",
                ),
            )
        iops = _prometheus_single_value(
            config.prometheus_url,
            f"sum(rate(container_fs_writes_total{{{exact_matcher}}}[5m]) + rate(container_fs_reads_total{{{exact_matcher}}}[3m])) by (pod)",
        )
        exporter_matchers = _prometheus_exporter_matchers(config)
        tps = _prometheus_first_value(
            config.prometheus_url,
            [
                f"sum(rate(ccp_stat_database_xact_commit{{{matcher}}}[5m])) + sum(rate(ccp_stat_database_xact_rollback{{{matcher}}}[5m]))"
                for matcher in exporter_matchers
            ]
            + [
                f'sum(rate(pg_stat_database_xact_commit{{{matcher},datname!=""}}[5m]))'
                for matcher in exporter_matchers
            ],
        )
        qps = None
        if config.db_type == "Vastbase":
            qps = _prometheus_first_value(
                config.prometheus_url,
                [f"sum(rate(gauss_workload_sql_count_select_count{{{matcher}}}[5m]))" for matcher in exporter_matchers],
            )
        if cpu_percent is None and memory_percent is None and iops is None and tps is None and qps is None:
            continue
        return {
            "captured_at": monotonic(),
            "metrics_source": "prometheus",
            "prometheus_namespace": namespace,
            "prometheus_pod": pod_name,
            "prometheus_container": scope,
            "container_name": scope,
            "container_cpu_percent": _round_percent(cpu_percent),
            "container_memory_percent": _round_percent(memory_percent),
            "container_io_ops_rate": round(iops, 2) if iops is not None else None,
            "prometheus_qps": round(qps, 2) if qps is not None else None,
            "prometheus_tps": round(tps, 2) if tps is not None else None,
        }

    return None


def _prometheus_mysql_runtime_sample(config: "BenchmarkConfig") -> Optional[Dict[str, Any]]:
    if not config.prometheus_url:
        return None

    label_context = _prometheus_mysql_label_context(config)
    instance_matcher = _build_prometheus_exact_matcher({"instance": label_context["instance"]})
    exporter_matchers = _prometheus_exporter_matchers(config)
    container_matcher = _build_prometheus_exact_matcher(
        {
            "namespace": label_context["namespace"],
            "pod": label_context["pod"],
            "container": label_context["container"],
        },
        regex_labels={"container"},
    )

    if config.db_type == "TiDB":
        qps, tps = _prometheus_tidb_runtime_rates(config)
    else:
        qps = _prometheus_first_value(
            config.prometheus_url,
            [f"irate(mysql_global_status_questions{{{matcher}}}[5m])" for matcher in exporter_matchers]
            + ([f"irate(mysql_global_status_questions{{{instance_matcher}}}[5m])"] if instance_matcher else []),
        )
        tps = _prometheus_first_value(
            config.prometheus_url,
            [f'sum(irate(mysql_global_status_commands_total{{command=~"insert|delete|update|replace",{matcher}}}[5m]))' for matcher in exporter_matchers]
            + ([f'sum(irate(mysql_global_status_commands_total{{command=~"insert|delete|update|replace",{instance_matcher}}}[5m]))'] if instance_matcher else []),
        )
    cpu_limit_cores = (
        _prometheus_single_value(
            config.prometheus_url,
            f"max(container_spec_cpu_quota{{{container_matcher}}} / 100000) by (namespace,pod,container)",
        )
        if container_matcher
        else None
    )
    cpu_percent = (
        _prometheus_single_value(
            config.prometheus_url,
            _prometheus_resource_percent_query(
                f"sum(rate(container_cpu_usage_seconds_total{{{container_matcher}}}[3m]))",
                f"sum(container_spec_cpu_quota{{{container_matcher}}} / 100000)",
            ),
        )
        if container_matcher
        else None
    )
    memory_usage = (
        _prometheus_single_value(
            config.prometheus_url,
            f"max(container_memory_rss{{{container_matcher}}}) by (namespace,pod,container)",
        )
        if container_matcher
        else None
    )
    memory_limit = (
        _prometheus_single_value(
            config.prometheus_url,
            f"max(qfrds_container_spec_memory_limit_bytes{{{container_matcher}}}) by (pod,namespace,container)",
        )
        if container_matcher
        else None
    )
    memory_percent = min(memory_usage / memory_limit * 100, 100) if memory_usage is not None and memory_limit and memory_limit > 0 else None
    iops = (
        _prometheus_single_value(
            config.prometheus_url,
            f"sum(irate(container_fs_writes_total{{{container_matcher}}}[5m]) + irate(container_fs_reads_total{{{container_matcher}}}[5m])) by (pod)",
        )
        if container_matcher
        else None
    )

    return {
        "captured_at": monotonic(),
        "metrics_source": "prometheus",
        "configured_threads": ",".join(str(value) for value in sorted(set(config.threads_values))),
        "prometheus_qps": round(qps, 2) if qps is not None else None,
        "prometheus_tps": round(tps, 2) if tps is not None else None,
        "container_cpu_limit_cores": round(cpu_limit_cores, 2) if cpu_limit_cores is not None else None,
        "container_cpu_percent": _round_percent(cpu_percent),
        "container_memory_percent": _round_percent(memory_percent),
        "container_memory_usage_bytes": memory_usage,
        "container_memory_limit_bytes": memory_limit,
        "container_io_ops_rate": round(iops, 2) if iops is not None else None,
    }
