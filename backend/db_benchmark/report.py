from __future__ import annotations

from datetime import datetime
from io import BytesIO
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo

from docx import Document
from docx.enum.section import WD_SECTION
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_LINE_SPACING, WD_TAB_ALIGNMENT, WD_TAB_LEADER
from docx.shared import Inches, Pt, RGBColor
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.text.paragraph import Paragraph as DocxParagraph

from db_benchmark.benchmark import BenchmarkSummary, ConcurrencyResult

try:
    from PIL import Image, ImageDraw, ImageFont
except ImportError:  # pragma: no cover
    Image = None
    ImageDraw = None
    ImageFont = None


BEIJING_TZ = ZoneInfo("Asia/Shanghai")


def _beijing_now() -> datetime:
    return datetime.now(BEIJING_TZ)


def _set_default_style(document: Document) -> None:
    normal_style = document.styles["Normal"]
    normal_style.font.name = "Arial"
    normal_style.font.size = Pt(10.5)
    for section in document.sections:
        section.top_margin = Inches(0.7)
        section.bottom_margin = Inches(0.7)
        section.left_margin = Inches(0.7)
        section.right_margin = Inches(0.7)


def _set_outline_level(paragraph: Any, level: int) -> None:
    if level <= 0:
        return
    p_pr = paragraph._p.get_or_add_pPr()
    outline = p_pr.find(qn("w:outlineLvl"))
    if outline is None:
        outline = OxmlElement("w:outlineLvl")
        p_pr.append(outline)
    outline.set(qn("w:val"), str(max(level - 1, 0)))


def _add_report_heading(document: Document, text: str, level: int = 1) -> Any:
    """Add a TOC-visible heading without inheriting template heading decorations."""
    paragraph = document.add_paragraph(style="Normal")
    paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER if level == 0 else WD_ALIGN_PARAGRAPH.LEFT
    paragraph_format = paragraph.paragraph_format
    paragraph_format.page_break_before = False
    paragraph_format.keep_with_next = level in (1, 2)
    paragraph_format.keep_together = False
    paragraph_format.space_before = Pt({0: 8, 1: 12, 2: 8, 3: 6}.get(level, 6))
    paragraph_format.space_after = Pt({0: 8, 1: 6, 2: 4, 3: 3}.get(level, 3))

    run = paragraph.add_run(text)
    run.bold = level <= 3
    run.font.name = "Arial"
    try:
        run._element.rPr.rFonts.set(qn("w:eastAsia"), "SimHei" if level <= 1 else "SimSun")
    except Exception:
        pass
    run.font.size = Pt({0: 18, 1: 16, 2: 13, 3: 11}.get(level, 10.5))
    if level > 0:
        _set_outline_level(paragraph, level)
    return paragraph


def _paragraph_has_visible_payload(paragraph: Any) -> bool:
    if paragraph.text.strip():
        return True
    xml = paragraph._element.xml
    return any(token in xml for token in ("<w:drawing", "<w:pict", "<w:object"))


def _trim_trailing_empty_paragraphs(document: Document) -> None:
    while document.paragraphs and not _paragraph_has_visible_payload(document.paragraphs[-1]):
        paragraph = document.paragraphs[-1]
        parent = paragraph._element.getparent()
        if parent is None:
            return
        parent.remove(paragraph._element)


def _write_kv_table(document: Document, items: Iterable[tuple[str, str]]) -> None:
    table = document.add_table(rows=0, cols=2)
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    _set_table_style_safe(table, "Light Grid Accent 1")
    for key, value in items:
        row = table.add_row().cells
        row[0].text = key
        row[1].text = value






def _add_bullet_paragraph_safe(document: Document, text: str) -> None:
    try:
        document.add_paragraph(text, style="List Bullet")
    except Exception:
        document.add_paragraph(f"• {text}")

def _set_table_style_safe(table: Any, preferred: str = "Medium Grid 1 Accent 1") -> None:
    for style_name in (preferred, "Light Grid Accent 1", "Table Grid"):
        try:
            table.style = style_name
            return
        except Exception:
            continue

def _load_font(size: int, bold: bool = False) -> Any:
    if ImageFont is None:
        return None
    candidates = [
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
        "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for candidate in candidates:
        path = Path(candidate)
        if path.exists():
            try:
                return ImageFont.truetype(str(path), size=size)
            except Exception:
                continue
    return ImageFont.load_default()


def _runtime_series_map(runtime_history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    series_meta: Dict[str, Dict[str, Any]] = {}
    series_points: Dict[str, List[tuple[str, float]]] = {}
    for snapshot in runtime_history:
        captured_at = str(snapshot.get("captured_at", "-"))
        for item in snapshot.get("series", []):
            key = str(item.get("key", "")).strip()
            value = item.get("value")
            if not key or value is None:
                continue
            try:
                numeric = float(value)
            except Exception:
                continue
            series_meta[key] = {
                "key": key,
                "label": str(item.get("chart_label") or item.get("label", key)),
                "unit": str(item.get("unit", "")),
                "display": str(item.get("display", numeric)),
            }
            series_points.setdefault(key, []).append((captured_at, numeric))

    result: List[Dict[str, Any]] = []
    for key, meta in series_meta.items():
        points = series_points.get(key, [])
        if not points:
            continue
        result.append(
            {
                "key": key,
                "label": meta["label"],
                "unit": meta["unit"],
                "display": meta["display"],
                "points": points[-24:],
            }
        )
    return result


def _build_runtime_chart_image(series: Dict[str, Any], compact: bool = False) -> Optional[BytesIO]:
    if Image is None or ImageDraw is None:
        return None

    points = list(series.get("points", []))
    if len(points) < 2:
        return None

    if compact:
        width = 620
        height = 320
        padding_left = 54
        padding_right = 22
        padding_top = 58
        padding_bottom = 48
        title_size = 21
        value_size = 17
        small_size = 12
        radius = 18
        title_x = 24
        title_y = 20
        value_x = width - 180
        value_y = 22
        min_x = 24
        max_x = width - 205
    else:
        width = 980
        height = 270
        padding_left = 72
        padding_right = 28
        padding_top = 64
        padding_bottom = 54
        title_size = 28
        value_size = 22
        small_size = 15
        radius = 28
        title_x = 36
        title_y = 24
        value_x = width - 260
        value_y = 24
        min_x = 36
        max_x = width - 280

    image = Image.new("RGB", (width, height), "#f6f8ff")
    draw = ImageDraw.Draw(image)
    title_font = _load_font(title_size, bold=True)
    value_font = _load_font(value_size, bold=True)
    small_font = _load_font(small_size, bold=False)

    draw.rounded_rectangle((0, 0, width - 1, height - 1), radius=radius, fill="#f6f8ff", outline="#dbe5ff", width=2)
    draw.text((title_x, title_y), str(series.get("label", "-")), fill="#13203a", font=title_font)
    draw.text((value_x, value_y), str(series.get("display", "-")), fill="#214fd1", font=value_font)

    values = [point[1] for point in points]
    min_value = min(values)
    max_value = max(values)
    if max_value == min_value:
      max_value = min_value + 1

    chart_left = padding_left
    chart_top = padding_top
    chart_right = width - padding_right
    chart_bottom = height - padding_bottom
    chart_width = chart_right - chart_left
    chart_height = chart_bottom - chart_top

    for step in range(5):
        y = chart_top + (chart_height * step / 4)
        draw.line((chart_left, y, chart_right, y), fill="#e4ecff", width=1)

    path_points: List[tuple[float, float]] = []
    for index, (_, value) in enumerate(points):
        x = chart_left + (chart_width * index / (len(points) - 1))
        normalized = (value - min_value) / (max_value - min_value)
        y = chart_bottom - normalized * chart_height
        path_points.append((x, y))

    area_points = [(chart_left, chart_bottom)] + path_points + [(chart_right, chart_bottom)]
    draw.polygon(area_points, fill="#dbe7ff")
    draw.line(path_points, fill="#2b6dff", width=4, joint="curve")

    last_x, last_y = path_points[-1]
    draw.ellipse((last_x - 6, last_y - 6, last_x + 6, last_y + 6), fill="#2b6dff", outline="#ffffff", width=2)

    unit = str(series.get("unit", "")).strip()
    min_text = f"最小值 {min(values):.2f}{(' ' + unit) if unit else ''}"
    max_text = f"最大值 {max(values):.2f}{(' ' + unit) if unit else ''}"
    draw.text((min_x, height - 34), min_text, fill="#5f739a", font=small_font)
    draw.text((max_x, height - 34), max_text, fill="#5f739a", font=small_font)
    draw.text((chart_left, chart_bottom + 10), points[0][0][-8:], fill="#7a89a8", font=small_font)
    draw.text((chart_right - 54, chart_bottom + 10), points[-1][0][-8:], fill="#7a89a8", font=small_font)

    buffer = BytesIO()
    image.save(buffer, format="PNG")
    buffer.seek(0)
    return buffer


def _build_runtime_chart_grid_image(images: List[BytesIO], columns: int = 3) -> Optional[BytesIO]:
    if Image is None or not images:
        return None

    decoded = []
    for image_stream in images:
        image_stream.seek(0)
        decoded.append(Image.open(image_stream).convert("RGB"))

    columns = max(1, min(columns, len(decoded)))
    gap = 18
    card_width = max(image.width for image in decoded)
    card_height = max(image.height for image in decoded)
    rows = (len(decoded) + columns - 1) // columns
    width = columns * card_width + (columns - 1) * gap
    height = rows * card_height + (rows - 1) * gap
    canvas = Image.new("RGB", (width, height), "#ffffff")

    for index, image in enumerate(decoded):
        x = (index % columns) * (card_width + gap)
        y = (index // columns) * (card_height + gap)
        canvas.paste(image, (x, y))

    buffer = BytesIO()
    canvas.save(buffer, format="PNG")
    buffer.seek(0)
    return buffer


def _write_runtime_charts(
    document: Document,
    runtime_history: List[Dict[str, Any]],
    heading: str,
    level: int = 3,
    summary: Optional[BenchmarkSummary] = None,
) -> None:
    _add_report_heading(document, heading, level=level)
    if not runtime_history:
        document.add_paragraph("本次测试未采集到可用于绘图的实时监控数据。")
        return
    if Image is None:
        document.add_paragraph("当前运行环境未安装 Pillow，无法将实时折线图写入 Word。")
        return

    series_items = _runtime_series_map(runtime_history)
    if summary is not None:
        metric_attrs = {attr for _, attr in _metric_columns(summary)}
        filtered: List[Dict[str, Any]] = []
        for series in series_items:
            key_label = f"{series.get('key', '')} {series.get('label', '')}".lower()
            if "qps" in key_label and "qps" not in metric_attrs:
                continue
            if "tps" in key_label and "tps" not in metric_attrs:
                continue
            filtered.append(series)
        series_items = filtered
    if not series_items:
        document.add_paragraph("本次测试未形成有效的实时监控序列。")
        return

    rendered: List[tuple[Dict[str, Any], BytesIO]] = []
    for series in series_items:
        image_stream = _build_runtime_chart_image(series, compact=True)
        if image_stream is None:
            continue
        rendered.append((series, image_stream))

    if not rendered:
        document.add_paragraph("本次测试未形成有效的实时监控序列。")
        return

    image_streams = [image_stream for _, image_stream in rendered]
    for index in range(0, len(image_streams), 6):
        grid_stream = _build_runtime_chart_grid_image(image_streams[index:index + 6], columns=3)
        if grid_stream is None:
            continue
        paragraph = document.add_paragraph()
        if index:
            paragraph.paragraph_format.page_break_before = True
        paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER
        paragraph.paragraph_format.keep_together = True
        paragraph.paragraph_format.space_before = Pt(2)
        paragraph.paragraph_format.space_after = Pt(6)
        paragraph.add_run().add_picture(grid_stream, width=Inches(6.45))

    _write_runtime_metric_conclusions(document, [series for series, _ in rendered])


def _series_numeric_values(series: Dict[str, Any]) -> List[float]:
    values: List[float] = []
    for _, value in series.get("points", []):
        try:
            values.append(float(value))
        except Exception:
            continue
    return values


def _format_percent_value(value: float) -> str:
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _write_runtime_metric_conclusions(document: Document, series_items: List[Dict[str, Any]]) -> None:
    cpu_series: Optional[Dict[str, Any]] = None
    for series in series_items:
        label_text = f"{series.get('key', '')} {series.get('label', '')}".lower()
        if "cpu" in label_text and ("使用率" in str(series.get("label", "")) or "usage" in label_text or "%" in str(series.get("unit", ""))):
            cpu_series = series
            break
    if cpu_series is None:
        return
    values = _series_numeric_values(cpu_series)
    if not values:
        return
    avg_value = sum(values) / len(values)
    peak_value = max(values)
    if avg_value >= 90:
        conclusion = (
            f"CPU 使用率平均 {_format_percent_value(avg_value)}%，峰值 {_format_percent_value(peak_value)}%，"
            "压测期间 CPU 基本处于高负载状态，继续提升吞吐时 CPU 可能成为主要瓶颈。"
        )
    elif avg_value >= 70:
        conclusion = (
            f"CPU 使用率平均 {_format_percent_value(avg_value)}%，峰值 {_format_percent_value(peak_value)}%，"
            "资源利用率较高，建议结合延迟、吞吐拐点和数据库等待事件判断是否接近性能上限。"
        )
    else:
        conclusion = (
            f"CPU 使用率平均 {_format_percent_value(avg_value)}%，峰值 {_format_percent_value(peak_value)}%，"
            "CPU 仍有余量；若吞吐未继续提升，应优先排查 IO、锁等待、连接数或数据库参数配置。"
        )
    title = document.add_paragraph()
    title.paragraph_format.space_before = Pt(4)
    title.paragraph_format.space_after = Pt(2)
    run = title.add_run("监控结论")
    _style_text_run(run, 10.5, bold=True, color="1f4e79")
    _add_bullet_paragraph_safe(document, conclusion)


def _normalized_workload(summary: BenchmarkSummary) -> str:
    return str(summary.parameters.get("workload", "")).strip().lower()


def _is_read_metric_workload(workload: str) -> bool:
    read_tokens = {"oltp_read_only", "oltp_point_select", "select_only", "read", "randread"}
    return workload in read_tokens or workload.endswith("read_only")


def _is_write_metric_workload(workload: str) -> bool:
    write_tokens = {"oltp_read_write", "oltp_update_index", "oltp_update_non_index", "tpcb_like", "write", "randwrite", "randrw"}
    return workload in write_tokens or any(token in workload for token in ("read_write", "update", "insert", "delete", "write"))


def _metric_columns(summary: BenchmarkSummary) -> list[tuple[str, str]]:
    db_type = str(summary.target.get("db_type", ""))
    workload = _normalized_workload(summary)
    engine = str(summary.parameters.get("benchmark_engine", "")).strip().lower()

    if db_type == "FIO":
        return [("带宽(MB/s)", "tps"), ("IOPS", "qps")]
    if db_type == "MongoDB":
        return [("OPS", "qps")]
    if db_type == "Redis":
        return [("QPS", "qps")]
    if db_type == "Kafka":
        return [("records/s", "qps"), ("MB/s", "tps")]
    if db_type == "RocketMQ":
        return [("msg/s", "qps"), ("MB/s", "tps")]
    if db_type == "RabbitMQ":
        return [("msg/s", "qps"), ("MB/s", "tps")]
    if db_type == "ElasticSearch":
        return [("吞吐(ops/s)", "qps")]
    if db_type == "ClickHouse":
        return [("QPS", "qps")]
    if db_type in {"Oracle", "SQL Server"}:
        return [("TPM", "tps"), ("NOPM", "qps")]
    if db_type in {"DM", "OceanBase", "KingBase", "GaussDB"}:
        return [("tpmTOTAL", "tps"), ("tpmC", "qps")]
    if db_type == "TiDB" and engine == "tiup-bench-tpcc":
        return [("tpmC", "qps")]

    if db_type in {"MySQL", "TiDB"} or engine == "sysbench":
        if _is_read_metric_workload(workload):
            return [("QPS", "qps")]
        if _is_write_metric_workload(workload):
            return [("TPS", "tps")]
    if db_type in {"PostgreSQL", "OpenGauss", "Vastbase"} and engine == "pgbench":
        if _is_read_metric_workload(workload):
            return [("QPS", "qps")]
        return [("TPS", "tps")]

    return [("TPS", "tps"), ("QPS", "qps")]


def _throughput_labels(summary: BenchmarkSummary) -> tuple[str, str]:
    columns = _metric_columns(summary)
    if len(columns) == 1:
        return columns[0][0], columns[0][0]
    return columns[0][0], columns[1][0]


def _metric_value(result: ConcurrencyResult, attr: str) -> float:
    return float(getattr(result, attr, 0.0) or 0.0)


def _metric_display(result: ConcurrencyResult, attr: str) -> str:
    value = _metric_value(result, attr)
    return str(round(value, 2)).rstrip("0").rstrip(".") if value % 1 else str(int(value))


def _metric_summary_text(summary: BenchmarkSummary, result: Optional[ConcurrencyResult]) -> str:
    if not result:
        return "-"
    return " / ".join(f"{label} {_metric_display(result, attr)}" for label, attr in _metric_columns(summary))


def _count_columns(summary: BenchmarkSummary) -> list[tuple[str, str]]:
    db_type = str(summary.target.get("db_type", ""))
    metric_columns = _metric_columns(summary)
    metric_attrs = {attr for _, attr in metric_columns}
    if db_type in {"MongoDB", "Redis", "Kafka", "RocketMQ", "RabbitMQ", "ElasticSearch", "ClickHouse", "FIO"}:
        return [("操作数", "total_events")]
    if metric_attrs == {"qps"}:
        return [("查询/操作数", "total_queries")]
    if metric_attrs == {"tps"}:
        return [("事务/操作数", "total_transactions")]
    return [("事务/操作数", "total_transactions"), ("查询/操作数", "total_queries")]


def _metric_sort_key(summary: BenchmarkSummary, result: ConcurrencyResult) -> tuple[float, float, float]:
    metric_columns = _metric_columns(summary)
    primary = _metric_value(result, metric_columns[0][1]) if metric_columns else 0.0
    secondary = _metric_value(result, metric_columns[1][1]) if len(metric_columns) > 1 else 0.0
    return primary, secondary, float(result.success_rate or 0.0)


def _write_results_table(document: Document, summary: BenchmarkSummary, title: str, level: int = 3) -> None:
    _add_report_heading(document, title, level=level)
    count_columns = _count_columns(summary)
    metric_columns = _metric_columns(summary)
    headers = ["线程数", *[label for label, _ in count_columns], "成功率", *[label for label, _ in metric_columns], "平均延迟(ms)", "P95(ms)", "结论"]
    table = document.add_table(rows=1, cols=len(headers))
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    _set_table_style_safe(table, "Medium Grid 1 Accent 1")
    for index, header in enumerate(headers):
        table.rows[0].cells[index].text = header

    for result in summary.results:
        values = [
            str(result.concurrency),
            *[str(getattr(result, attr, 0)) for _, attr in count_columns],
            f"{result.success_rate}%",
            *[_metric_display(result, attr) for _, attr in metric_columns],
            str(result.avg_latency_ms),
            str(result.p95_latency_ms),
            result.baseline_label,
        ]
        row = table.add_row().cells
        for index, value in enumerate(values):
            row[index].text = value


def _write_baseline_table(
    document: Document,
    summary: BenchmarkSummary,
    recommended: Optional[ConcurrencyResult],
    title: str = "性能基线表",
    level: int = 3,
) -> None:
    _add_report_heading(document, title, level=level)
    metric_columns = _metric_columns(summary)
    headers = ["线程数", *[f"基线 {label}" for label, _ in metric_columns], "P95(ms)", "平均延迟(ms)", "成功率", "稳定性评级", "是否推荐"]
    table = document.add_table(rows=1, cols=len(headers))
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    _set_table_style_safe(table, "Medium Grid 1 Accent 1")
    for index, header in enumerate(headers):
        table.rows[0].cells[index].text = header

    for result in summary.results:
        values = [
            str(result.concurrency),
            *[_metric_display(result, attr) for _, attr in metric_columns],
            str(result.p95_latency_ms),
            str(result.avg_latency_ms),
            f"{result.success_rate}%",
            result.baseline_label,
            "是" if recommended and recommended.concurrency == result.concurrency else "否",
        ]
        row = table.add_row().cells
        for index, value in enumerate(values):
            row[index].text = value


def _build_conclusion(summary: BenchmarkSummary) -> list[str]:
    if not summary.results:
        return ["本次测试未产生有效结果。"]

    top_result = _summary_best(summary)
    best_baseline = summary.recommended_baseline
    conclusion = [
        f"本次测试共覆盖 {len(summary.results)} 组线程配置，峰值吞吐出现在 {top_result.concurrency if top_result else '-'} 线程，{_metric_summary_text(summary, top_result)}。",
    ]
    if best_baseline:
        conclusion.append(
            f"建议将 {best_baseline.concurrency} 线程作为当前性能基线，成功率 {best_baseline.success_rate}%，P95 延迟 {best_baseline.p95_latency_ms} ms。"
        )
    failures = [result for result in summary.results if result.failed_requests > 0]
    if failures:
        conclusion.append("部分线程组出现执行失败，建议优先复查压测数据、数据库资源和实例参数。")
    else:
        conclusion.append("全部线程组均完成执行，可将报告中的基线表作为后续版本回归对照。")
    return conclusion


def _engine_label(summary: BenchmarkSummary) -> str:
    db_type = summary.target.get("db_type")
    if db_type == "MongoDB":
        return "YCSB workload"
    if db_type == "Redis":
        return "redis-benchmark 命令"
    if db_type == "Kafka":
        return "Kafka 压测模型"
    if db_type == "RocketMQ":
        return "RocketMQ 压测模型"
    if db_type == "RabbitMQ":
        return "RabbitMQ 压测模型"
    if db_type == "FIO":
        return "FIO 模式"
    if db_type == "ElasticSearch":
        return "Rally track"
    if db_type == "ClickHouse":
        return "ClickHouse 查询模型"
    if db_type in {"Oracle", "SQL Server"}:
        return "HammerDB 模型"
    if db_type == "GaussDB":
        return "BenchmarkSQL 模型"
    if db_type in {"PostgreSQL", "OpenGauss", "Vastbase"}:
        return "pgbench 模型" if summary.parameters.get("benchmark_engine") == "pgbench" else "sysbench 模型"
    return "sysbench 模型"


def _build_parameter_items(summary: BenchmarkSummary) -> list[tuple[str, str]]:
    parameters = summary.parameters
    if summary.target.get("db_type") == "FIO":
        return [
            ("压测引擎", str(parameters.get("benchmark_engine", "fio"))),
            (_engine_label(summary), str(parameters.get("workload", "-"))),
            ("执行方式", "SSH 远程执行" if parameters.get("fio_ssh_enabled") else "本地执行"),
            ("测试时长(秒)", str(parameters.get("duration_seconds", 0))),
            ("预热时长(秒)", str(parameters.get("warmup_seconds", 0))),
            ("块大小", str(parameters.get("fio_block_size", "-"))),
            ("iodepth", str(parameters.get("fio_iodepth", 0))),
            ("测试文件大小(MB)", str(parameters.get("fio_size_mb", 0))),
            ("Direct IO", "是" if parameters.get("fio_direct") else "否"),
            ("混合读比例(%)", str(parameters.get("fio_read_percent", 0))),
            ("测试后删除测试文件", "是" if parameters.get("auto_cleanup") else "否"),
            ("线程列表", ", ".join(str(item) for item in parameters.get("threads_values", []))),
        ]
    if summary.target.get("db_type") == "ElasticSearch":
        return [
            ("压测引擎", "Elastic Rally"),
            ("Rally pipeline", str(parameters.get("es_pipeline", "benchmark-only"))),
            (_engine_label(summary), str(parameters.get("workload", "-"))),
            ("Rally challenge", str(parameters.get("es_challenge") or "默认")),
            ("Track 参数", str(parameters.get("es_track_params") or "-")),
            ("测试时长(秒)", f"{parameters.get('duration_seconds', 0)}（Rally 实际执行时长由 track/challenge schedule 决定）"),
            ("报告间隔(秒)", str(parameters.get("report_interval", 0))),
            ("bulk size", str(parameters.get("table_size", 0))),
            ("SSL 连接", "是" if parameters.get("ssl_enabled") else "否"),
            ("严格校验证书", "是" if parameters.get("ssl_verify") else "否"),
            ("并发客户端列表", ", ".join(str(item) for item in parameters.get("threads_values", []))),
        ]
    if summary.target.get("db_type") == "RocketMQ":
        return [
            ("压测引擎", str(parameters.get("benchmark_engine", "rocketmq-example-benchmark"))),
            (_engine_label(summary), str(parameters.get("workload", "-"))),
            ("NameServer", str(parameters.get("rocketmq_namesrv") or summary.target.get("host", "-"))),
            ("Topic", str(summary.target.get("database", "-"))),
            ("测试时长(秒)", str(parameters.get("duration_seconds", 0))),
            ("预热时长(秒)", str(parameters.get("warmup_seconds", 0))),
            ("消息总数", str(parameters.get("operation_count", 0))),
            ("消息大小(bytes)", str(parameters.get("table_size", 0))),
            ("报告间隔(秒)", str(parameters.get("report_interval", 0))),
            ("ACL", "是" if str(summary.target.get("user", "-")).strip() not in {"", "-"} else "否"),
            ("consumer 前置写入消息", "是" if parameters.get("auto_prepare") else "否"),
            ("并发线程列表", ", ".join(str(item) for item in parameters.get("threads_values", []))),
        ]
    if summary.target.get("db_type") == "RabbitMQ":
        return [
            ("压测引擎", str(parameters.get("benchmark_engine", "rabbitmq-perf-test"))),
            (_engine_label(summary), str(parameters.get("workload", "mixed"))),
            ("Broker", str(parameters.get("rabbitmq_broker") or summary.target.get("host", "-"))),
            ("VHost", str(parameters.get("rabbitmq_vhost") or parameters.get("auth_database") or "/")),
            ("Queue", str(parameters.get("collection_name") or summary.target.get("database", "-"))),
            ("测试时长(秒)", str(parameters.get("duration_seconds", 0))),
            ("预热时长(秒)", str(parameters.get("warmup_seconds", 0))),
            ("消息总数", str(parameters.get("operation_count", 0))),
            ("消息大小(bytes)", str(parameters.get("table_size", 0))),
            ("报告间隔(秒)", str(parameters.get("report_interval", 0))),
            ("SSL 连接", "是" if parameters.get("ssl_enabled") else "否"),
            ("consumer 前置写入消息", "是" if parameters.get("auto_prepare") else "否"),
            ("Queue 自动删除", "是" if parameters.get("auto_cleanup") else "否"),
            ("并发客户端列表", ", ".join(str(item) for item in parameters.get("threads_values", []))),
        ]
    if summary.target.get("db_type") == "Redis":
        return [
            ("压测引擎", "redis-benchmark"),
            (_engine_label(summary), str(parameters.get("workload", "set"))),
            ("并发列表", ", ".join(str(item) for item in parameters.get("threads_values", []))),
            ("操作总数(-n)", str(parameters.get("operation_count", 0))),
            ("随机 key 空间(-r)", str(parameters.get("table_size", 0))),
            ("Value 大小(-d)", f"{parameters.get('redis_data_size', 128)} bytes"),
            ("测试前数据准备", "是" if parameters.get("auto_prepare") else "否"),
        ]
    if summary.target.get("db_type") == "Kafka":
        return [
            ("压测引擎", str(parameters.get("benchmark_engine", "kafka-perf-test"))),
            (_engine_label(summary), str(parameters.get("workload", "producer"))),
            ("Bootstrap Servers", str(parameters.get("kafka_bootstrap_servers") or summary.target.get("host", "-"))),
            ("Topic", str(parameters.get("collection_name") or summary.target.get("database", "-"))),
            ("并发客户端列表", ", ".join(str(item) for item in parameters.get("threads_values", []))),
            ("消息总数", str(parameters.get("operation_count", 0))),
            ("消息大小(bytes)", str(parameters.get("table_size", 0))),
            ("Topic 分区数", str(parameters.get("table_count", 0))),
            ("acks", str(parameters.get("kafka_acks", "1"))),
            ("吞吐限制(records/s)", str(parameters.get("kafka_throughput", -1))),
            ("测试前创建 Topic", "是" if parameters.get("auto_prepare") else "否"),
            ("测试后删除 Topic", "是" if parameters.get("auto_cleanup") else "否"),
        ]
    if summary.target.get("db_type") == "MongoDB":
        return [
            ("压测引擎", str(parameters.get("benchmark_engine", "ycsb"))),
            (_engine_label(summary), str(parameters.get("workload", "-"))),
            ("拓扑类型", str(parameters.get("mongo_topology", "-"))),
            ("读偏好", str(parameters.get("mongo_read_preference", "-"))),
            ("副本集名称", str(parameters.get("mongo_replica_set", "-"))),
            ("分片键字段", str(parameters.get("mongo_shard_key", "-"))),
            ("记录总数", str(parameters.get("table_size", 0))),
            ("操作总数", str(parameters.get("operation_count", 0))),
            ("预热时长(秒)", f"{parameters.get('warmup_seconds', 0)}（标准 YCSB 流程不单独执行）"),
            ("测试前自动载入数据", "是" if parameters.get("auto_prepare") else "否"),
            ("测试后自动清理集合", "是" if parameters.get("auto_cleanup") else "否"),
            ("线程列表", ", ".join(str(item) for item in parameters.get("threads_values", []))),
        ]
    if summary.target.get("db_type") == "ClickHouse":
        return [
            ("压测引擎", "QDBmark ClickHouse HTTP Benchmark"),
            (_engine_label(summary), str(parameters.get("workload", "-"))),
            ("测试时长(秒)", str(parameters.get("duration_seconds", 0))),
            ("预热时长(秒)", str(parameters.get("warmup_seconds", 0))),
            ("报告间隔(秒)", str(parameters.get("report_interval", 0))),
            ("测试表", str(parameters.get("collection_name", "-"))),
            ("表数量", str(parameters.get("table_count", 0))),
            ("单表造数行数", str(parameters.get("table_size", 0))),
            ("SSL 连接", "是" if parameters.get("ssl_enabled") else "否"),
            ("自动准备数据", "是" if parameters.get("auto_prepare") else "否"),
            ("测试后删除测试表", "是" if parameters.get("auto_cleanup") else "否"),
            ("线程列表", ", ".join(str(item) for item in parameters.get("threads_values", []))),
        ]
    if summary.target.get("db_type") == "GaussDB":
        return [
            ("压测引擎", "GaussDB BenchmarkSQL"),
            ("GaussDB 架构", "分布式" if str(parameters.get("gaussdb_architecture", "centralized")) == "distributed" else "集中式"),
            (_engine_label(summary), str(parameters.get("workload", "TPC-C"))),
            ("测试时长(秒)", str(parameters.get("duration_seconds", 0))),
            ("预热时长(秒)", str(parameters.get("warmup_seconds", 0))),
            ("造数并发(LOADWORKERS)", str(parameters.get("table_count", 0))),
            ("仓库数(WAREHOUSES)", str(parameters.get("table_size", 0))),
            ("CN 节点列表", str(parameters.get("gaussdb_cn_hosts") or summary.target.get("hosts") or "-")),
            ("JDBC 参数", str(parameters.get("gaussdb_conn_params", "-"))),
            ("测试前自动 destroy/build", "是" if parameters.get("auto_prepare") else "否"),
            ("线程列表", ", ".join(str(item) for item in parameters.get("threads_values", []))),
        ]
    if summary.target.get("db_type") in {"PostgreSQL", "OpenGauss", "Vastbase"}:
        if parameters.get("benchmark_engine") == "pgbench":
            return [
                ("压测引擎", "pgbench"),
                (_engine_label(summary), str(parameters.get("workload", "-"))),
                ("测试时长(秒)", str(parameters.get("duration_seconds", 0))),
                ("报告间隔(秒)", str(parameters.get("report_interval", 0))),
                ("预热时长(秒)", f"{parameters.get('warmup_seconds', 0)}（原生 pgbench 流程不单独执行）"),
                ("pgbench scale", str(parameters.get("pgbench_scale", parameters.get("table_size", 0)))),
                ("pgbench jobs", str(parameters.get("pgbench_jobs", 0))),
                ("fillfactor", str(parameters.get("pgbench_fillfactor", 0))),
                ("延迟门限(ms)", str(parameters.get("pgbench_latency_limit", 0))),
                ("测试前自动初始化", "是" if parameters.get("auto_prepare") else "否"),
                ("测试后自动清理", "是" if parameters.get("auto_cleanup") else "否"),
                ("线程列表", ", ".join(str(item) for item in parameters.get("threads_values", []))),
            ]
        return [
            ("压测引擎", "sysbench"),
            (_engine_label(summary), str(parameters.get("workload", "-"))),
            ("测试时长(秒)", str(parameters.get("duration_seconds", 0))),
            ("报告间隔(秒)", str(parameters.get("report_interval", 0))),
            ("预热时长(秒)", str(parameters.get("warmup_seconds", 0))),
            ("表数量", str(parameters.get("table_count", 0))),
            ("单表行数", str(parameters.get("table_size", 0))),
            ("自动准备测试数据", "是" if parameters.get("auto_prepare") else "否"),
            ("测试后自动清理", "是" if parameters.get("auto_cleanup") else "否"),
            ("线程列表", ", ".join(str(item) for item in parameters.get("threads_values", []))),
        ]
    return [
        ("压测引擎", str(parameters.get("benchmark_engine", "sysbench"))),
        (_engine_label(summary), str(parameters.get("workload", "-"))),
        ("测试时长(秒)", str(parameters.get("duration_seconds", 0))),
        ("报告间隔(秒)", str(parameters.get("report_interval", 0))),
        ("预热时长(秒)", str(parameters.get("warmup_seconds", 0))),
        ("表数量", str(parameters.get("table_count", 0))),
        ("单表行数", str(parameters.get("table_size", 0))),
        ("自动准备测试数据", "是" if parameters.get("auto_prepare") else "否"),
        ("测试后自动清理", "是" if parameters.get("auto_cleanup") else "否"),
        ("线程列表", ", ".join(str(item) for item in parameters.get("threads_values", []))),
    ]


def _target_value(summary: BenchmarkSummary, *keys: str) -> str:
    for key in keys:
        value = summary.target.get(key)
        if value is None or value == "":
            value = summary.parameters.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _append_target_item(items: list[tuple[str, str]], label: str, value: Any) -> None:
    text = str(value or "").strip()
    if text and text not in {"-", "None", "[]", "{}"}:
        items.append((label, text))


def _build_target_items(summary: BenchmarkSummary) -> list[tuple[str, str]]:
    db_type = str(summary.target.get("db_type", "未知类型"))
    label = "测试类型" if db_type == "FIO" else "数据库类型"
    items: list[tuple[str, str]] = [(label, db_type)]

    host = _target_value(summary, "host", "broker", "namesrv")
    port = _target_value(summary, "port")
    database = _target_value(summary, "database", "db_name")
    user = _target_value(summary, "user")
    schema = _target_value(summary, "schema", "postgres_schema")
    tenant = _target_value(summary, "tenant_name", "ob_tenant_name")
    current_primary = _target_value(summary, "current_primary", "primary", "instance_host")
    collection = _target_value(summary, "collection_name", "collection")
    target_hosts = _target_value(summary, "target_hosts", "hosts", "mongo_hosts", "kafka_bootstrap_servers", "rocketmq_namesrv", "rabbitmq_broker")
    fio_target = _target_value(summary, "fio_target_path", "target_path")

    if db_type == "FIO":
        _append_target_item(items, "测试文件", fio_target)
        _append_target_item(items, "执行节点", _target_value(summary, "execution_node", "ssh_host", "fio_ssh_host"))
        return items

    if host or port:
        _append_target_item(items, "目标实例", f"{host}:{port}" if port else host)
    _append_target_item(items, "集群/节点", target_hosts)
    _append_target_item(items, "当前主库/实例", current_primary)
    _append_target_item(items, "租户名", tenant)
    _append_target_item(items, "连接用户", user)
    _append_target_item(items, "数据库名", database)
    _append_target_item(items, "Schema", schema)
    _append_target_item(items, "集合/Topic/Queue", collection)

    parameters = summary.parameters or {}
    architecture = ""
    if db_type == "GaussDB":
        architecture = "分布式" if str(parameters.get("gaussdb_architecture", "centralized")) == "distributed" else "集中式"
    elif db_type == "MongoDB":
        topology = str(parameters.get("mongo_topology", "")).strip()
        architecture = {"replica_set": "副本集", "sharded": "分片集群", "standalone": "单节点"}.get(topology, topology)
    elif db_type == "TiDB":
        architecture = "TiDB 分布式集群"
    elif db_type == "OceanBase":
        architecture = "OceanBase 租户集群"
    elif db_type in {"Kafka", "RocketMQ", "RabbitMQ"}:
        architecture = "消息队列集群/节点"
    _append_target_item(items, "数据库架构", architecture)
    _append_target_item(items, "CN 节点", parameters.get("gaussdb_cn_hosts", ""))
    return items


def _write_summary_overview(document: Document, summaries: List[BenchmarkSummary]) -> None:
    _add_report_heading(document, "3.1 汇总概览", level=2)
    table = document.add_table(rows=1, cols=5)
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    _set_table_style_safe(table, "Medium Grid 1 Accent 1")
    headers = ["数据库类型", "执行时间", "线程配置", "推荐基线", "峰值吞吐"]
    for index, header in enumerate(headers):
        table.rows[0].cells[index].text = header

    for summary in summaries:
        peak = _summary_best(summary)
        row = table.add_row().cells
        row[0].text = str(summary.target["db_type"])
        row[1].text = summary.executed_at
        row[2].text = ", ".join(str(item) for item in summary.parameters.get("threads_values", []))
        row[3].text = (
            f"{summary.recommended_baseline.concurrency} 线程 / {_metric_summary_text(summary, summary.recommended_baseline)}"
            if summary.recommended_baseline
            else "无"
        )
        row[4].text = _metric_summary_text(summary, peak)


def _write_single_summary(
    document: Document,
    summary: BenchmarkSummary,
    index: int,
    runtime_history: Optional[List[Dict[str, Any]]] = None,
    chapter_number: int = 3,
) -> None:
    db_type = str(summary.target.get("db_type", "数据库"))
    section_prefix = f"{chapter_number}.{index}"
    section_heading = _add_report_heading(document, f"{section_prefix} {db_type}", level=2)
    if index > 1:
        section_heading.paragraph_format.page_break_before = True
    _add_report_heading(document, f"{section_prefix}.1 测试目标", level=3)
    _write_kv_table(
        document,
        _build_target_items(summary) + [("执行时间", summary.executed_at)],
    )

    _add_report_heading(document, f"{section_prefix}.2 测试参数", level=3)
    _write_kv_table(document, _build_parameter_items(summary))

    _write_results_table(document, summary, f"{section_prefix}.3 明细结果")
    _write_baseline_table(document, summary, summary.recommended_baseline, f"{section_prefix}.4 性能基线表")
    _write_runtime_charts(document, runtime_history or [], f"{section_prefix}.5 实时监控曲线", summary=summary)



def generate_report(
    summary: BenchmarkSummary,
    reports_dir: Path,
    runtime_history: Optional[List[Dict[str, Any]]] = None,
) -> Path:
    reports_dir.mkdir(parents=True, exist_ok=True)

    document = Document()
    _set_default_style(document)

    title = _add_report_heading(document, summary.report_title, level=0)
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    meta = document.add_paragraph()
    meta.alignment = WD_ALIGN_PARAGRAPH.CENTER
    meta.add_run(f"生成时间：{summary.executed_at}")

    _add_report_heading(document, "1. 测试目标", level=1)
    _write_kv_table(document, _build_target_items(summary))

    _add_report_heading(document, "2. 测试参数", level=1)
    _write_kv_table(document, _build_parameter_items(summary))

    document.add_section(WD_SECTION.CONTINUOUS)
    _add_report_heading(document, "3. 性能结果", level=1)
    _write_results_table(document, summary, "3.1 明细结果")
    _write_baseline_table(document, summary, summary.recommended_baseline, "3.2 性能基线表")
    _write_runtime_charts(document, runtime_history or [], "3.3 实时监控曲线")


    error_results = [result for result in summary.results if result.sample_errors]
    if error_results:
        _add_report_heading(document, "5. 失败样例", level=1)
        for result in error_results:
            _add_bullet_paragraph_safe(document, f"线程 {result.concurrency}")
            for message in result.sample_errors:
                document.add_paragraph(message)

    prefix = (summary.target.get("db_type") or "db").lower().replace(" ", "_")
    file_name = f"{prefix}_benchmark_{summary.executed_at.replace(':', '').replace(' ', '_').replace('-', '')}.docx"
    report_path = reports_dir / file_name
    document.save(report_path)
    return report_path


def generate_batch_report(
    summaries: List[BenchmarkSummary],
    reports_dir: Path,
    report_title: str,
    file_prefix: str,
    runtime_histories: Optional[List[List[Dict[str, Any]]]] = None,
) -> Path:
    if not summaries:
        raise ValueError("没有可导出的测试记录。")

    reports_dir.mkdir(parents=True, exist_ok=True)
    document = Document()
    _set_default_style(document)

    title = _add_report_heading(document, report_title, level=0)
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    meta = document.add_paragraph()
    meta.alignment = WD_ALIGN_PARAGRAPH.CENTER
    meta.add_run(f"生成时间：{_beijing_now().strftime('%Y-%m-%d %H:%M:%S')}")

    _write_summary_overview(document, summaries)
    for index, summary in enumerate(summaries, start=2):
        document.add_section(WD_SECTION.CONTINUOUS)
        history = runtime_histories[index - 2] if runtime_histories and len(runtime_histories) >= index - 1 else []
        _write_single_summary(document, summary, index, runtime_history=history)

    docx_path = reports_dir / f"{_safe_report_filename_stem(customer_name)}.docx"
    pdf_path = docx_path.with_suffix(".pdf")
    docx_path.unlink(missing_ok=True)
    pdf_path.unlink(missing_ok=True)
    _update_docx_fields_flag(document)
    document.save(docx_path)
    return _convert_docx_to_pdf(docx_path)

# PDF report generation overrides. The original Word generator above is kept as a fallback reference,
# while the public generate_report/generate_batch_report names below now emit PDF files.
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.pdfbase import pdfmetrics
from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

try:
    pdfmetrics.registerFont(UnicodeCIDFont("STSong-Light"))
except Exception:  # pragma: no cover
    pass


def _pdf_styles() -> Dict[str, ParagraphStyle]:
    styles = getSampleStyleSheet()
    base_font = "STSong-Light"
    return {
        "title": ParagraphStyle("QTitle", parent=styles["Title"], fontName=base_font, fontSize=24, leading=32, alignment=TA_CENTER, spaceAfter=18),
        "subtitle": ParagraphStyle("QSubtitle", parent=styles["Normal"], fontName=base_font, fontSize=14, leading=22, alignment=TA_CENTER, spaceAfter=12),
        "h1": ParagraphStyle("QH1", parent=styles["Heading1"], fontName=base_font, fontSize=16, leading=24, spaceBefore=12, spaceAfter=8),
        "h2": ParagraphStyle("QH2", parent=styles["Heading2"], fontName=base_font, fontSize=13, leading=20, spaceBefore=8, spaceAfter=6),
        "body": ParagraphStyle("QBody", parent=styles["BodyText"], fontName=base_font, fontSize=9.5, leading=15, alignment=TA_LEFT),
        "small": ParagraphStyle("QSmall", parent=styles["BodyText"], fontName=base_font, fontSize=8.5, leading=13, alignment=TA_LEFT),
    }


def _pdf_para(value: Any, style: ParagraphStyle) -> Paragraph:
    text = str(value if value is not None else "-")
    escaped = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\n", "<br/>")
    return Paragraph(escaped, style)


def _pdf_table(rows: List[List[Any]], col_widths: Optional[List[float]] = None, header: bool = True) -> Table:
    styles = _pdf_styles()
    body = [[_pdf_para(cell, styles["small"]) for cell in row] for row in rows]
    table = Table(body, colWidths=col_widths, repeatRows=1 if header else 0)
    commands = [
        ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#b8c7e0")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]
    if header and rows:
        commands.extend([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#dbeafe")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#12315f")),
        ])
    table.setStyle(TableStyle(commands))
    return table


def _safe_report_meta(customer_name: str = "", tester_name: str = "") -> tuple[str, str]:
    return (customer_name.strip() or "客户名称", tester_name.strip() or "测试人员")


def _safe_report_filename_stem(customer_name: str = "") -> str:
    customer, _ = _safe_report_meta(customer_name, "")
    stem = f"{customer}数据库性能测试报告"
    stem = re.sub(r'[\\/:*?"<>|\r\n\t]+', "_", stem).strip(" ._")
    return stem or "数据库性能测试报告"


def _pdf_cover(story: List[Any], title: str, customer_name: str, tester_name: str) -> None:
    styles = _pdf_styles()
    customer_name, tester_name = _safe_report_meta(customer_name, tester_name)
    story.extend([
        Spacer(1, 40 * mm),
        _pdf_para(customer_name, styles["title"]),
        _pdf_para("数据库性能测试报告", styles["title"]),
        Spacer(1, 10 * mm),
        _pdf_para(f"报告名称：{title}", styles["subtitle"]),
        _pdf_para(f"测试人员：{tester_name}", styles["subtitle"]),
        _pdf_para(f"生成时间：{_beijing_now().strftime('%Y-%m-%d %H:%M:%S')}", styles["subtitle"]),
        Spacer(1, 48 * mm),
        _pdf_para("杭州沃趣科技股份有限公司 制作", styles["subtitle"]),
        PageBreak(),
    ])


def _summary_best(summary: BenchmarkSummary) -> Optional[ConcurrencyResult]:
    return max(summary.results, key=lambda item: _metric_sort_key(summary, item), default=None)


def _pdf_overview(story: List[Any], summaries: List[BenchmarkSummary]) -> None:
    styles = _pdf_styles()
    story.append(_pdf_para("1. 数据库性能测试概览", styles["h1"]))
    rows: List[List[Any]] = [["数据库类型", "目标", "执行时间", "测试模型", "线程配置", "推荐基线", "峰值吞吐"]]
    for summary in summaries:
        peak = _summary_best(summary)
        recommended = summary.recommended_baseline
        target = summary.target
        db_type = str(target.get("db_type", "-"))
        target_display = str(target.get("target_path") or f"{target.get('host', '-') }:{target.get('port', '-')}/{target.get('database', '-')}")
        rows.append([
            db_type,
            target_display,
            summary.executed_at,
            str(summary.parameters.get("workload", "-")),
            ", ".join(str(item) for item in summary.parameters.get("threads_values", [])),
            f"{recommended.concurrency} 并发 / {_metric_summary_text(summary, recommended)}" if recommended else "-",
            _metric_summary_text(summary, peak),
        ])
    story.append(_pdf_table(rows, col_widths=[24*mm, 44*mm, 34*mm, 30*mm, 28*mm, 34*mm, 34*mm]))
    story.append(Spacer(1, 8 * mm))


def _pdf_results_table(summary: BenchmarkSummary) -> Table:
    count_columns = _count_columns(summary)
    metric_columns = _metric_columns(summary)
    rows: List[List[Any]] = [["线程数", *[label for label, _ in count_columns], *[label for label, _ in metric_columns], "平均延迟(ms)", "P95(ms)", "成功率", "评级"]]
    for result in summary.results:
        rows.append([
            result.concurrency,
            *[getattr(result, attr, 0) for _, attr in count_columns],
            *[_metric_display(result, attr) for _, attr in metric_columns],
            result.avg_latency_ms,
            result.p95_latency_ms,
            f"{result.success_rate}%",
            result.baseline_label,
        ])
    width_count = len(rows[0])
    return _pdf_table(rows, col_widths=[max(22*mm, 190*mm / width_count)] * width_count)


def _pdf_single_summary(story: List[Any], summary: BenchmarkSummary, index: int, runtime_history: Optional[List[Dict[str, Any]]] = None) -> None:
    styles = _pdf_styles()
    story.append(_pdf_para(f"{index}. {summary.target.get('db_type', '-') } 测试详情", styles["h1"]))
    story.append(_pdf_para(f"{index}.1 测试目标", styles["h2"]))
    target_items = _build_target_items(summary) + [("执行时间", summary.executed_at)]
    story.append(_pdf_table([["项目", "内容"], *target_items], col_widths=[42*mm, 150*mm]))
    story.append(_pdf_para(f"{index}.2 测试参数", styles["h2"]))
    story.append(_pdf_table([["参数", "值"], *_build_parameter_items(summary)], col_widths=[42*mm, 150*mm]))
    story.append(_pdf_para(f"{index}.3 性能结果", styles["h2"]))
    story.append(_pdf_results_table(summary))
    story.append(Spacer(1, 8 * mm))


def _pdf_doc_template(report_path: Path) -> SimpleDocTemplate:
    return SimpleDocTemplate(
        str(report_path),
        pagesize=landscape(A4),
        rightMargin=12 * mm,
        leftMargin=12 * mm,
        topMargin=12 * mm,
        bottomMargin=12 * mm,
        title=report_path.stem,
    )


def generate_report(
    summary: BenchmarkSummary,
    reports_dir: Path,
    runtime_history: Optional[List[Dict[str, Any]]] = None,
    customer_name: str = "",
    tester_name: str = "",
) -> Path:
    reports_dir.mkdir(parents=True, exist_ok=True)
    timestamp = summary.executed_at.replace(":", "").replace(" ", "_").replace("-", "")
    prefix = (summary.target.get("db_type") or "db").lower().replace(" ", "_")
    report_path = reports_dir / f"{prefix}_benchmark_{timestamp}.pdf"
    story: List[Any] = []
    _pdf_cover(story, summary.report_title, customer_name, tester_name)
    _pdf_overview(story, [summary])
    _pdf_single_summary(story, summary, 2, runtime_history=runtime_history)
    _pdf_doc_template(report_path).build(story)
    return report_path


def generate_batch_report(
    summaries: List[BenchmarkSummary],
    reports_dir: Path,
    report_title: str,
    file_prefix: str,
    runtime_histories: Optional[List[List[Dict[str, Any]]]] = None,
    customer_name: str = "",
    tester_name: str = "",
) -> Path:
    if not summaries:
        raise ValueError("没有可导出的测试记录。")
    reports_dir.mkdir(parents=True, exist_ok=True)
    timestamp = _beijing_now().strftime("%Y%m%d_%H%M%S")
    report_path = reports_dir / f"{file_prefix}_{timestamp}.pdf"
    story: List[Any] = []
    _pdf_cover(story, report_title, customer_name, tester_name)
    _pdf_overview(story, summaries)
    for index, summary in enumerate(summaries, start=2):
        history = runtime_histories[index - 2] if runtime_histories and len(runtime_histories) >= index - 1 else []
        _pdf_single_summary(story, summary, index, runtime_history=history)
    _pdf_doc_template(report_path).build(story)
    return report_path

# Template-based report generation override. This intentionally supersedes the PDF generator above
# because the customer report must preserve the official Woqutech template, logo, declaration,
# document properties, and distribution pages.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
FRONTEND_DIR = PROJECT_ROOT / "frontend"
REPORT_TEMPLATE_PATH = FRONTEND_DIR / "report_template.docx"
REPORT_LOGO_PATH = FRONTEND_DIR / "static" / "qfusion-mark.png"
REPORT_WOQU_LOGO_PATH = FRONTEND_DIR / "static" / "woqu-logo.png"


def _replace_text_in_paragraph(paragraph: Any, replacements: Dict[str, str]) -> None:
    if not paragraph.runs:
        return
    original = "".join(run.text for run in paragraph.runs)
    replaced = original
    for old, new in replacements.items():
        replaced = replaced.replace(old, new)
    if replaced == original:
        return
    paragraph.runs[0].text = replaced
    for run in paragraph.runs[1:]:
        run.text = ""


def _replace_text_in_document(document: Document, replacements: Dict[str, str]) -> None:
    for paragraph in document.paragraphs:
        _replace_text_in_paragraph(paragraph, replacements)
    for table in document.tables:
        for row in table.rows:
            for cell in row.cells:
                for paragraph in cell.paragraphs:
                    _replace_text_in_paragraph(paragraph, replacements)
    for section in document.sections:
        for paragraph in section.header.paragraphs:
            _replace_text_in_paragraph(paragraph, replacements)
        for table in section.header.tables:
            for row in table.rows:
                for cell in row.cells:
                    for paragraph in cell.paragraphs:
                        _replace_text_in_paragraph(paragraph, replacements)
        for paragraph in section.footer.paragraphs:
            _replace_text_in_paragraph(paragraph, replacements)


def _clear_paragraph(paragraph: Any) -> None:
    if paragraph.runs:
        paragraph.runs[0].text = ""
        for run in paragraph.runs[1:]:
            run.text = ""
    else:
        paragraph.add_run("")


def _is_heading_level(paragraph: Any, level: int) -> bool:
    outline = paragraph._p.pPr.find(qn("w:outlineLvl")) if paragraph._p.pPr is not None else None
    if outline is not None:
        try:
            return int(outline.get(qn("w:val"))) == level - 1
        except Exception:
            return False
    style_name = getattr(paragraph.style, "name", "")
    return style_name in {f"Heading {level}", f"标题 {level}"}


def _mark_template_toc(document: Document) -> None:
    replaced = False
    for paragraph in document.paragraphs:
        text = paragraph.text.strip()
        if "错误！未找到目录项" in text or "目录将在打开" in text:
            _clear_paragraph(paragraph)
            paragraph.runs[0].text = "__QDBMARK_TOC_LEVEL_1__"
            replaced = True
            break
    if not replaced:
        document.add_paragraph("目录")
        marker = document.add_paragraph()
        marker.add_run("__QDBMARK_TOC_LEVEL_1__")


def _set_paragraph_bottom_border(paragraph: Any, color: str = "f05a1a", size: str = "8") -> None:
    p_pr = paragraph._p.get_or_add_pPr()
    p_bdr = p_pr.find(qn("w:pBdr"))
    if p_bdr is None:
        p_bdr = OxmlElement("w:pBdr")
        p_pr.append(p_bdr)
    bottom = p_bdr.find(qn("w:bottom"))
    if bottom is None:
        bottom = OxmlElement("w:bottom")
        p_bdr.append(bottom)
    bottom.set(qn("w:val"), "single")
    bottom.set(qn("w:sz"), size)
    bottom.set(qn("w:space"), "2")
    bottom.set(qn("w:color"), color)


def _set_table_borders_none(table: Any) -> None:
    tbl_pr = table._tbl.tblPr
    borders = tbl_pr.find(qn("w:tblBorders"))
    if borders is None:
        borders = OxmlElement("w:tblBorders")
        tbl_pr.append(borders)
    for edge in ("top", "left", "bottom", "right", "insideH", "insideV"):
        node = borders.find(qn(f"w:{edge}"))
        if node is None:
            node = OxmlElement(f"w:{edge}")
            borders.append(node)
        node.set(qn("w:val"), "nil")


def _set_cell_shading(cell: Any, fill: str) -> None:
    tc_pr = cell._tc.get_or_add_tcPr()
    shading = tc_pr.find(qn("w:shd"))
    if shading is None:
        shading = OxmlElement("w:shd")
        tc_pr.append(shading)
    shading.set(qn("w:fill"), fill)


def _style_cell_text(cell: Any, size: float = 9, bold: bool = False, color: str = "000000") -> None:
    for paragraph in cell.paragraphs:
        paragraph.paragraph_format.space_before = Pt(0)
        paragraph.paragraph_format.space_after = Pt(0)
        paragraph.paragraph_format.line_spacing_rule = WD_LINE_SPACING.SINGLE
        for run in paragraph.runs:
            _style_text_run(run, size, bold=bold, color=color)


def _set_cell_margins(cell: Any, top: int = 80, start: int = 90, bottom: int = 80, end: int = 90) -> None:
    tc_pr = cell._tc.get_or_add_tcPr()
    tc_mar = tc_pr.find(qn("w:tcMar"))
    if tc_mar is None:
        tc_mar = OxmlElement("w:tcMar")
        tc_pr.append(tc_mar)
    for edge, value in (("top", top), ("start", start), ("bottom", bottom), ("end", end)):
        node = tc_mar.find(qn(f"w:{edge}"))
        if node is None:
            node = OxmlElement(f"w:{edge}")
            tc_mar.append(node)
        node.set(qn("w:w"), str(value))
        node.set(qn("w:type"), "dxa")


def _style_compact_table(table: Any, header_fill: str = "EAF4FF") -> None:
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    _set_table_style_safe(table, "Table Grid")
    for row_index, row in enumerate(table.rows):
        for cell in row.cells:
            _set_cell_margins(cell)
            if row_index == 0:
                _set_cell_shading(cell, header_fill)
                _style_cell_text(cell, size=8.5, bold=True, color="1f4e79")
            else:
                _style_cell_text(cell, size=8.5)


def _strip_template_body_after_cover(document: Document) -> None:
    body = document._body._element
    children = list(body)
    keep_until = None
    for index, child in enumerate(children):
        if child.tag == qn("w:p") and child.find(".//" + qn("w:sectPr")) is not None:
            keep_until = index
            break
    if keep_until is None:
        return

    # The Word template keeps an empty paragraph that only contains sectPr after
    # the cover. LibreOffice may render that paragraph as a standalone blank
    # page with just header/footer, so move the section properties onto the last
    # visible cover paragraph and remove the empty paragraph with the old body.
    def has_visible_payload(element: Any) -> bool:
        text = "".join(node.text or "" for node in element.findall(".//" + qn("w:t"))).strip()
        # Template cover metadata is redrawn into the real header/footer below;
        # do not keep it as body content because it can push an empty page.
        if text and not any(marker in text for marker in ("发布", "制作")):
            return True
        xml = element.xml
        return any(token in xml for token in ("<w:drawing", "<w:pict", "<w:object")) and "制作" not in text

    remove_from = keep_until + 1
    sect_paragraph = children[keep_until]
    sect_pr = sect_paragraph.find(".//" + qn("w:sectPr"))
    last_visible_index = None
    for index in range(keep_until - 1, -1, -1):
        child = children[index]
        if child.tag == qn("w:p") and has_visible_payload(child):
            last_visible_index = index
            break

    if sect_pr is not None and last_visible_index is not None:
        sect_parent = sect_pr.getparent()
        if sect_parent is not None:
            sect_parent.remove(sect_pr)
        target_paragraph = children[last_visible_index]
        target_p_pr = target_paragraph.find(qn("w:pPr"))
        if target_p_pr is None:
            target_p_pr = OxmlElement("w:pPr")
            target_paragraph.insert(0, target_p_pr)
        existing = target_p_pr.find(qn("w:sectPr"))
        if existing is not None:
            target_p_pr.remove(existing)
        target_p_pr.append(sect_pr)
        remove_from = last_visible_index + 1

    for child in children[remove_from:]:
        body.remove(child)


def _add_simple_field(paragraph: Any, instruction: str, fallback: str) -> None:
    run = paragraph.add_run()
    begin = OxmlElement("w:fldChar")
    begin.set(qn("w:fldCharType"), "begin")
    run._r.append(begin)
    instr = OxmlElement("w:instrText")
    instr.set(qn("xml:space"), "preserve")
    instr.text = f" {instruction} "
    run._r.append(instr)
    separate = OxmlElement("w:fldChar")
    separate.set(qn("w:fldCharType"), "separate")
    run._r.append(separate)
    text = OxmlElement("w:t")
    text.text = fallback
    run._r.append(text)
    end = OxmlElement("w:fldChar")
    end.set(qn("w:fldCharType"), "end")
    run._r.append(end)
    _style_text_run(run, 8.5, color="666666")


def _style_text_run(run: Any, size: float, bold: bool = False, color: str = "000000") -> None:
    run.bold = bold
    run.font.name = "Arial"
    run.font.size = Pt(size)
    try:
        run.font.color.rgb = RGBColor.from_string(color)
        run._element.rPr.rFonts.set(qn("w:eastAsia"), "SimSun")
    except Exception:
        pass


def _insert_toc_paragraph_after(paragraph: Any) -> Any:
    new_p = OxmlElement("w:p")
    paragraph._p.addnext(new_p)
    return DocxParagraph(new_p, paragraph._parent)


def _add_bookmark_to_paragraph(paragraph: Any, name: str, bookmark_id: int) -> None:
    start = OxmlElement("w:bookmarkStart")
    start.set(qn("w:id"), str(bookmark_id))
    start.set(qn("w:name"), name)
    end = OxmlElement("w:bookmarkEnd")
    end.set(qn("w:id"), str(bookmark_id))
    insert_at = 1 if paragraph._p.pPr is not None else 0
    paragraph._p.insert(insert_at, start)
    paragraph._p.append(end)


def _add_toc_entry_content(paragraph: Any, text: str, level: int, bookmark_name: str, fallback_page: str) -> None:
    _clear_paragraph(paragraph)
    paragraph.alignment = WD_ALIGN_PARAGRAPH.LEFT
    paragraph.paragraph_format.left_indent = Inches(0.16 if level == 1 else 0.42)
    paragraph.paragraph_format.first_line_indent = Inches(0)
    paragraph.paragraph_format.space_before = Pt(1)
    paragraph.paragraph_format.space_after = Pt(5 if level == 1 else 3)
    try:
        paragraph.paragraph_format.tab_stops.clear_all()
        paragraph.paragraph_format.tab_stops.add_tab_stop(Inches(6.45), WD_TAB_ALIGNMENT.RIGHT, WD_TAB_LEADER.DOTS)
    except Exception:
        pass
    run = paragraph.runs[0] if paragraph.runs else paragraph.add_run()
    run.text = text
    _style_text_run(run, 10.5 if level == 1 else 10, bold=False, color="111827")
    paragraph.add_run("	")
    _add_simple_field(paragraph, f"PAGEREF {bookmark_name} \h", fallback_page)


def _populate_level_1_toc(document: Document) -> None:
    items: List[tuple[Any, str, int, str, str]] = []
    fallback_page = 3
    for paragraph in document.paragraphs:
        text = paragraph.text.strip()
        if not text or text == "目录" or "__QDBMARK_TOC_LEVEL_1__" in text:
            continue
        level = 0
        if _is_heading_level(paragraph, 1):
            level = 1
        elif _is_heading_level(paragraph, 2):
            level = 2
        if not level:
            continue
        bookmark_name = f"qdbmark_toc_{len(items) + 1}"
        _add_bookmark_to_paragraph(paragraph, bookmark_name, len(items) + 1)
        if level == 1 and items:
            fallback_page += 1
        items.append((paragraph, text, level, bookmark_name, str(fallback_page)))
    for paragraph in document.paragraphs:
        if "__QDBMARK_TOC_LEVEL_1__" not in paragraph.text:
            continue
        if not items:
            _clear_paragraph(paragraph)
            paragraph.runs[0].text = "暂无目录项"
            return
        current = paragraph
        for item_index, (_, text, level, bookmark_name, page_text) in enumerate(items):
            if item_index:
                current = _insert_toc_paragraph_after(current)
            _add_toc_entry_content(current, text, level, bookmark_name, page_text)
        return

def _compact_template_cover(document: Document) -> None:
    if document.sections:
        document.sections[0].top_margin = Inches(0.45)
        document.sections[0].bottom_margin = Inches(0.45)
        document.sections[0].left_margin = Inches(0.65)
        document.sections[0].right_margin = Inches(0.65)
    for paragraph in document.paragraphs[:15]:
        paragraph.paragraph_format.space_before = Pt(0)
        paragraph.paragraph_format.space_after = Pt(0)
        paragraph.paragraph_format.line_spacing_rule = WD_LINE_SPACING.SINGLE
        paragraph.paragraph_format.line_spacing = 1


def _force_header_report_title(document: Document, title: str) -> None:
    replacements = {
        "客户名称数据库性能测试报告": title,
        "山东港口QFusion性能测试报告": title,
    }
    for section in document.sections:
        for part in (section.header, section.first_page_header, section.footer, section.first_page_footer):
            for paragraph in part.paragraphs:
                _replace_text_in_paragraph(paragraph, replacements)
            for table in part.tables:
                for row in table.rows:
                    for cell in row.cells:
                        for paragraph in cell.paragraphs:
                            _replace_text_in_paragraph(paragraph, replacements)


def _clear_part_paragraphs(part: Any) -> None:
    for paragraph in part.paragraphs:
        _clear_paragraph(paragraph)
        paragraph.paragraph_format.space_before = Pt(0)
        paragraph.paragraph_format.space_after = Pt(0)


def _clear_cover_body_metadata(document: Document) -> None:
    for paragraph in document.paragraphs[:16]:
        text = paragraph.text.strip()
        if "发布" in text or "制作" in text:
            _clear_paragraph(paragraph)


def _style_cover_paragraph(paragraph: Any, font_size: float, bold: bool = False) -> None:
    paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER
    paragraph.paragraph_format.space_before = Pt(0)
    paragraph.paragraph_format.space_after = Pt(0)
    paragraph.paragraph_format.line_spacing_rule = WD_LINE_SPACING.SINGLE
    for run in paragraph.runs:
        _style_text_run(run, font_size, bold=bold)


def _polish_template_cover(document: Document, full_title: str, customer_name: str) -> None:
    if not document.sections:
        return
    section = document.sections[0]
    section.different_first_page_header_footer = True
    sect_pr = section._sectPr
    pg_num = sect_pr.find(qn("w:pgNumType"))
    if pg_num is None:
        pg_num = OxmlElement("w:pgNumType")
        sect_pr.append(pg_num)
    pg_num.set(qn("w:fmt"), "decimal")
    pg_num.set(qn("w:start"), "1")
    section.top_margin = Inches(0.72)
    section.bottom_margin = Inches(0.7)
    section.left_margin = Inches(0.65)
    section.right_margin = Inches(0.65)

    first_header = section.first_page_header
    _clear_part_paragraphs(first_header)
    header_paragraph = first_header.paragraphs[0] if first_header.paragraphs else first_header.add_paragraph()
    header_paragraph.alignment = WD_ALIGN_PARAGRAPH.LEFT
    header_paragraph.paragraph_format.space_after = Pt(4)
    header_paragraph.paragraph_format.tab_stops.add_tab_stop(Inches(6.4), WD_TAB_ALIGNMENT.RIGHT)
    header_paragraph.add_run().add_tab()
    right_run = header_paragraph.add_run(full_title)
    _style_text_run(right_run, 8.5, color="666666")
    _set_paragraph_bottom_border(header_paragraph)

    first_footer = section.first_page_footer
    _clear_part_paragraphs(first_footer)
    footer_table = first_footer.add_table(rows=1, cols=3, width=Inches(6.4))
    footer_table.alignment = WD_TABLE_ALIGNMENT.CENTER
    _set_table_borders_none(footer_table)
    footer_table.columns[0].width = Inches(2.6)
    footer_table.columns[1].width = Inches(1.2)
    footer_table.columns[2].width = Inches(2.6)
    left_para = footer_table.cell(0, 0).paragraphs[0]
    left_para.alignment = WD_ALIGN_PARAGRAPH.LEFT
    left_run = left_para.add_run("杭州沃趣科技股份有限公司")
    _style_text_run(left_run, 8.5, color="666666")
    page_para = footer_table.cell(0, 1).paragraphs[0]
    page_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
    _add_simple_field(page_para, "PAGE", "1")
    slash_run = page_para.add_run(" / ")
    _style_text_run(slash_run, 8.5, color="666666")
    _add_simple_field(page_para, "NUMPAGES", "1")
    slogan_para = footer_table.cell(0, 2).paragraphs[0]
    slogan_para.alignment = WD_ALIGN_PARAGRAPH.RIGHT
    slogan_run = slogan_para.add_run("Let Data Drive!")
    _style_text_run(slogan_run, 10, bold=True, color="f05a1a")

    _clear_cover_body_metadata(document)
    title_paragraphs = [p for p in document.paragraphs[:12] if p.text.strip() in {customer_name, "数据库性能测试报告"}]
    for index, paragraph in enumerate(title_paragraphs):
        _style_cover_paragraph(paragraph, 22 if index == 0 else 24, bold=False)
    if title_paragraphs:
        title_paragraphs[0].paragraph_format.space_before = Pt(58)
        title_paragraphs[0].paragraph_format.space_after = Pt(12)
        if len(title_paragraphs) > 1:
            title_paragraphs[1].paragraph_format.space_after = Pt(72)
    for paragraph in document.paragraphs[:14]:
        if '<w:drawing' in paragraph._p.xml and not paragraph.text.strip():
            paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER
            paragraph.paragraph_format.space_before = Pt(0)
            paragraph.paragraph_format.space_after = Pt(0)

def _write_report_header_footer(section: Any, full_title: str) -> None:
    section.top_margin = Inches(0.72)
    section.bottom_margin = Inches(0.7)
    section.left_margin = Inches(0.65)
    section.right_margin = Inches(0.65)
    header = section.header
    _clear_part_paragraphs(header)
    header_paragraph = header.paragraphs[0] if header.paragraphs else header.add_paragraph()
    header_paragraph.alignment = WD_ALIGN_PARAGRAPH.LEFT
    header_paragraph.paragraph_format.space_after = Pt(4)
    header_paragraph.paragraph_format.tab_stops.add_tab_stop(Inches(6.4), WD_TAB_ALIGNMENT.RIGHT)
    header_paragraph.add_run().add_tab()
    right_run = header_paragraph.add_run(full_title)
    _style_text_run(right_run, 8.5, color="666666")
    _set_paragraph_bottom_border(header_paragraph)

    footer = section.footer
    _clear_part_paragraphs(footer)
    footer_table = footer.add_table(rows=1, cols=3, width=Inches(6.4))
    footer_table.alignment = WD_TABLE_ALIGNMENT.CENTER
    _set_table_borders_none(footer_table)
    footer_table.columns[0].width = Inches(2.6)
    footer_table.columns[1].width = Inches(1.2)
    footer_table.columns[2].width = Inches(2.6)
    left_para = footer_table.cell(0, 0).paragraphs[0]
    left_para.alignment = WD_ALIGN_PARAGRAPH.LEFT
    left_run = left_para.add_run("杭州沃趣科技股份有限公司")
    _style_text_run(left_run, 8.5, color="666666")
    page_para = footer_table.cell(0, 1).paragraphs[0]
    page_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
    _add_simple_field(page_para, "PAGE", "1")
    slash_run = page_para.add_run(" / ")
    _style_text_run(slash_run, 8.5, color="666666")
    _add_simple_field(page_para, "NUMPAGES", "1")
    slogan_para = footer_table.cell(0, 2).paragraphs[0]
    slogan_para.alignment = WD_ALIGN_PARAGRAPH.RIGHT
    slogan_run = slogan_para.add_run("Let Data Drive!")
    _style_text_run(slogan_run, 10, bold=True, color="f05a1a")


def _add_cover_spacer(document: Document, points: int) -> None:
    paragraph = document.add_paragraph()
    paragraph.paragraph_format.space_before = Pt(points)
    paragraph.paragraph_format.space_after = Pt(0)
    paragraph.paragraph_format.line_spacing_rule = WD_LINE_SPACING.SINGLE


def _add_cover_meta_line(document: Document, label: str, value: str) -> None:
    paragraph = document.add_paragraph()
    paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER
    paragraph.paragraph_format.space_before = Pt(0)
    paragraph.paragraph_format.space_after = Pt(4)
    paragraph.paragraph_format.line_spacing_rule = WD_LINE_SPACING.SINGLE
    label_run = paragraph.add_run(label)
    _style_text_run(label_run, 11, bold=True)
    value_run = paragraph.add_run(f"  {value}  ")
    _style_text_run(value_run, 11, bold=False)
    value_run.underline = True


def _new_template_document(report_title: str, customer_name: str, tester_name: str) -> Document:
    customer_name, tester_name = _safe_report_meta(customer_name, tester_name)
    export_date = _beijing_now().strftime("%Y年%m月%d日")
    document = Document()
    _set_default_style(document)
    full_title = f"{customer_name}数据库性能测试报告"
    section = document.sections[0]
    _write_report_header_footer(section, full_title)

    title_one = document.paragraphs[0] if document.paragraphs else document.add_paragraph()
    title_one.alignment = WD_ALIGN_PARAGRAPH.CENTER
    title_one.paragraph_format.space_before = Pt(145)
    title_one.paragraph_format.space_after = Pt(14)
    title_one.paragraph_format.line_spacing_rule = WD_LINE_SPACING.SINGLE
    run = title_one.add_run(customer_name)
    _style_text_run(run, 22, bold=False)

    title_two = document.add_paragraph()
    title_two.alignment = WD_ALIGN_PARAGRAPH.CENTER
    title_two.paragraph_format.space_after = Pt(105)
    title_two.paragraph_format.line_spacing_rule = WD_LINE_SPACING.SINGLE
    run = title_two.add_run("数据库性能测试报告")
    _style_text_run(run, 24, bold=False)

    if REPORT_WOQU_LOGO_PATH.exists():
        document.add_picture(str(REPORT_WOQU_LOGO_PATH), width=Inches(2.9))
        logo_paragraph = document.paragraphs[-1]
        logo_paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER
        logo_paragraph.paragraph_format.space_after = Pt(0)
    else:
        logo_paragraph = document.add_paragraph()
        logo_paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run = logo_paragraph.add_run("WOQU TECH")
        _style_text_run(run, 22, bold=True, color="f05a1a")

    _add_cover_spacer(document, 118)
    _add_cover_meta_line(document, "测试人员：", tester_name)
    _add_cover_meta_line(document, "时    间：", export_date)

    document.add_page_break()
    toc_title = document.add_paragraph("目录")
    toc_title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    toc_title.paragraph_format.space_before = Pt(20)
    toc_title.paragraph_format.space_after = Pt(10)
    _style_text_run(toc_title.runs[0], 14, bold=True)
    marker = document.add_paragraph()
    marker.paragraph_format.left_indent = Inches(0.15)
    marker.add_run("__QDBMARK_TOC_LEVEL_1__")
    return document


def _system_info_items(system_info: Optional[Dict[str, Any]]) -> List[tuple[str, str]]:
    if not isinstance(system_info, dict):
        system_info = {}
    rows: List[tuple[str, str]] = []
    captured_at = str(system_info.get("captured_at", "")).strip() or "未采集"
    rows.append(("采集时间", captured_at))

    seen: set[str] = set()
    for group_name in ("cards", "details"):
        group = system_info.get(group_name)
        if not isinstance(group, list):
            continue
        for item in group:
            if not isinstance(item, dict):
                continue
            label = str(item.get("label", "")).strip()
            if not label or label in seen:
                continue
            value = str(item.get("value", "")).strip() or "-"
            rows.append((label, value))
            seen.add(label)
    if len(rows) == 1:
        rows.extend([
            ("主机名", "未采集"),
            ("CPU 核数", "未采集"),
            ("CPU 型号", "未采集"),
            ("服务器架构", "未采集"),
            ("内存容量", "未采集"),
            ("K8s Node 节点", "未采集"),
            ("K8s Server 版本", "未采集"),
        ])
    return rows


def _node_endpoint(node: Dict[str, Any]) -> str:
    host = str(node.get("host", "-")).strip() or "-"
    port = str(node.get("port", "")).strip()
    return f"{host}:{port}" if port else host


def _write_system_summary_table(document: Document, captured_at: str, nodes: List[Dict[str, Any]], failures: List[Dict[str, Any]]) -> None:
    table = document.add_table(rows=1, cols=4)
    headers = ["采集时间", "成功节点", "失败节点", "节点总数"]
    values = [captured_at, str(len(nodes)), str(len(failures)), str(len(nodes) + len(failures))]
    for index, header in enumerate(headers):
        table.rows[0].cells[index].text = header
    row = table.add_row().cells
    for index, value in enumerate(values):
        row[index].text = value
    _style_compact_table(table)


def _write_single_node_table(document: Document, index: int, node: Dict[str, Any]) -> None:
    title = document.add_paragraph()
    title.paragraph_format.space_before = Pt(6)
    title.paragraph_format.space_after = Pt(3)
    run = title.add_run(f"节点 {index}: {_node_endpoint(node)}")
    _style_text_run(run, 10.5, bold=True, color="1f4e79")
    table = document.add_table(rows=1, cols=4)
    headers = ["项目", "值", "项目", "值"]
    for col, header in enumerate(headers):
        table.rows[0].cells[col].text = header
    fields = [
        ("连接状态", "采集成功"),
        ("主机名", str(node.get("hostname", "-"))),
        ("CPU 核数", str(node.get("cpu_cores", "-"))),
        ("CPU 型号", str(node.get("cpu_model", "-"))),
        ("服务器架构", str(node.get("arch", "-"))),
        ("内存容量", str(node.get("memory", "-"))),
        ("内核版本", str(node.get("kernel", "-"))),
        ("K8s Node", str(node.get("k8s_nodes", "-"))),
        ("K8s Server", str(node.get("k8s_version", "-"))),
        ("K8s Context", str(node.get("k8s_context", "-"))),
    ]
    for offset in range(0, len(fields), 2):
        row = table.add_row().cells
        left = fields[offset]
        right = fields[offset + 1] if offset + 1 < len(fields) else ("", "")
        row[0].text, row[1].text = left
        row[2].text, row[3].text = right
    _style_compact_table(table)
    for row in table.rows[1:]:
        _set_cell_shading(row.cells[0], "F7FBFF")
        _set_cell_shading(row.cells[2], "F7FBFF")
        _style_cell_text(row.cells[0], size=8.5, bold=True, color="1f4e79")
        _style_cell_text(row.cells[2], size=8.5, bold=True, color="1f4e79")


def _write_system_info_section(document: Document, system_info: Optional[Dict[str, Any]]) -> None:
    _add_report_heading(document, "第 2 章 系统信息", level=1)
    if not isinstance(system_info, dict):
        system_info = {}
    captured_at = str(system_info.get("captured_at", "")).strip() or "未采集"
    raw_nodes = system_info.get("nodes")
    nodes = [node for node in raw_nodes if isinstance(node, dict)] if isinstance(raw_nodes, list) else []
    raw_failures = system_info.get("failures")
    failures = [failure for failure in raw_failures if isinstance(failure, dict)] if isinstance(raw_failures, list) else []
    if nodes or failures:
        _write_system_summary_table(document, captured_at, nodes, failures)
        for index, node in enumerate(nodes, start=1):
            _write_single_node_table(document, index, node)
        for failure in failures:
            paragraph = document.add_paragraph()
            run = paragraph.add_run(f"节点 {_node_endpoint(failure)} 采集失败: {failure.get('error', '-')}")
            _style_text_run(run, 9, color="C00000")
        return
    _write_kv_table(document, _system_info_items(system_info))


def _append_template_report_body(
    document: Document,
    summaries: List[BenchmarkSummary],
    runtime_histories: Optional[List[List[Dict[str, Any]]]] = None,
    system_info: Optional[Dict[str, Any]] = None,
) -> None:
    _trim_trailing_empty_paragraphs(document)
    document.add_section(WD_SECTION.NEW_PAGE)
    _add_report_heading(document, "第 1 章 测试概述", level=1)
    document.add_paragraph("本文档用于记录数据库基础性能测试过程、测试结果、监控曲线和建议基线。")
    _write_system_info_section(document, system_info)
    _add_report_heading(document, "第 3 章 数据库性能测试概览", level=1)
    _write_summary_overview(document, summaries)
    _add_report_heading(document, "第 4 章 数据库类型", level=1)
    for index, summary in enumerate(summaries, start=1):
        if index > 1:
            document.add_page_break()
        history = runtime_histories[index - 1] if runtime_histories and len(runtime_histories) >= index else []
        _write_single_summary(document, summary, index, runtime_history=history, chapter_number=4)


def _update_docx_fields_flag(document: Document) -> None:
    settings = document.settings.element
    update_fields = settings.find(qn("w:updateFields"))
    if update_fields is None:
        update_fields = OxmlElement("w:updateFields")
        settings.append(update_fields)
    update_fields.set(qn("w:val"), "true")


def _convert_docx_to_pdf(docx_path: Path) -> Path:
    soffice = shutil.which("soffice") or shutil.which("libreoffice")
    if not soffice:
        raise RuntimeError("未找到 LibreOffice/soffice，无法将 DOCX 转换为 PDF。")
    output_dir = docx_path.parent
    subprocess.run(
        [soffice, "--headless", "--convert-to", "pdf", "--outdir", str(output_dir), str(docx_path)],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=120,
    )
    pdf_path = docx_path.with_suffix(".pdf")
    if not pdf_path.exists():
        raise RuntimeError(f"LibreOffice 未生成 PDF: {pdf_path}")
    return pdf_path


def generate_report(
    summary: BenchmarkSummary,
    reports_dir: Path,
    runtime_history: Optional[List[Dict[str, Any]]] = None,
    customer_name: str = "",
    tester_name: str = "",
    system_info: Optional[Dict[str, Any]] = None,
) -> Path:
    reports_dir.mkdir(parents=True, exist_ok=True)
    document = _new_template_document(summary.report_title, customer_name, tester_name)
    _append_template_report_body(document, [summary], runtime_histories=[runtime_history or []], system_info=system_info)
    _populate_level_1_toc(document)
    docx_path = reports_dir / f"{_safe_report_filename_stem(customer_name)}.docx"
    pdf_path = docx_path.with_suffix(".pdf")
    docx_path.unlink(missing_ok=True)
    pdf_path.unlink(missing_ok=True)
    _update_docx_fields_flag(document)
    document.save(docx_path)
    return _convert_docx_to_pdf(docx_path)


def generate_batch_report(
    summaries: List[BenchmarkSummary],
    reports_dir: Path,
    report_title: str,
    file_prefix: str,
    runtime_histories: Optional[List[List[Dict[str, Any]]]] = None,
    customer_name: str = "",
    tester_name: str = "",
    system_info: Optional[Dict[str, Any]] = None,
) -> Path:
    if not summaries:
        raise ValueError("没有可导出的测试记录。")
    reports_dir.mkdir(parents=True, exist_ok=True)
    document = _new_template_document(report_title, customer_name, tester_name)
    _append_template_report_body(document, summaries, runtime_histories=runtime_histories, system_info=system_info)
    _populate_level_1_toc(document)
    docx_path = reports_dir / f"{_safe_report_filename_stem(customer_name)}.docx"
    pdf_path = docx_path.with_suffix(".pdf")
    docx_path.unlink(missing_ok=True)
    pdf_path.unlink(missing_ok=True)
    _update_docx_fields_flag(document)
    document.save(docx_path)
    return _convert_docx_to_pdf(docx_path)
