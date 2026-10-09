#!/usr/bin/env python3
"""
export_backup.py —— 访问 Synology Backup Exporter 的 /metrics 端点，
                    把 Prometheus 格式的指标解析成 CSV。

用法:
    python3 export_backup.py                      # 用默认配置
    python3 export_backup.py --once              # 只导一次就退出（适合 cron）
    python3 export_backup.py --interval 300      # 每 300 秒导出一次
    python3 export_backup.py --endpoint http://127.0.0.1:9791/metrics
    python3 export_backup.py --output /var/log/backup_report.csv

依赖: pip install prometheus_client
"""

import argparse
import csv
import os
import sys
import time
from datetime import datetime

try:
    from prometheus_client.parser import text_string_to_metric_families
except ImportError:
    from prometheus_client.client import text_string_to_metric_families

# ============ 默认配置 ============
DEFAULT_ENDPOINT = "http://127.0.0.1:9771"

if getattr(sys, 'frozen', False):
    DEFAULT_OUTPUT = os.path.join(
        os.path.dirname(os.path.abspath(sys.executable)),
        "synology_backup_report.csv"
    )
else:
    DEFAULT_OUTPUT = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "synology_backup_report.csv"
    )

# ============ init.py 实际暴露的 5 个 label ============
ACTUAL_LABELS = ["nas", "vmname", "hostname", "vmuuid", "vmos"]

# ============ 任务状态码 -> 可读名称 ============
# 来源: init.py 中的 last_result.status 映射
RESULT_LABELS = {
    2: "Success",
    3: "Partial Success",
    4: "Failed",
    5: "Cancelled",
    6: "No Backup",
}
RESULT_LABELS_STR = {str(k): v for k, v in RESULT_LABELS.items()}

# ============ 要导出的指标 ============
METRICS_TO_EXPORT = {
    "synology_active_backup_lastbackup_timestamp": {
        "column": "last_backup_time",
        "transform": lambda v: (
            datetime.fromtimestamp(v).strftime("%Y/%m/%d %H:%M:%S")
            if v and v > 0 else "Never"
        ),
    },
    "synology_active_backup_lastbackup_duration": {
        "column": "duration_seconds",
        "transform": lambda v: f"{v:.1f}" if v else "0",
    },
    "synology_active_backup_lastbackup_transfered_bytes": {
        "column": "transferred_bytes",
        "transform": lambda v: str(int(v)) if v else "0",
    },
    "synology_active_backup_lastbackup_result": {
        "column": "result_raw",
        "transform": lambda v: str(int(v)) if v is not None else "",
    },
    # scrape_success 只用 nas 一个标签，独立导出
    "synology_active_backup_scrape_success": {
        "column": "scrape_ok",
        "by_nas_only": True,
        "transform": lambda v: "OK" if v == 1 else "Fail",
    },
}


def fetch_metrics(endpoint):
    """抓取 /metrics 端点，返回原始文本"""
    try:
        import urllib.request
        req = urllib.request.Request(
            endpoint, headers={"User-Agent": "backup-csv-exporter"}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.read().decode("utf-8")
    except Exception as e:
        print(f"[ERROR] Failed to fetch {endpoint}: {e}")
        return None


def parse_metrics(text):
    """
    解析 Prometheus 文本格式
    返回: {
        metric_name: { (label_tuple_5): value, ... },   # 常规 5-label 指标
        metric_name: { (label_tuple_1,): value, ... },   # 仅 nas 标签指标
    }
    """
    result = {}
    families = list(text_string_to_metric_families(text))
    for family in families:
        if family.name not in METRICS_TO_EXPORT:
            continue
        bucket = result.setdefault(family.name, {})
        cfg = METRICS_TO_EXPORT[family.name]
        labels_to_pick = ["nas"] if cfg.get("by_nas_only") else ACTUAL_LABELS
        for sample in family.samples:
            if sample.name != family.name:
                continue
            key = tuple(sample.labels.get(ln, "") for ln in labels_to_pick)
            bucket[key] = sample.value
    return result


def build_rows(parsed):
    """以 timestamp 指标为主表，按 label 组合并成一行"""
    ts_name = "synology_active_backup_lastbackup_timestamp"
    if ts_name not in parsed or not parsed[ts_name]:
        return []

    rows = []
    ts_bucket = parsed[ts_name]
    scrape_bucket = parsed.get("synology_active_backup_scrape_success", {})

    for key in ts_bucket:
        # key = (nas, vmname, hostname, vmuuid, vmos)
        row = {
            "source_nas":  key[0],
            "vm_name":     key[1],
            "hypervisor":  key[2],
            "vm_uuid":     key[3],
            "os_type":     key[4],
        }

        # per-NAS 指标（scrape_success）
        nas_key = (key[0],)
        scrape_val = scrape_bucket.get(nas_key)
        try:
            row["scrape_ok"] = (
                METRICS_TO_EXPORT["synology_active_backup_scrape_success"]
                    ["transform"](scrape_val)
                if scrape_val is not None else "Unknown"
            )
        except Exception:
            row["scrape_ok"] = str(scrape_val) if scrape_val is not None else ""

        # 通用 5-label 指标
        for m_name, cfg in METRICS_TO_EXPORT.items():
            if cfg.get("by_nas_only"):
                continue
            bucket = parsed.get(m_name, {})
            raw = bucket.get(key)
            if raw is None:
                row[cfg["column"]] = ""
            else:
                try:
                    row[cfg["column"]] = cfg["transform"](raw)
                except Exception:
                    row[cfg["column"]] = str(raw)

        # result_raw -> 可读状态
        raw = row.get("result_raw", "")
        if raw == "":
            row["result"] = "Unknown"
        elif raw in RESULT_LABELS_STR:
            row["result"] = RESULT_LABELS_STR[raw]
        else:
            row["result"] = raw  # 未知状态码原样保留

        rows.append(row)

    return rows


def write_csv(rows, output_path):
    if not rows:
        print("[WARN] No data to write, skip.")
        return

    fieldnames = [
        "source_nas",
        "vm_name",
        "hypervisor",
        "vm_uuid",
        "os_type",
        "last_backup_time",
        "duration_seconds",
        "transferred_bytes",
        "result_raw",
        "result",
        "scrape_ok",
    ]

    rows.sort(key=lambda r: (r.get("source_nas", ""), r.get("vm_name", "")))

    tmp_path = output_path + ".tmp"
    with open(tmp_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    os.replace(tmp_path, output_path)
    print(f"[INFO] Wrote {len(rows)} rows -> {output_path}")


def do_export(endpoint, output):
    text = fetch_metrics(endpoint)
    if text is None:
        return False

    parsed = parse_metrics(text)
    rows = build_rows(parsed)
    write_csv(rows, output)
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Export Synology Backup Exporter metrics to CSV"
    )
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT,
                        help=f"Metrics endpoint (default: {DEFAULT_ENDPOINT})")
    parser.add_argument("--output", default=DEFAULT_OUTPUT,
                        help=f"Output CSV path (default: {DEFAULT_OUTPUT})")
    parser.add_argument("--interval", type=int, default=0,
                        help="Repeat every N seconds (0 = run once and exit)")
    parser.add_argument("--once", action="store_true",
                        help="Run once and exit (same as --interval 0)")
    args = parser.parse_args()

    print(f"[INFO] Endpoint : {args.endpoint}")
    print(f"[INFO] Output   : {args.output}")

    if args.once or args.interval <= 0:
        do_export(args.endpoint, args.output)
    else:
        print(f"[INFO] Interval : {args.interval}s (Ctrl+C to stop)")
        try:
            while True:
                do_export(args.endpoint, args.output)
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\n[INFO] Stopped by user.")


if __name__ == "__main__":
    main()