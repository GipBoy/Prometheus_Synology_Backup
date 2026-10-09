#!/usr/bin/env python3
"""
Synology Active Backup for Business Prometheus Exporter (multi-NAS)

数据源: SYNO.ActiveBackup.Overview / list_device_transfer_size
  - 时间窗 90 天（避开 synology_api 默认 24h 窗口过小的问题）
  - 每个 VM 取 transfer_list 中 time_end 最大的一条作为 "上一次备份"
  - 缺失字段默认 "Unknown"（首字母大写，与既有 9771 exporter 保持一致）

多 NAS 处理流程（严格串行）：
  NAS1:  login → collect → logout
  NAS2:  login → collect → logout
  ...
  全部完成后再启动 Prometheus HTTP Server、暴露指标
这样可避免库内部 session/cookie 错乱，也保证指标里不会出现 "半截" 数据。
"""

from prometheus_client import start_http_server, Gauge, Summary
import random
import time
import json
import sys
import os
import traceback
from pathlib import Path

# 恢复官方库：9771 上的现网 exporter 用它正常工作，证明库本身没有问题。
# 之前因为怀疑它多实例共享 session 才改成自写 DirectABClient，但自写客户端
# 与 ABB Inventory API 协议细节对不上（所有 NAS 都报 code=120），所以回退。
from synology_api.core_active_backup import ActiveBackupBusiness


# 抑制 urllib3 在 Cert_Verify=false 时的 InsecureRequestWarning 噪音
try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except Exception:
    pass


# ===== 自动定位 config.json =====
def resource_path(name):
    if getattr(sys, 'frozen', False):
        return Path(os.path.dirname(os.path.abspath(sys.executable))) / name
    return Path(__file__).resolve().parent / name


with open(resource_path('config.json')) as f:
    config = json.load(f)


def validate_config(cfg):
    """启动期校验 config，发现问题直接退出，避免运行时崩溃"""
    required_top = ['ExporterPort', 'NasList']
    for k in required_top:
        if k not in cfg:
            sys.exit(f"[FATAL] config.json missing top-level key: {k}")

    if not isinstance(cfg['NasList'], list) or not cfg['NasList']:
        sys.exit("[FATAL] NasList must be a non-empty list")

    required_nas = {
        'name', 'DSMAddress', 'DSMPort', 'Username', 'Password',
        'Secure', 'Cert_Verify', 'ActiveBackup',
    }
    seen_names = {}
    for i, nas in enumerate(cfg['NasList']):
        prefix = f"NasList[{i}]"
        if not isinstance(nas, dict):
            sys.exit(f"[FATAL] {prefix} is not an object")
        missing = required_nas - nas.keys()
        if missing:
            sys.exit(f"[FATAL] {prefix} missing required keys: {sorted(missing)}")
        n = nas['name']
        if not n or not isinstance(n, str):
            sys.exit(f"[FATAL] {prefix}.name is empty or not a string")
        if n in seen_names:
            sys.exit(f"[FATAL] duplicate nas name {n!r} at {prefix} "
                     f"(already used by {seen_names[n]})")
        seen_names[n] = prefix

    print(f"[INFO] Config validated: {len(cfg['NasList'])} NAS(es) configured")
    for nas in cfg['NasList']:
        print(f"       - {nas['name']} ({nas['DSMAddress']}:{nas['DSMPort']})")


validate_config(config)

EXPORTER_PORT = int(config['ExporterPort'])
NAS_LIST = config['NasList']

# list_device_transfer_size 时间窗：90 天 = 7776000 秒
TRANSFER_WINDOW_SECONDS = 90 * 24 * 3600


# -------- Metrics --------
LABELS = ['nas', 'vmname', 'hostname', 'vmuuid', 'vmos']

g_last_ts = Gauge(
    'synology_active_backup_lastbackup_timestamp',
    'Timestamp of last backup',
    LABELS
)
g_duration = Gauge(
    'synology_active_backup_lastbackup_duration',
    'Duration of last backup in Seconds',
    LABELS
)
g_bytes = Gauge(
    'synology_active_backup_lastbackup_transfered_bytes',
    'Transfered data of last backup in Bytes',
    LABELS
)
g_result = Gauge(
    'synology_active_backup_lastbackup_result',
    'Result of last backup - 2=Success, 4=Fail',
    LABELS
)
g_scrape_ok = Gauge(
    'synology_active_backup_scrape_success',
    '1 if exporter could query this NAS',
    ['nas']
)


def clear_nas_labels(nas_name):
    """清除属于此 nas 的所有 Gauge 时间序列，避免 VM 删除后数据残留"""
    for g in (g_last_ts, g_duration, g_bytes, g_result):
        keys_to_remove = [
            k for k in list(g._metrics.keys())
            if k and len(k) == len(LABELS) and k[0] == nas_name
        ]
        for k in keys_to_remove:
            try:
                g.remove(*k)
            except KeyError:
                pass


def safe_get(obj, *keys, default='Unknown'):
    """逐层取 dict / value，找不到或为空都返回 'Unknown'"""
    cur = obj
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
    if cur in (None, '', {}):
        return default
    return cur


def is_scheduled_task(task):
    """
    判断任务是否带有"启用的"备份计划：
    - status == 'unscheduled' → 无计划
    - sched_content 为空 → 无计划
    - sched_content.enable == 0 → 计划被停用
    """
    if task.get('status') == 'unscheduled':
        return False
    sched = task.get('sched_content')
    if sched is None or sched == '' or sched == {}:
        return False
    if isinstance(sched, str):
        try:
            sched = json.loads(sched)
        except (ValueError, TypeError):
            return False
    if isinstance(sched, dict):
        if sched.get('enable', 1) == 0:
            return False
    return True


def get_scheduled_device_uuids(sess, name):
    """
    调用 list_tasks，收集"属于有计划任务的设备" device_uuid 集合。

    返回值：
        set  → 正常拿到白名单（可能为空集，表示该 NAS 没有任何计划任务）
        None → 接口调用失败，无法判断（调用方自行决定降级策略）
    """
    try:
        resp = sess.list_tasks(load_status=True, load_result=False,
                               load_devices=True)
    except Exception as e:
        print(f"[{name}] WARN: list_tasks failed ({e}); "
              f"cannot build scheduled-VM whitelist")
        return None

    data = resp.get('data') if isinstance(resp, dict) else None

    tasks = []
    if isinstance(data, list):
        tasks = [t for t in data if isinstance(t, dict)]
    elif isinstance(data, dict):
        v = data.get('tasks')
        if isinstance(v, list):
            tasks = [t for t in v if isinstance(t, dict)]

    uuids = set()
    for task in tasks:
        if not is_scheduled_task(task):
            continue
        devices = task.get('devices')
        if not isinstance(devices, list) or not devices:
            devices = [task.get('device') or {}]
        for dev in devices:
            if isinstance(dev, dict):
                u = dev.get('device_uuid')
                if u:
                    uuids.add(u)

    print(f"[{name}] scheduled whitelist: {len(uuids)} device(s) "
          f"(from {len(tasks)} tasks)")
    return uuids


def collect_one(nas):
    """
    单台 NAS 处理流程：
        1. 登录建会话
        2. 拉取 hypervisor 清单 + transfer_size（90 天窗口）
        3. 导出指标
        4. 无论是否异常都 logout 释放会话
    """
    name = nas['name']

    if not nas.get('ActiveBackup', False):
        print(f"[{name}] ActiveBackup disabled, skipping")
        return 0

    clear_nas_labels(name)

    sess = None
    exported = 0
    t_start = time.time()
    try:
        # ---- 1) login ----
        sess = ActiveBackupBusiness(
            nas['DSMAddress'],
            int(nas['DSMPort']),
            nas['Username'],
            nas['Password'],
            nas['Secure'],
            nas['Cert_Verify'],
        )
        print(f"[{name}] logged in, collecting...")

        # ---- 2a) hypervisor 清单 ----
        # 单独 try：只备份物理机/PC 的 NAS 可能没有 VM hypervisor，
        # 该接口失败不应阻断 transfer_size 采集。
        hypervisor_list = {}
        try:
            abb_hv = sess.list_vm_hypervisor()
            if isinstance(abb_hv, dict) and 'data' in abb_hv:
                data = abb_hv['data']
                # data 可能是 list，也可能是 dict{'hypervisor_list': [...]} 等
                items = data if isinstance(data, list) else (
                    data.get('hypervisor_list')
                    or data.get('inventories')
                    or []
                )
                if not isinstance(items, list):
                    items = []
                hypervisor_list = {
                    (h.get('inventory_id') or ''): (h.get('host_name') or 'Unknown')
                    for h in items if isinstance(h, dict)
                }
        except Exception as hv_err:
            print(f"[{name}] WARN: list_vm_hypervisor failed ({hv_err}); "
                  f"hostname will fall back to 'Unknown'")

        # ---- 2b) 计划任务白名单（要求 1：只导出有计划备份任务的 VM）----
        scheduled_uuids = get_scheduled_device_uuids(sess, name)

        # ---- 2c) transfer_size（90 天窗口）----
        now = int(time.time())
        abb_vms = sess.list_device_transfer_size(
            time_start=now - TRANSFER_WINDOW_SECONDS,
            time_end=now,
        )

        if not isinstance(abb_vms, dict) or 'data' not in abb_vms:
            print(f"[{name}] WARN: unexpected response from list_device_transfer_size")
            g_scrape_ok.labels(name).set(0)
            return 0

        device_list = abb_vms['data'].get('device_list', []) or []
        if not isinstance(device_list, list):
            device_list = []

        # DEBUG: 直接打印每台 NAS 拉到的 device_list 前 2 条 + 总数，
        # 用于判断 "5 台 NAS 是否真的返回了不同数据"。
        first = []
        for vm in device_list[:2]:
            if isinstance(vm, dict):
                d = vm.get('device') or {}
                first.append({
                    'host': (d.get('host_name') if isinstance(d, dict) else None) or '?',
                    'inv':  (d.get('inventory_id') if isinstance(d, dict) else None) or '?',
                })
        print(f"[{name}] DEBUG device_list: total={len(device_list)}, "
              f"first2={first}")

        # ---- 3) 导出指标 ----
        skipped_unscheduled = 0
        for vm in device_list:
            if not isinstance(vm, dict):
                continue
            dev = vm.get('device') or {}
            if not isinstance(dev, dict):
                dev = {}

            # 要求 1：只导出"有计划备份任务"的 VM（白名单来自 list_tasks）。
            # 白名单获取失败(None)时不过滤，避免整台 NAS 数据消失。
            raw_uuid = dev.get('device_uuid') or ''
            if scheduled_uuids is not None and raw_uuid not in scheduled_uuids:
                skipped_unscheduled += 1
                continue

            transfers = vm.get('transfer_list') or []
            if not isinstance(transfers, list) or not transfers:
                continue

            # 取 transfer_list 中 time_end 最大的一条作为 "上一次备份"
            valid_transfers = [t for t in transfers if isinstance(t, dict)]
            if not valid_transfers:
                continue
            t = max(valid_transfers, key=lambda x: x.get('time_end', 0) or 0)
            t_end = t.get('time_end') or 0
            t_start_b = t.get('time_start') or 0
            if not t_end:
                continue

            inv_id = dev.get('inventory_id') or ''
            hypervisor = hypervisor_list.get(inv_id, 'Unknown')
            vmname = safe_get(dev, 'host_name')
            vmuuid = safe_get(dev, 'device_uuid')
            vmos = safe_get(dev, 'os_name')

            g_last_ts.labels(name, vmname, hypervisor, vmuuid, vmos).set(t_end)
            g_duration.labels(name, vmname, hypervisor, vmuuid, vmos).set(
                max(t_end - t_start_b, 0)
            )
            g_bytes.labels(name, vmname, hypervisor, vmuuid, vmos).set(
                t.get('transfered_bytes') or 0
            )
            g_result.labels(name, vmname, hypervisor, vmuuid, vmos).set(
                t.get('status', -1)
            )
            exported += 1

        g_scrape_ok.labels(name).set(1)
        elapsed = time.time() - t_start
        filter_note = (f", skipped {skipped_unscheduled} unscheduled"
                       if scheduled_uuids is not None else ", filter unavailable")
        print(f"[{name}] OK, exported {exported} VMs{filter_note} in {elapsed:.2f}s")
        return exported

    except Exception as e:
        print(f"[{name}] ERROR: {e}")
        traceback.print_exc()
        g_scrape_ok.labels(name).set(0)
        return 0
    finally:
        # ---- 4) logout ----
        if sess is not None:
            # synology_api 不同版本的 logout() 签名不一致：
            #   有的版本 logout(self)
            #   有的版本 logout(self, application)
            # 都 try 一遍，谁成算谁的
            for fn_call in (lambda: sess.logout(),
                            lambda: sess.logout('ActiveBackup')):
                try:
                    fn_call()
                    print(f"[{name}] logged out")
                    break
                except TypeError:
                    continue
                except Exception as logout_err:
                    print(f"[{name}] WARN: logout failed ({logout_err})")
                    break


def collect_all():
    """串行采集所有 NAS，最后汇总打印"""
    print("=" * 60)
    print("Phase 1: collecting from all NAS sequentially...")
    print("=" * 60)

    summary = []
    for nas in NAS_LIST:
        count = collect_one(nas)
        summary.append((nas['name'], count))

    print("-" * 60)
    print("Collection summary:")
    total = 0
    for name, count in summary:
        print(f"  {name:<24} {count:>5} VMs")
        total += count
    print(f"  {'TOTAL':<24} {total:>5} VMs")
    print("-" * 60)
    return summary


REQUEST_TIME = Summary('request_processing_seconds', 'Time spent processing request')


@REQUEST_TIME.time()
def process_request(t):
    time.sleep(t)


if __name__ == '__main__':
    print("Synology Backup Exporter (multi-NAS, sequential) starting...")
    print(f"Using config: {resource_path('config.json')}")

    # 先全部采完，再起 HTTP server。避免 Prometheus 拉到半截数据。
    collect_all()

    start_http_server(EXPORTER_PORT)
    print(f"Web Server running on Port {EXPORTER_PORT}")

    while True:
        process_request(random.random())
        time.sleep(30)
        collect_all()
