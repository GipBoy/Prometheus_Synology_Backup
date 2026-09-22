#!/usr/bin/env python3

from prometheus_client import start_http_server, Gauge, Summary
import random
import time
import json
import sys
import os
from pathlib import Path

from synology_api.core_active_backup import ActiveBackupBusiness


# ===== 自动定位 config.json（开发模式 / 打包后都能用）=====
def resource_path(name):
    if getattr(sys, 'frozen', False):
        # PyInstaller 打包后：和二进制同目录
        return Path(os.path.dirname(os.path.abspath(sys.executable))) / name
    # 开发模式：和 init.py 同目录
    return Path(__file__).resolve().parent / name


with open(resource_path('config.json')) as f:
    config = json.load(f)

EXPORTER_PORT = int(config['ExporterPort'])
NAS_LIST = config['NasList']


# -------- Metrics（全部加 nas label）--------
g_last_ts = Gauge(
    'synology_active_backup_lastbackup_timestamp',
    'Timestamp of last backup',
    ['nas', 'vmname', 'hostname', 'vmuuid', 'vmos']
)
g_duration = Gauge(
    'synology_active_backup_lastbackup_duration',
    'Duration of last backup in Seconds',
    ['nas', 'vmname', 'hostname', 'vmuuid', 'vmos']
)
g_bytes = Gauge(
    'synology_active_backup_lastbackup_transfered_bytes',
    'Transfered data of last backup in Bytes',
    ['nas', 'vmname', 'hostname', 'vmuuid', 'vmos']
)
g_result = Gauge(
    'synology_active_backup_lastbackup_result',
    'Result of last backup - 2 = Good, 4 = Bad',
    ['nas', 'vmname', 'hostname', 'vmuuid', 'vmos']
)
g_scrape_ok = Gauge(
    'synology_active_backup_scrape_success',
    '1 if exporter could query this NAS',
    ['nas']
)


def collect_one(nas):
    name = nas['name']

    if not nas.get('ActiveBackup', False):
        print(f"[{name}] ActiveBackup disabled, skipping")
        return

    try:
        sess = ActiveBackupBusiness(
            nas['DSMAddress'],
            int(nas['DSMPort']),
            nas['Username'],
            nas['Password'],
            nas['Secure'],
            nas['Cert_Verify'],
        )

        abb_hypervisor = sess.list_vm_hypervisor()
        abb_vms = sess.list_device_transfer_size()

        if 'data' not in abb_hypervisor or 'data' not in abb_vms:
            print(f"[{name}] unexpected response")
            g_scrape_ok.labels(name).set(0)
            return

        hypervisor_list = {
            h.get('inventory_id', ''): h.get('host_name', 'unknown')
            for h in abb_hypervisor['data']
        }

        for vm in abb_vms['data'].get('device_list', []):
            dev = vm.get('device', {})
            hypervisor = hypervisor_list.get(dev.get('inventory_id', ''), 'unknown')
            vmname = dev.get('host_name', 'unknown')
            vmuuid = dev.get('device_uuid', 'unknown')
            vmos = dev.get('os_name', 'unknown')

            transfers = vm.get('transfer_list', [])
            if not transfers:
                continue

            t = transfers[0]
            g_last_ts.labels(name, vmname, hypervisor, vmuuid, vmos).set(t.get('time_end', 0))
            g_duration.labels(name, vmname, hypervisor, vmuuid, vmos).set(
                t.get('time_end', 0) - t.get('time_start', 0)
            )
            g_bytes.labels(name, vmname, hypervisor, vmuuid, vmos).set(t.get('transfered_bytes', 0))
            g_result.labels(name, vmname, hypervisor, vmuuid, vmos).set(t.get('status', -1))

        g_scrape_ok.labels(name).set(1)
        print(f"[{name}] OK")

    except Exception as e:
        print(f"[{name}] ERROR: {e}")
        g_scrape_ok.labels(name).set(0)


def collect_all():
    for nas in NAS_LIST:
        collect_one(nas)


REQUEST_TIME = Summary('request_processing_seconds', 'Time spent processing request')


@REQUEST_TIME.time()
def process_request(t):
    time.sleep(t)


if __name__ == '__main__':
    print("Synology Backup Exporter (multi-NAS) starting...")

    # 启动时打印 config 路径，方便排查
    print(f"Using config: {resource_path('config.json')}")

    collect_all()
    start_http_server(EXPORTER_PORT)
    print(f"Web Server running on Port {EXPORTER_PORT}")

    while True:
        process_request(random.random())
        time.sleep(300)
        collect_all()
