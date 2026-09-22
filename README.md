# Prometheus_Synology_Backup
Check Synology abb Backup status

需要使用大于Python 3.10以上

**pip需要安装**

certifi==2020.12.5
chardet==3.0.4
idna==2.10
prometheus-client==0.9.0
requests==2.25.0
synology-api==0.1.3.1
urllib3==1.26.2


端口 9771

Prometheus 配置

  - job_name: "synology_backup"
    static_configs:
      - targets: ['0.0.0.0:9771']
