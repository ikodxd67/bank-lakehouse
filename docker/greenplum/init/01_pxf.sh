#!/bin/bash
# Сервер PXF по умолчанию смотрит в наш HDFS. Запускается один раз при инициализации кластера.
set -euo pipefail
source /home/gpadmin/.bashrc
server_dir="${PXF_BASE}/servers/default"
mkdir -p "${server_dir}"

cat > "${server_dir}/core-site.xml" <<'XML'
<?xml version="1.0"?>
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://namenode:8020</value></property>
</configuration>
XML

cat > "${server_dir}/hdfs-site.xml" <<'XML'
<?xml version="1.0"?>
<configuration>
  <property><name>dfs.replication</name><value>1</value></property>
</configuration>
XML

# без Kerberos подмена пользователя не нужна: PXF читает HDFS от имени gpadmin
cat > "${server_dir}/pxf-site.xml" <<'XML'
<?xml version="1.0"?>
<configuration>
  <property><name>pxf.service.user.impersonation</name><value>false</value></property>
</configuration>
XML

pxf cluster sync
pxf cluster restart
