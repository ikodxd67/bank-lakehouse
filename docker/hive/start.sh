#!/bin/bash
# Entrypoint образа Hive 3 на каждом старте делает schematool -initSchema и падает,
# если схема уже есть. Здесь схема создаётся только в пустой базе.
if [ "${SERVICE_NAME}" == "metastore" ] && /opt/hive/bin/schematool -dbType postgres -info > /dev/null 2>&1; then
  export IS_RESUME=true
fi
exec /entrypoint.sh "$@"
