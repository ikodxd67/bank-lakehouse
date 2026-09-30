# Hive 3.1, а не 4.x: Spark 3.5 и Iceberg HiveCatalog ходят в Metastore клиентом
# Hive 2.3, который вызывает get_table, а в Metastore 4.0+ этого метода нет
# (проверено на 4.0.1 и 4.2.1: "Invalid method name: 'get_table'").
FROM apache/hive:3.1.3

ARG PG_JDBC_VERSION=42.7.13
USER root
# в образе нет curl, jar скачивает сам docker build
RUN rm -f /opt/hive/lib/postgresql-9.4.1208.jre7.jar
ADD --chmod=644 https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar /opt/hive/lib/
# core-site и hdfs-site общие с кластером; entrypoint образа делает этот каталог HADOOP_CONF_DIR
COPY hive/conf/ /hive_custom_conf/
COPY hadoop/conf/core-site.xml hadoop/conf/hdfs-site.xml /hive_custom_conf/
COPY --chmod=755 hive/start.sh /opt/start.sh
ENV HIVE_CUSTOM_CONF_DIR=/hive_custom_conf TZ=Asia/Almaty
USER hive
ENTRYPOINT ["/opt/start.sh"]
