# Один образ на весь кластер: NameNode, DataNode, ResourceManager, NodeManager,
# History Server и клиент spark-submit. На NodeManager нужен тот же Python, что
# у драйвера PySpark, поэтому Spark и Python живут в одном образе с Hadoop.
FROM apache/hadoop:3.5.0

ARG SPARK_VERSION=3.5.9
ARG ICEBERG_VERSION=1.12.0
ARG PG_JDBC_VERSION=42.7.13
ARG MAVEN=https://repo1.maven.org/maven2

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3-setuptools zip \
 && rm -rf /var/lib/apt/lists/*

# Сначала CDN Apache (быстрый, но только актуальные версии), потом архив (все
# версии, но бывает очень медленным: в CI однажды отдавал 0,1 МБ/с).
RUN SPARK_TGZ="spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
 && (curl -fsSL -o /tmp/spark.tgz "https://dlcdn.apache.org/spark/${SPARK_TGZ}" \
     || curl -fsSL -o /tmp/spark.tgz "https://archive.apache.org/dist/spark/${SPARK_TGZ}") \
 && tar -xzf /tmp/spark.tgz -C /opt && rm /tmp/spark.tgz \
 && mv "/opt/spark-${SPARK_VERSION}-bin-hadoop3" /opt/spark \
 && cd /opt/spark/jars \
 && curl -fsSLO "${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
 && curl -fsSLO "${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar" \
 && curl -fsSLO "${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar" \
 && curl -fsSLO "${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" \
 && curl -fsSLO "${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" \
 && curl -fsSLO "${MAVEN}/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar" \
 && cd /opt/spark/jars && zip -q -0 /opt/spark/spark-libs.zip *.jar

COPY conf/ /opt/hadoop/etc/hadoop/
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data /var/log/hadoop \
 && chown -R hadoop:users /hadoop /var/log/hadoop /opt/spark

# YARN_CONF_DIR не задаётся: starter.sh образа превращает переменные вида
# <имя>_<формат>_<ключ> в файлы конфигурации и принял бы её за yarn.conf.
# Spark находит YARN по HADOOP_CONF_DIR.
ENV SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:${PATH} \
    TZ=Asia/Almaty

USER hadoop
