# Airflow запускает Spark на YARN через spark-submit в режиме cluster: драйвер
# живёт в YARN, а здесь нужен только клиент — Java, дистрибутив Spark и
# конфигурация Hadoop. Всё это берётся из образа кластера, чтобы версии совпадали.
FROM bank-lakehouse/hadoop:local AS cluster

FROM apache/airflow:3.3.2-python3.12

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/*
COPY --from=cluster --chown=airflow:root /opt/spark /opt/spark
COPY --from=cluster /opt/hadoop/etc/hadoop /opt/hadoop-conf
# архив jar-ов уже лежит в HDFS, клиенту он не нужен
RUN rm -f /opt/spark/spark-libs.zip \n && mkdir /opt/dbt-venv && chown airflow:root /opt/dbt-venv
ENV SPARK_HOME=/opt/spark \
    HADOOP_CONF_DIR=/opt/hadoop-conf \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH=${PATH}:/opt/spark/bin \
    TZ=Asia/Almaty

USER airflow
# провайдер ставится по constraints той же версии Airflow, иначе pip подтянет новый Airflow
RUN pip install --no-cache-dir \
      --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt \
      "apache-airflow==3.3.2" apache-airflow-providers-apache-spark
# dbt в своём окружении: его зависимости (protobuf, agate) спорят с Airflow
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-postgres>=1.10,<1.12"
