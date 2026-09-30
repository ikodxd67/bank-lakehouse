# Kafka Connect с Debezium. Готовые образы debezium/connect на Docker Hub
# остановились на 3.0.0 (2024), поэтому воркер собирается из образа Kafka
# и плагина с Maven Central.
FROM apache/kafka:4.2.2

ARG DEBEZIUM_VERSION=3.7.0.Final

USER root
RUN mkdir -p /opt/connectors \
 && wget -q -O /tmp/dbz.tar.gz \
      "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz" \
 && tar -xzf /tmp/dbz.tar.gz -C /opt/connectors \
 && rm /tmp/dbz.tar.gz \
 && chown -R appuser:appuser /opt/connectors

COPY connect.properties /opt/kafka/config/connect-bank.properties
USER appuser

CMD ["/opt/kafka/bin/connect-distributed.sh", "/opt/kafka/config/connect-bank.properties"]
