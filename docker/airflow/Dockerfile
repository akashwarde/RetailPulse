FROM apache/airflow:2.10.5-python3.12

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && ln -s "$(dirname $(dirname $(readlink -f $(command -v java))))" /opt/java \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/opt/java
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYTHONPATH=/opt/airflow/src

USER airflow

COPY requirements.txt /requirements.txt
COPY requirements-dev.txt /requirements-dev.txt
RUN grep -v "confluent-kafka" /requirements.txt > /tmp/reqs.txt \
    && pip install --no-cache-dir \
       --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt" \
       -r /tmp/reqs.txt \
    && pip install --no-cache-dir confluent-kafka==2.8.2 \
    && pip install --no-cache-dir -r /requirements-dev.txt \
    && rm /tmp/reqs.txt

COPY --chown=airflow:root src /opt/airflow/src
COPY --chown=airflow:root sql /opt/airflow/sql
COPY --chown=airflow:root tests /opt/airflow/tests
COPY --chown=airflow:root compose.yaml compose.streaming.yml /opt/airflow/
COPY --chown=airflow:root docker /opt/airflow/docker
COPY --chown=airflow:root --chmod=0755 docker/airflow/entrypoint.sh /opt/airflow/retailpulse-entrypoint.sh

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/opt/airflow/retailpulse-entrypoint.sh"]
