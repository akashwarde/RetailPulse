FROM apache/airflow:2.10.5-python3.12

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYTHONPATH=/opt/airflow/src

USER airflow

COPY requirements.txt /requirements.txt

RUN grep -v "confluent-kafka" /requirements.txt > /tmp/reqs.txt \
    && pip install --no-cache-dir \
       --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt" \
       -r /tmp/reqs.txt \
    && pip install --no-cache-dir confluent-kafka==2.8.2 \
    && rm /tmp/reqs.txt
