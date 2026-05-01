FROM flink:1.18.1-java17

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip curl \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

COPY flink-job/requirements.txt /tmp/flink-requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/flink-requirements.txt

RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.18.jar \
        https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.18/flink-sql-connector-kafka-3.2.0-1.18.jar

COPY flink-job/ /opt/flink/jobs/
RUN chmod +x /opt/flink/jobs/submit_job.sh

USER flink
