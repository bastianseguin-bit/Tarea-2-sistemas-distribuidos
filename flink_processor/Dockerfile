FROM flink:1.18.1-scala_2.12-java11


RUN apt-get update && \
    apt-get install -y wget python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python


RUN pip3 install --upgrade pip


RUN wget -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar


COPY flink_job.py /opt/flink/flink_job.py


RUN pip3 install --no-cache-dir apache-flink && \
    pip3 install --no-cache-dir kafka-python && \
    pip3 install --no-cache-dir sentence-transformers

WORKDIR /opt/flink

CMD ["python3", "flink_job.py"]