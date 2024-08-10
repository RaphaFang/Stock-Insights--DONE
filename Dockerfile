FROM openjdk:11-jdk-slim AS builder

RUN apt-get update && apt-get install -y \
    python3.9 \
    python3-pip \
    wget \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

FROM bitnami/spark:3.5.1

# 安裝 wget
USER root
RUN install_packages wget

RUN mkdir -p /opt/bitnami/spark/custom-jars
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar -P /opt/bitnami/spark/custom-jars/ \
    && wget https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.12/3.5.1/spark-streaming-kafka-0-10_2.12-3.5.1.jar -P /opt/bitnami/spark/custom-jars/


COPY . /app

# COPY --from=builder /usr/local/lib/python3.9 /usr/local/lib/python3.9
# COPY --from=builder /usr/local/bin/pip* /usr/local/bin/
# COPY --from=builder /usr/local/bin/python3* /usr/local/bin/
RUN mkdir -p /opt/bitnami/spark/tmp && chmod 777 /opt/bitnami/spark/tmp
USER 1001

WORKDIR /app


EXPOSE 8000


CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", "/app/sp/spark_application_first.py"]


# FROM openjdk:11-jdk-slim
# RUN apt-get update && apt-get install -y \
#     python3.9 \
#     python3-pip \
#     wget \
#     procps \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/*
# WORKDIR /app
# COPY requirements.txt .
# RUN pip3 install --no-cache-dir -r requirements.txt

# FROM bitnami/spark:3.5.1
# # RUN apt-get clean && apt-get update && apt-get install -y wget
# RUN mkdir -p /opt/bitnami/spark/custom-jars
# RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.1.2/spark-sql-kafka-0-10_2.12-3.1.2.jar -P /opt/bitnami/spark/custom-jars/ \
#     && wget https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.12/3.1.2/spark-streaming-kafka-0-10_2.12-3.1.2.jar -P /opt/bitnami/spark/custom-jars/
# COPY . .
# EXPOSE 8000
# CMD ["spark-submit", "/app/sp/spark_application_first.py"]


# # CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2", "main.py"]

