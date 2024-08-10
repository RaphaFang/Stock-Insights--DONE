FROM openjdk:11-jdk-slim

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

# 下载安裝Spark和Kafka連接機制
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.1.2/spark-sql-kafka-0-10_2.12-3.1.2.jar -P /opt/spark/jars/
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.12/3.1.2/spark-streaming-kafka-0-10_2.12-3.1.2.jar -P /opt/spark/jars/


COPY . .

# COPY wait.sh /wait.sh
# RUN chmod +x /wait.sh

EXPOSE 8000

CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2", "main.py"]

# "/wait.sh",