FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8001

CMD ["python", "main.py"]

##----------------------------------------------------------------------
# FROM openjdk:11-jdk-slim

# RUN apt-get update && apt-get install -y \
#     python3.9 \
#     python3-pip \
#     wget \
#     procps \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/*

# WORKDIR /app

# ENV C_INCLUDE_PATH=/usr/include/librdkafka
# ENV LDFLAGS="-L/usr/lib"

# COPY requirements.txt .
# RUN pip3 install --no-cache-dir -r requirements.txt
# COPY . .
# EXPOSE 8001

# RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.1.2/spark-sql-kafka-0-10_2.12-3.1.2.jar -P /opt/spark/jars/
# RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.12/3.1.2/spark-streaming-kafka-0-10_2.12-3.1.2.jar -P /opt/spark/jars/
# CMD ["spark-submit", "--jars", "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.1.2.jar,/opt/spark/jars/spark-streaming-kafka-0-10_2.12-3.1.2.jar", "main.py"]