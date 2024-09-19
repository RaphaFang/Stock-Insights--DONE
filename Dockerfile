FROM python:3.11-slim AS builder

RUN pip install --upgrade pip && \
    pip install apache-flink

FROM bitnami/flink:latest

USER root

COPY --from=builder /usr/local /usr/local

WORKDIR /opt/flink-app

CMD ["/opt/bitnami/scripts/flink/run.sh"]



# FROM bitnami/flink:latest

# USER root

# RUN apt-get update && \
#     apt-get install -y python3 python3-pip && \
#     ln -s /usr/bin/python3 /usr/bin/python && \
#     /usr/bin/python3 -m pip install --break-system-packages apache-flink

# ENV PYTHONPATH="/usr/local/lib/python3.11/dist-packages"

# WORKDIR /opt/flink-app

# CMD ["/opt/bitnami/scripts/flink/run.sh"]