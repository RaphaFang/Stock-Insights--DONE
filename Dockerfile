FROM bitnami/flink:latest

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python

ENV JAVA_HOME="/opt/bitnami/java"
ENV PATH="$JAVA_HOME/bin:$PATH"

ENV PIP_BREAK_SYSTEM_PACKAGES=1

RUN python -m pip install --upgrade pip && \
    python -m pip install apache-flink

WORKDIR /opt/flink-app

CMD ["/opt/bitnami/scripts/flink/run.sh"]

# -----------------------
# optimize, set 1 RUN for slimming the docker