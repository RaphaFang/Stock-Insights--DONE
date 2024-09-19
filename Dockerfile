FROM bitnami/flink:latest

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python

ENV JAVA_HOME="/opt/bitnami/java"
ENV PATH="$JAVA_HOME/bin:$PATH"

RUN python -m pip install --upgrade pip && \
    python -m pip install --break-system-packages apache-flink

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