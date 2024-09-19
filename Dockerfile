FROM bitnami/flink:latest

USER root

RUN apt-get update && apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python

RUN pip install pyflink

WORKDIR /opt/flink-app

CMD ["/opt/bitnami/scripts/flink/run.sh"]


