FROM bitnami/flink:latest

USER root
RUN apt-get update && \
    apt-get install -y python3.12 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /opt/flink-app

RUN pip3 install apache-flink==1.20.0 kafka-python==2.0.2
