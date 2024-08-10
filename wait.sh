#!/bin/bash

# 等待Kafka端口變為可用
while ! nc -z kafka 9092; do
  echo "等待 Kafka 啟動..."
  sleep 2
done

# 當 Kafka 可用後，啟動應用程式
echo "Kafka 已啟動，啟動應用程式..."
python -u main.py