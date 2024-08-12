FROM apache/spark-py:v3.1.2

# 安裝額外的 Python 依賴
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# 複製您的應用程序代碼
COPY . .

# 設置 Kafka 連接器
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.1.2/spark-sql-kafka-0-10_2.12-3.1.2.jar -P $SPARK_HOME/jars/

# 運行您的應用程序
CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2", "main.py"]