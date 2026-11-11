# Python 3.10 để khớp với Python trên Spark worker (image Spark 3.5.3 chạy Ubuntu 22.04)
FROM apache/airflow:2.10.5-python3.10

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
RUN pip install --no-cache-dir "apache-airflow==2.10.5" apache-airflow-providers-apache-spark \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.10.txt" \
 && pip install --no-cache-dir pyspark==3.5.3 kafka-python-ng
