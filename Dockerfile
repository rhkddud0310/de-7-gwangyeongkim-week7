FROM apache/airflow:2.9.2

USER root

# JDK 설치
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
         default-jdk-headless \
         procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

# Python 3.12 공식 제약조건 적용 초고속 설치
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.2/constraints-3.12.txt"