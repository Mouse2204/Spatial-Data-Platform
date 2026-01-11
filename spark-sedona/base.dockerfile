ARG PYTHON_VERSION=3.9-slim-bookworm
FROM python:${PYTHON_VERSION}

ARG PYSPARK_VERSION=3.4.0
ARG HADOOP_VERSION=3.3.4

WORKDIR /app

RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    wget \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV HADOOP_HOME="/usr/hadoop"

RUN pip install --no-cache-dir pyspark==${PYSPARK_VERSION}

ENV SPARK_HOME="/usr/local/lib/python3.9/site-packages/pyspark"
ENV PATH="$PATH:$JAVA_HOME/bin:$SPARK_HOME/bin:$HADOOP_HOME/bin"

RUN wget -q "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz \
    && mv hadoop-${HADOOP_VERSION} ${HADOOP_HOME} \
    && rm hadoop-${HADOOP_VERSION}.tar.gz