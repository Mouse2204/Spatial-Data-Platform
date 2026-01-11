ARG PYTHON_VERSION=3.9.5
ARG PYSPARK_VERSION=3.4.0
FROM local_pyspark:Py_${PYTHON_VERSION}_Spark_${PYSPARK_VERSION}

ARG SCALA_VERSION=2.12
ARG SEDONA_VERSION=1.4.1
ARG DELTA_CORE_VERSION=2.4.0
ARG GEOTOOLS_WRAPPER_VERSION=1.4.0-28.2
ARG AWS_SDK_VERSION=1.12.262 

WORKDIR /app

RUN pip install --no-cache-dir \
    apache-sedona==${SEDONA_VERSION} \
    delta-spark==${DELTA_CORE_VERSION} \
    shapely

RUN wget -q https://repo1.maven.org/maven2/io/delta/delta-core_${SCALA_VERSION}/${DELTA_CORE_VERSION}/delta-core_${SCALA_VERSION}-${DELTA_CORE_VERSION}.jar -P ${SPARK_HOME}/jars/ \
 && wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_CORE_VERSION}/delta-storage-${DELTA_CORE_VERSION}.jar -P ${SPARK_HOME}/jars/

RUN wget -q https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.0_${SCALA_VERSION}/${SEDONA_VERSION}/sedona-spark-shaded-3.0_${SCALA_VERSION}-${SEDONA_VERSION}.jar -P ${SPARK_HOME}/jars/ \
 && wget -q https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/${GEOTOOLS_WRAPPER_VERSION}/geotools-wrapper-${GEOTOOLS_WRAPPER_VERSION}.jar -P ${SPARK_HOME}/jars/

RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P ${SPARK_HOME}/jars/ \
 && wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -P ${SPARK_HOME}/jars/

ENV SPARK_LOCAL_IP=""