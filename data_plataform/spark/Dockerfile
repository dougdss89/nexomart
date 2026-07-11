FROM python:3.12-slim-bookworm AS spark-base

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      curl \
      sudo \
      openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN useradd -m -s /bin/bash spark && \
    echo 'spark ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/spark

ENV SPARK_VERSION=3.5.8
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

WORKDIR /opt/spark

RUN curl -fSL https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3-scala2.13.tgz -o spark.tgz \
    && tar -xzf spark.tgz --strip-components 1 \
    && rm spark.tgz \
    && chown -R spark:spark /opt/spark

FROM spark-base AS pyspark

COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Portas: 7077 (Master RPC), 8080 (UI Master), 18080 (History), 15002 (Connect)
EXPOSE 7077 8080 18080 15002

ENV PYSPARK_PYTHON=python3
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-*.zip:$PYTHONPATH

COPY conf/spark-defaults.conf "$SPARK_HOME/conf/"
COPY entrypoint.sh .

# DataFlint JAR in $SPARK_HOME/jars/ so it loads for Spark sessions AND History Server.
# Scala 2.13 artifact: spark_2.13 (matches spark-*-bin-hadoop3-scala2.13.tgz).
RUN curl -fSL https://repo1.maven.org/maven2/io/dataflint/spark_2.13/0.9.9/spark_2.13-0.9.9.jar \
        -o "$SPARK_HOME/jars/spark_2.13-0.9.9.jar" && \
    chmod +x entrypoint.sh && \
    mkdir -p /opt/spark/spark-events && \
    chown spark:spark "$SPARK_HOME/jars/spark_2.13-0.9.9.jar" && \
    chown -R spark:spark /opt/spark/spark-events

# hadoop-aws + aws-java-sdk-bundle baked into the image so the History Server can read
# s3a://spark-events/apps on first boot, without depending on a prior spark-submit job
# having populated the shared Ivy cache (spark.jars.packages only resolves via SparkSubmit,
# which Master/Worker/HistoryServer daemons bypass).
RUN curl -fSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
        -o "$SPARK_HOME/jars/hadoop-aws-3.3.4.jar" && \
    curl -fSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
        -o "$SPARK_HOME/jars/aws-java-sdk-bundle-1.12.262.jar" && \
    chown spark:spark "$SPARK_HOME/jars/hadoop-aws-3.3.4.jar" "$SPARK_HOME/jars/aws-java-sdk-bundle-1.12.262.jar"

USER spark
ENTRYPOINT ["/entrypoint.sh"]
