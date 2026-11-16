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

ENV SPARK_VERSION=4.1.1
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

WORKDIR /opt/spark

RUN curl -fSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark.tgz \
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
# Spark 4 needs the dedicated dataflint-spark4 artifact: the spark_2.13 one is built against
# Spark 3.5 (javax.servlet) and fails at plugin init on Spark 4 (jakarta.servlet).
RUN curl -fSL https://repo1.maven.org/maven2/io/dataflint/dataflint-spark4_2.13/0.9.9/dataflint-spark4_2.13-0.9.9.jar \
        -o "$SPARK_HOME/jars/dataflint-spark4_2.13-0.9.9.jar" && \
    chmod +x entrypoint.sh && \
    mkdir -p /opt/spark/spark-events && \
    chown spark:spark "$SPARK_HOME/jars/dataflint-spark4_2.13-0.9.9.jar" && \
    chown -R spark:spark /opt/spark/spark-events

# hadoop-aws + AWS SDK v2 bundle baked into the image so the History Server can read
# s3a://spark-events/apps on first boot, without depending on a prior spark-submit job
# having populated the shared Ivy cache (spark.jars.packages only resolves via SparkSubmit,
# which Master/Worker/HistoryServer daemons bypass).
# Spark 4.1 ships Hadoop 3.4.2, and hadoop-aws 3.4.x moved from the v1 aws-java-sdk-bundle to
# the SDK v2 bundle. Keep hadoop-aws and the bundle at exactly these versions (the bundle
# version is the one hadoop-project-3.4.2 declares as aws-java-sdk-v2.version).
RUN curl -fSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar \
        -o "$SPARK_HOME/jars/hadoop-aws-3.4.2.jar" && \
    curl -fSL https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar \
        -o "$SPARK_HOME/jars/awssdk-bundle-2.29.52.jar" && \
    chown spark:spark "$SPARK_HOME/jars/hadoop-aws-3.4.2.jar" "$SPARK_HOME/jars/awssdk-bundle-2.29.52.jar"

# Delta Lake baked into every node (master/worker/history) instead of relying only on the driver
# shipping it via spark.jars.packages. Keep DELTA_VERSION in sync with spark.jars.packages in
# conf/spark-defaults.conf.template — Delta is no longer listed there, so the jar below is the only copy
# (resolving it via spark.jars.packages too would put two Delta versions on the executor classpath).
# Python bindings (delta.tables) installed with --no-deps: pyspark already lives in $SPARK_HOME/python.
ARG DELTA_VERSION=4.1.0
RUN curl -fSL https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/${DELTA_VERSION}/delta-spark_2.13-${DELTA_VERSION}.jar \
        -o "$SPARK_HOME/jars/delta-spark_2.13-${DELTA_VERSION}.jar" && \
    curl -fSL https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar \
        -o "$SPARK_HOME/jars/delta-storage-${DELTA_VERSION}.jar" && \
    chown spark:spark "$SPARK_HOME/jars/delta-spark_2.13-${DELTA_VERSION}.jar" "$SPARK_HOME/jars/delta-storage-${DELTA_VERSION}.jar" && \
    pip install --no-cache-dir --no-deps delta-spark==${DELTA_VERSION}

# Iceberg runtime baked in (iceberglake catalog, legacy AdventureWorks). It must live in the same
# classloader as the DataFlint jar above: DataFlint probes org.apache.iceberg classes at plugin init
# and fails with NoClassDefFoundError when the Iceberg jar comes from spark.jars.packages instead.
RUN curl -fSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.11.0/iceberg-spark-runtime-4.1_2.13-1.11.0.jar \
        -o "$SPARK_HOME/jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar" && \
    chown spark:spark "$SPARK_HOME/jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar"

USER spark
ENTRYPOINT ["/entrypoint.sh"]
