FROM apache/hive:4.0.0

USER root

# Add S3A support (hadoop-aws + AWS SDK bundle) for MinIO access.
# The base image includes hadoop but not hadoop-aws, and has no curl.
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends wget && \
    rm -rf /var/lib/apt/lists/* && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
        -O /opt/hive/lib/hadoop-aws-3.3.4.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar \
        -O /opt/hive/lib/aws-java-sdk-bundle-1.12.367.jar && \
    wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar \
        -O /opt/hive/lib/postgresql-42.7.5.jar

USER hive
