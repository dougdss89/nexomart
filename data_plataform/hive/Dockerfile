FROM apache/hive:4.0.0

USER root

# Add S3A support (hadoop-aws + AWS SDK bundle) for MinIO access.
# The base image includes hadoop but not hadoop-aws.
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
        -o /opt/hive/lib/hadoop-aws-3.3.4.jar && \
    curl -fsSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar \
        -o /opt/hive/lib/aws-java-sdk-bundle-1.12.367.jar

USER hive
