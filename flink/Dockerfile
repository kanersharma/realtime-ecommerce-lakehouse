FROM flink:1.20.1-scala_2.12-java11

# Connectors dropped straight into Flink's classpath (versions must match Flink 1.20).
ARG MVN=https://repo1.maven.org/maven2
# Kafka source for Flink SQL
ADD --chmod=644 ${MVN}/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar /opt/flink/lib/
# Iceberg sink + catalog
ADD --chmod=644 ${MVN}/org/apache/iceberg/iceberg-flink-runtime-1.20/1.8.1/iceberg-flink-runtime-1.20-1.8.1.jar /opt/flink/lib/
# S3FileIO (AWS SDK v2) so Iceberg can write to S3 (RustFS)
ADD --chmod=644 ${MVN}/org/apache/iceberg/iceberg-aws-bundle/1.8.1/iceberg-aws-bundle-1.8.1.jar /opt/flink/lib/
# Iceberg's Flink catalog still references Hadoop's Configuration class
ADD --chmod=644 ${MVN}/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar /opt/flink/lib/
