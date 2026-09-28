FROM apache/iceberg-rest-fixture:1.8.1

# The fixture bundles only SQLite, whose single-writer locking fails concurrent Iceberg commits
# (SQLITE_BUSY_SNAPSHOT -> CommitStateUnknownException -> Flink restart loop). Add the Postgres
# JDBC driver so the catalog can run on a real database.
ADD --chmod=644 https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /usr/lib/iceberg-rest/
CMD ["java", "-cp", "iceberg-rest-adapter.jar:postgresql-42.7.4.jar", "org.apache.iceberg.rest.RESTCatalogServer"]
