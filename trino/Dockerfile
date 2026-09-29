# Trino with only the Iceberg plugin. The image ships 56 plugins, and loading their classes took so
# much native memory that the 1 GB heap left no headroom in the 1.5 GB container: the kernel stalled
# Trino reclaiming memory up to a third of the time (cgroup memory.pressure), and queries timed out.
FROM trinodb/trino:470
USER root
RUN cd /usr/lib/trino/plugin && for p in *; do [ "$p" = iceberg ] || rm -rf "$p"; done
USER trino
