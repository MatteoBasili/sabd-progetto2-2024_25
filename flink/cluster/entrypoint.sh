#!/bin/sh
chmod -R 777 /tmp/flink-checkpoints

exec /docker-entrypoint.sh "$@"

