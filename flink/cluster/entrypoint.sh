#!/bin/sh
chmod -R 777 /data/output

exec /docker-entrypoint.sh "$@"

