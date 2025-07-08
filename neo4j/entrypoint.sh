#!/bin/bash

service cron start

exec /docker-entrypoint.sh neo4j