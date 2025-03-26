#!/bin/bash
/var/lib/neo4j/bin/neo4j stop
/var/lib/neo4j/bin/neo4j-admin dump --database=neo4j --to=/dumps/neo4j/backup_$(date +%Y%m%d%H%M).dump
/var/lib/neo4j/bin/neo4j start
find "/dumps/neo4j" -type f -mtime +10 -exec rm {} \;
