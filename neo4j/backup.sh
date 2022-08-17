#!/bin/bash
/var/lib/neo4j/bin/neo4j stop
/var/lib/neo4j/bin/neo4j-admin dump --database=neo4j --to=/dumps/neo4j/backup_$(date +%Y%m%d%H).dump
/var/lib/neo4j/bin/neo4j start