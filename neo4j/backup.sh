#!/bin/bash

/var/lib/neo4j/bin/cypher-shell -u $Neo4jUser -p $Neo4jPassword -d system "STOP DATABASE neo4j;"
/var/lib/neo4j/bin/neo4j-admin dump --database=neo4j --to=/dumps/neo4j/backup_$(date +%Y%m%d%H%M).dump
/var/lib/neo4j/bin/cypher-shell -u $Neo4jUser -p $Neo4jPassword -d system "START DATABASE neo4j;"