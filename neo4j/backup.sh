#!/bin/bash
neo4j stop
neo4j-admin dump --database=neo4j --to=/dumps/neo4j/backup_$(date +%Y%m%d).dump
neo4j start