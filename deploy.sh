#!/bin/bash
git pull origin main
docker compose up --build -d
docker exec -it neo4j neo4j start
docker exec -it neo4j service cron start