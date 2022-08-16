#!/bin/bash
git pull origin main
touch ./update/log.txt
docker compose up --build -d