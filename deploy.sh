#!/bin/bash
git pull origin main
touch log.txt
docker compose up --build -d