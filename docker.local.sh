#!/bin/bash

docker compose -f docker-compose.local.yml rm -f  --volumes
docker compose -f docker-compose.local.yml build
docker compose -f docker-compose.local.yml up