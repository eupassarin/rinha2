#!/bin/bash

sudo docker compose down  --volumes
sudo docker compose build
sudo docker compose up&