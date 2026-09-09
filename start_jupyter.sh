#!/usr/bin/env bash

set -e

sudo docker compose run --rm --service-ports spark \
  jupyter lab \
  --ip=0.0.0.0 \
  --port=8888 \
  --no-browser \
  --ServerApp.root_dir=/app
