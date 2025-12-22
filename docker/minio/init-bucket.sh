#!/usr/bin/env bash

set -e

mc alias set local http://localhost:9000 minioadmin minioadmin

mc mb local/touchgrass || true

mc ls local
