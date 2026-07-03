#!/bin/bash
set -e

superset db upgrade

superset fab create-admin \
  --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}" \
  --firstname "${SUPERSET_ADMIN_FIRSTNAME:-Superset}" \
  --lastname "${SUPERSET_ADMIN_LASTNAME:-Admin}" \
  --email "${SUPERSET_ADMIN_EMAIL:-admin@admin.com}" \
  || echo "Admin user already exists, skipping"

superset init