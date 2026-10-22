#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source recursos_datalake.env
COPYFILE_DISABLE=1 tar --exclude=.git --exclude='recursos_*.env' --exclude='.env' --exclude='infra/airflow/logs' \
  -czf - dags src config sql infra \
  | ssh -i ~/.ssh/taller-datalake-key.pem ec2-user@$EC2_IP "mkdir -p ~/DE-DataLake && tar xzf - -C ~/DE-DataLake"
echo "Código sincronizado con la EC2"
