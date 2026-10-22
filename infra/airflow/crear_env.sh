#!/usr/bin/env bash
# Se ejecuta UNA vez dentro de la EC2. Genera las claves de Airflow; no se suben al repo.
set -euo pipefail
cd "$(dirname "$0")"
BUCKET="${1:?uso: ./crear_env.sh <bucket>}"
if [ -f .env ]; then echo ".env ya existe; no se sobrescribe"; exit 0; fi
FERNET=$(python3 -c "import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())")
cat > .env << EOF
POSTGRES_PASSWORD=$(openssl rand -hex 16)
FERNET_KEY=$FERNET
WEBSERVER_SECRET=$(openssl rand -hex 24)
AIRFLOW_ADMIN_PASSWORD=$(openssl rand -hex 8)
DATALAKE_BUCKET=$BUCKET
AWS_DEFAULT_REGION=us-east-2
GLUE_DATABASE=taller_datalake
ATHENA_WORKGROUP=taller-datalake-wg
EOF
chmod 600 .env
mkdir -p logs && chmod 777 logs
echo "Listo. Usuario de Airflow: admin. Clave: grep AIRFLOW_ADMIN_PASSWORD .env"
