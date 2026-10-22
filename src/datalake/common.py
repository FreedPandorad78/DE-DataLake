import json
import logging
import os
import re
from datetime import datetime, timezone

import boto3
import yaml
from botocore.exceptions import ClientError

REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")


def bucket():
    return os.environ["DATALAKE_BUCKET"]


def cliente_s3():
    return boto3.client("s3", region_name=REGION)


def ahora_utc():
    return datetime.now(timezone.utc)


def cargar_fuentes(ruta=None):
    ruta = ruta or os.environ.get("SOURCES_FILE", "/opt/airflow/datalake_config/sources.yaml")
    with open(ruta, encoding="utf-8") as f:
        return yaml.safe_load(f)["sources"]


def lote_seguro(run_id):
    """Convierte el run_id de Airflow en un texto válido para rutas de S3."""
    return re.sub(r"[^A-Za-z0-9_.=-]", "_", run_id)


def leer_json(cli, key):
    try:
        cuerpo = cli.get_object(Bucket=bucket(), Key=key)["Body"].read()
    except cli.exceptions.NoSuchKey:
        return None
    return json.loads(cuerpo)


def escribir_json(cli, key, datos):
    cli.put_object(
        Bucket=bucket(), Key=key, ContentType="application/json",
        Body=json.dumps(datos, ensure_ascii=False, indent=2).encode("utf-8"),
    )


def existe(cli, key):
    try:
        cli.head_object(Bucket=bucket(), Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


class RegistroLote:
    """Logger que escribe lote, tarea y fuente en cada línea, para saber qué falló y dónde."""

    def __init__(self, tarea, lote):
        self._log = logging.getLogger("airflow.task")
        self.tarea, self.lote = tarea, lote

    def _fmt(self, msg, fuente):
        return f"[lote={self.lote}] [tarea={self.tarea}] [fuente={fuente or '-'}] {msg}"

    def info(self, msg, fuente=None):
        self._log.info(self._fmt(msg, fuente))

    def warning(self, msg, fuente=None):
        self._log.warning(self._fmt(msg, fuente))

    def error(self, msg, fuente=None):
        self._log.error(self._fmt(msg, fuente))
