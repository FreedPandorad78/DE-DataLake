import os
from datetime import datetime, timedelta

import boto3
from airflow.decorators import dag, task
from airflow.models.param import Param

from datalake import bronze
from datalake.common import RegistroLote, REGION, bucket, cargar_fuentes, cliente_s3, leer_json, lote_seguro

SCHEDULE = None  # durante el laboratorio se cambia a una frecuencia fija, por ejemplo "@hourly"


@dag(
    dag_id="datalake_noticias",
    schedule=SCHEDULE,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,  # control de concurrencia: nunca dos corridas escribiendo a la vez
    is_paused_upon_creation=True,
    params={
        "reprocess_run_id": Param(
            "", type="string",
            description="Vacío: nueva extracción. Con el run_id de un lote de Bronze: reprocesa ese lote sin consultar los RSS.",
        )
    },
    default_args={
        "retries": 2, "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True, "max_retry_delay": timedelta(minutes=10),
    },
    tags=["datalake", "noticias"],
)
def datalake_noticias():
    @task
    def validar_configuracion(params=None, dag_run=None):
        reproceso = ((params or {}).get("reprocess_run_id") or "").strip()
        lote = reproceso or lote_seguro(dag_run.run_id)
        log = RegistroLote("validar_configuracion", lote)

        fuentes = cargar_fuentes()
        log.info(f"{len(fuentes)} fuentes configuradas: {[f['source_id'] for f in fuentes]}")
        cli = cliente_s3()
        cli.head_bucket(Bucket=bucket())
        boto3.client("glue", region_name=REGION).get_database(Name=os.environ["GLUE_DATABASE"])
        boto3.client("athena", region_name=REGION).get_work_group(WorkGroup=os.environ["ATHENA_WORKGROUP"])
        log.info("acceso verificado: S3, Glue y Athena")

        if reproceso:
            resumen = leer_json(cli, bronze.clave_resumen(reproceso))
            if resumen is None:
                raise ValueError(f"El lote {reproceso} no existe en Bronze")
            log.info("modo REPROCESO: se reutiliza el lote existente de Bronze")
            return {"modo": "REPROCESO", "lote": reproceso, "fecha": resumen["ingestion_date"]}
        fecha = dag_run.start_date.date().isoformat()  # fija durante los reintentos de la misma corrida
        log.info(f"modo NUEVA_EXTRACCION, fecha de ingesta {fecha}")
        return {"modo": "NUEVA_EXTRACCION", "lote": lote, "fecha": fecha}

    @task
    def extraer_bronze(info):
        log = RegistroLote("extraer_bronze", info["lote"])
        if info["modo"] == "REPROCESO":
            log.info("no se consultan los RSS")
            return leer_json(cliente_s3(), bronze.clave_resumen(info["lote"]))
        return bronze.extraer_todas(info["lote"], info["fecha"], log)

    extraer_bronze(validar_configuracion())


datalake_noticias()
