import hashlib
import xml.etree.ElementTree as ET

import requests

from .common import ahora_utc, bucket, cargar_fuentes, cliente_s3, escribir_json, existe, leer_json

USER_AGENT = "Mozilla/5.0 (compatible; DataLakeLab/1.0)"
TIMEOUT = (5, 20)  # segundos: conexión y lectura
ESTADOS_VALIDOS = ("OK", "FEED_SIN_NOTICIAS")


def prefijo_fuente(source_id, fecha, lote):
    return f"bronze/source={source_id}/ingestion_date={fecha}/run_id={lote}/"


def clave_resumen(lote):
    return f"bronze/_runs/run_id={lote}/summary.json"


def _consultar_feed(url):
    """Devuelve (http_status, contenido, estado, n_items, error)."""
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
    except requests.RequestException as e:
        return None, b"", "ERROR_DE_RED", 0, str(e)[:300]
    if r.status_code != 200:
        return r.status_code, b"", "ERROR_HTTP", 0, f"HTTP {r.status_code}"
    if not r.content.strip():
        return 200, b"", "RESPUESTA_VACIA", 0, "cuerpo vacío"
    try:
        raiz = ET.fromstring(r.content)
    except ET.ParseError as e:
        return 200, r.content, "XML_MALFORMADO", 0, str(e)[:300]
    n = len(raiz.findall(".//item"))
    return 200, r.content, ("OK" if n else "FEED_SIN_NOTICIAS"), n, None


def ingerir_fuente(cli, fuente, lote, fecha, log):
    sid = fuente["source_id"]
    base = prefijo_fuente(sid, fecha, lote)
    clave_manifiesto = base + "manifest.json"

    previo = leer_json(cli, clave_manifiesto)
    if previo and previo["status"] in ESTADOS_VALIDOS:
        log.info("ya estaba ingerida en este lote; no se vuelve a consultar", sid)
        previo["reutilizado"] = True
        return previo

    status, contenido, estado, n_items, error = _consultar_feed(fuente["url"])
    manifiesto = {
        "run_id": lote, "source_id": sid, "medio": fuente["medio"], "seccion": fuente["seccion"],
        "url": fuente["url"], "ingestion_ts_utc": ahora_utc().isoformat(timespec="seconds"),
        "http_status": status,
        "content_sha256": hashlib.sha256(contenido).hexdigest() if contenido else None,
        "content_length": len(contenido), "n_items": n_items, "status": estado, "error": error,
        "bronze_key": None, "reutilizado": False,
    }
    if estado in ESTADOS_VALIDOS:
        clave_xml = base + "feed.xml"
        if existe(cli, clave_xml):
            log.warning("feed.xml ya existía; no se sobrescribe", sid)
        else:
            cli.put_object(Bucket=bucket(), Key=clave_xml, Body=contenido, ContentType="application/xml")
        manifiesto["bronze_key"] = clave_xml
        log.info(f"ingerida: {n_items} noticias, sha256={manifiesto['content_sha256'][:12]}", sid)
    else:
        log.error(f"no se pudo ingerir: {estado} ({error})", sid)
    escribir_json(cli, clave_manifiesto, manifiesto)
    return manifiesto


def estado_del_lote(manifiestos):
    validos = [m for m in manifiestos if m["status"] in ESTADOS_VALIDOS]
    if len(validos) == len(manifiestos):
        return "COMPLETA"
    return "FALLIDA" if not validos else "PARCIAL"


def extraer_todas(lote, fecha, log):
    cli = cliente_s3()
    manifiestos = [ingerir_fuente(cli, f, lote, fecha, log) for f in cargar_fuentes()]
    resumen = {
        "run_id": lote, "ingestion_date": fecha, "estado": estado_del_lote(manifiestos),
        "fuentes": [
            {"source_id": m["source_id"], "status": m["status"], "n_items": m["n_items"], "bronze_key": m["bronze_key"]}
            for m in manifiestos
        ],
        "total_noticias_recibidas": sum(m["n_items"] for m in manifiestos if m["status"] in ESTADOS_VALIDOS),
        "actualizado_utc": ahora_utc().isoformat(timespec="seconds"),
    }
    escribir_json(cli, clave_resumen(lote), resumen)
    log.info(f"lote {resumen['estado']}: {resumen['total_noticias_recibidas']} noticias recibidas")
    if resumen["estado"] == "FALLIDA":
        raise RuntimeError("Ninguna fuente respondió con un feed válido")
    return resumen
