#!/usr/bin/env bash
cd "$(dirname "$0")/.."
source recursos_datalake.env
SSH="ssh -i $HOME/.ssh/taller-datalake-key.pem ec2-user@$EC2_IP"
AF='cd ~/DE-DataLake/infra/airflow && docker compose exec -T airflow-scheduler'
RUIDO='Warning|warnings.warn|plugins.py|RemovedInAirflow'
sec() { echo; echo "=================== $1 ==================="; }

sec "1. Bucket privado (bloqueo de acceso público)"
aws s3api get-public-access-block --bucket "$BUCKET" --query PublicAccessBlockConfiguration --output table

sec "2. Archivos de Bronze en S3"
aws s3 ls "s3://$BUCKET/bronze/" --recursive

sec "3. Resumen del lote"
RESUMEN=$(aws s3 ls "s3://$BUCKET/bronze/_runs/" --recursive | awk '{print $4}' | grep summary.json | tail -1)
aws s3 cp "s3://$BUCKET/$RESUMEN" -

sec "4. Manifiesto de cada fuente"
for M in $(aws s3 ls "s3://$BUCKET/bronze/" --recursive | awk '{print $4}' | grep manifest.json); do
  echo "--- $M"
  aws s3 cp "s3://$BUCKET/$M" -
  echo
done

sec "5. Integridad: hash del manifiesto frente al hash del archivo guardado"
for M in $(aws s3 ls "s3://$BUCKET/bronze/" --recursive | awk '{print $4}' | grep manifest.json); do
  base=${M%manifest.json}
  esperado=$(aws s3 cp "s3://$BUCKET/$M" - | python3 -c "import sys,json;print(json.load(sys.stdin)['content_sha256'])")
  real=$(aws s3 cp "s3://$BUCKET/${base}feed.xml" - | shasum -a 256 | awk '{print $1}')
  if [ "$esperado" = "$real" ]; then echo "COINCIDE   $base"; else echo "DIFERENTE  $base"; fi
done

sec "6. Contenido original conservado (primeros 900 caracteres del feed de Colombia)"
FEED=$(aws s3 ls "s3://$BUCKET/bronze/" --recursive | awk '{print $4}' | grep eltiempo_colombia | grep feed.xml | head -1)
aws s3 cp "s3://$BUCKET/$FEED" - 2>/dev/null | head -c 900
echo

sec "7. Corridas del DAG en Airflow"
$SSH "$AF airflow dags list-runs -d datalake_noticias" 2>&1 | grep -v -E "$RUIDO" | grep -E "dag_id|datalake_noticias"
RUNID=$($SSH "$AF airflow dags list-runs -d datalake_noticias" 2>/dev/null | grep datalake_noticias | head -1 | awk -F'|' '{print $2}' | tr -d ' ')
echo; echo "Estado de las tareas de la corrida $RUNID:"
$SSH "$AF airflow tasks states-for-run datalake_noticias '$RUNID'" 2>&1 | grep -v -E "$RUIDO"

sec "8. Registros con fuente, lote y tarea (todos los intentos)"
$SSH 'cd ~/DE-DataLake/infra/airflow && for f in $(find logs -path "*datalake_noticias*" -name "*.log" | sort); do echo "### $f"; grep -h "\[lote=" "$f"; done'
