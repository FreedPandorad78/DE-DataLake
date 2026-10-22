#!/usr/bin/env bash
set -euo pipefail
export AWS_PAGER=""
cd "$(dirname "$0")/.."

REGION=us-east-2
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
: > recursos_datalake.env
echo "REGION=$REGION" >> recursos_datalake.env

# 1. Bucket privado (un solo bucket para bronze, silver, gold y quarantine)
BUCKET="bucket-taller-datalake-$(date +%s)"
echo "Creando bucket $BUCKET..."
aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
  --create-bucket-configuration LocationConstraint="$REGION" > /dev/null
aws s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
echo "BUCKET=$BUCKET" >> recursos_datalake.env

# 2. Rol de la EC2 con permisos mínimos
echo "Creando rol de IAM de la EC2..."
cat > /tmp/confianza_ec2.json << 'JSON'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "ec2.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
JSON
aws iam create-role --role-name taller-datalake-ec2-role \
  --assume-role-policy-document file:///tmp/confianza_ec2.json > /dev/null
echo "ROLE_NAME=taller-datalake-ec2-role" >> recursos_datalake.env

cat > /tmp/permisos_ec2.json << JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3DelDataLake",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"],
      "Resource": "arn:aws:s3:::$BUCKET/*"
    },
    {
      "Sid": "S3ListarBucket",
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": "arn:aws:s3:::$BUCKET"
    },
    {
      "Sid": "Comprehend",
      "Effect": "Allow",
      "Action": ["comprehend:DetectEntities", "comprehend:DetectKeyPhrases", "comprehend:DetectDominantLanguage"],
      "Resource": "*"
    },
    {
      "Sid": "GlueSoloLaBaseDelProyecto",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable",
        "glue:GetPartition", "glue:GetPartitions", "glue:BatchGetPartition",
        "glue:CreatePartition", "glue:BatchCreatePartition",
        "glue:UpdatePartition", "glue:BatchUpdatePartition",
        "glue:DeletePartition", "glue:BatchDeletePartition"
      ],
      "Resource": [
        "arn:aws:glue:$REGION:$ACCOUNT_ID:catalog",
        "arn:aws:glue:$REGION:$ACCOUNT_ID:database/taller_datalake",
        "arn:aws:glue:$REGION:$ACCOUNT_ID:table/taller_datalake/*"
      ]
    },
    {
      "Sid": "AthenaSoloSuGrupo",
      "Effect": "Allow",
      "Action": ["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:StopQueryExecution", "athena:GetWorkGroup"],
      "Resource": "arn:aws:athena:$REGION:$ACCOUNT_ID:workgroup/taller-datalake-wg"
    }
  ]
}
JSON
aws iam put-role-policy --role-name taller-datalake-ec2-role \
  --policy-name permisos-datalake --policy-document file:///tmp/permisos_ec2.json
aws iam create-instance-profile --instance-profile-name taller-datalake-ec2-profile > /dev/null
aws iam add-role-to-instance-profile --instance-profile-name taller-datalake-ec2-profile \
  --role-name taller-datalake-ec2-role
echo "PROFILE_NAME=taller-datalake-ec2-profile" >> recursos_datalake.env
sleep 15   # el rol tarda unos segundos en propagarse

# 3. Glue y Athena
aws glue create-database --region "$REGION" \
  --database-input Name=taller_datalake,Description="Data lake de noticias RSS" > /dev/null
echo "DATABASE=taller_datalake" >> recursos_datalake.env
aws athena create-work-group --region "$REGION" --name taller-datalake-wg \
  --configuration "ResultConfiguration={OutputLocation=s3://$BUCKET/athena-resultados/},EnforceWorkGroupConfiguration=true,BytesScannedCutoffPerQuery=104857600" > /dev/null
echo "WORKGROUP=taller-datalake-wg" >> recursos_datalake.env

# 4. Red y clave SSH
VPC_ID=$(aws ec2 describe-vpcs --region "$REGION" --filters Name=isDefault,Values=true --query "Vpcs[0].VpcId" --output text)
SUBNET_ID=$(aws ec2 describe-subnets --region "$REGION" --filters Name=vpc-id,Values="$VPC_ID" Name=default-for-az,Values=true --query "Subnets[0].SubnetId" --output text)
MI_IP=$(curl -s https://checkip.amazonaws.com)
SG_ID=$(aws ec2 create-security-group --region "$REGION" --group-name taller-datalake-ec2-sg \
  --description "SSH desde mi IP hacia la EC2 de Airflow" --vpc-id "$VPC_ID" --query GroupId --output text)
aws ec2 authorize-security-group-ingress --region "$REGION" --group-id "$SG_ID" \
  --protocol tcp --port 22 --cidr "${MI_IP}/32" > /dev/null
echo "SG_ID=$SG_ID" >> recursos_datalake.env
mkdir -p ~/.ssh
aws ec2 create-key-pair --region "$REGION" --key-name taller-datalake-key --key-type rsa \
  --query KeyMaterial --output text > ~/.ssh/taller-datalake-key.pem
chmod 400 ~/.ssh/taller-datalake-key.pem
echo "KEY_NAME=taller-datalake-key" >> recursos_datalake.env

# 5. EC2 con Docker
cat > /tmp/userdata.sh << 'SH'
#!/bin/bash
dnf install -y docker git
systemctl enable --now docker
usermod -aG docker ec2-user
mkdir -p /usr/local/lib/docker/cli-plugins
curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
SH
AMI_ID=$(aws ssm get-parameter --region "$REGION" \
  --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
  --query Parameter.Value --output text)
echo "Creando EC2 t3.medium..."
INSTANCE_ID=$(aws ec2 run-instances --region "$REGION" \
  --image-id "$AMI_ID" --instance-type t3.medium --count 1 \
  --key-name taller-datalake-key --security-group-ids "$SG_ID" --subnet-id "$SUBNET_ID" \
  --associate-public-ip-address \
  --iam-instance-profile Name=taller-datalake-ec2-profile \
  --metadata-options "HttpTokens=required,HttpPutResponseHopLimit=2,HttpEndpoint=enabled" \
  --block-device-mappings "DeviceName=/dev/xvda,Ebs={VolumeSize=30,VolumeType=gp3,DeleteOnTermination=true}" \
  --user-data file:///tmp/userdata.sh \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=Airflow-DataLake}]' \
  --query "Instances[0].InstanceId" --output text)
echo "INSTANCE_ID=$INSTANCE_ID" >> recursos_datalake.env
aws ec2 wait instance-running --region "$REGION" --instance-ids "$INSTANCE_ID"
EC2_IP=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$INSTANCE_ID" \
  --query "Reservations[0].Instances[0].PublicIpAddress" --output text)
echo "EC2_IP=$EC2_IP" >> recursos_datalake.env

echo "Infraestructura lista:"
cat recursos_datalake.env
