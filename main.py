import json
import boto3
from datetime import datetime
import yaml
from src.domain.transform_data import FactoryAPIs


# Carrega configuração
with open("api_endpoints.yaml", "r") as f:
    endpoints = yaml.safe_load(f)

s3 = boto3.client('s3')
BUCKET = "meu-bucket-dados"

def lambda_handler(event, context):
    api_name = event.get("api_name")
    params = event.get("params", {})
    
    data = FactoryAPIs.execute_api(api_name, params)
    
    # Salva no S3 com estrutura baseada na API
    key = f"{api_name}/{datetime.now().strftime('%Y-%m-%d')}.json"
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(data))
    
    return {"status": "ok", "api": api_name, "records": len(data)}
