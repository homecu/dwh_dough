
def print_db_connection_info(cfg):
    hostname = cfg["host"]
    port = int(cfg["port"])
    username = cfg["username"]
    password = cfg["password"]
    db = os.environ["CONSOLIDATED_DB"]
    print(f"Connecting to DB at {hostname}:{port} with user {username}")

import json
import os
import base64
import boto3
from botocore.exceptions import ClientError



_secrets_client = boto3.client("secretsmanager")
_cached = None  # cache entre invocaciones (mismo warm container)

REQUIRED_KEYS = ("host", "port", "username", "password")

dynamodb = boto3.client('dynamodb')
DYNAMO_DICT_NAME = 'dwh_olb_dictionary_dough_dev'

# List of tables to upsert in DynamoDB
UPDYNAMO_TABLES = {
    "olbuseraccountownershiptype",
    "olbaccounttype",
    "olbuserstatus",
    "olbtransactioncategory",
    "olbuserrole",
    "olbfisubrole"
}

def upsert_dynamo_record(table_name, record_json, dynamo_table_name):
    item = {}
    # The id field: table_name + id
    item['id'] = {'S': f"{table_name}{record_json['id']}"}
    for k, v in record_json.items():
        if k == 'id':
            item['source_id'] = {'S': str(v)}
        elif isinstance(v, str):
            item[k] = {'S': v}
        elif isinstance(v, (int, float)):
            item[k] = {'N': str(v)}
        elif v is None:
            continue
        else:
            item[k] = {'S': json.dumps(v)}
    item['source_table'] = {'S': table_name}
    dynamodb.put_item(
        TableName=dynamo_table_name,
        Item=item
    )



def _load_db_config():
    global _cached
    if _cached is not None:
        return _cached

    secret_id = os.environ["CONSOLIDATED_SECRETS"]  # o SECRET_ARN_DOUGH si usas ARN

    try:
        resp = _secrets_client.get_secret_value(SecretId=secret_id)
    except ClientError as e:
        # Log y re-raise (no incluyas el secreto en logs)
        raise RuntimeError(f"Failed to get secret '{secret_id}': {e}") from e

    if "SecretString" in resp and resp["SecretString"]:
        payload = resp["SecretString"]
    else:
        # Si guardaste el secreto como binario
        payload = base64.b64decode(resp["SecretBinary"]).decode("utf-8")

    data = json.loads(payload)  # requiere que el secreto sea JSON

    missing = [k for k in REQUIRED_KEYS if k not in data or data[k] in (None, "")]
    if missing:
        raise ValueError(f"Secret is missing required keys: {missing}")

    # Quedarte SOLO con lo que necesitas
    _cached = {k: str(data[k]) for k in REQUIRED_KEYS}
    return _cached


def handler(event, context):
    cfg = _load_db_config()

    print_db_connection_info(cfg)

    # DynamoDB upsert for specified tables
    if 'records' in event:
        for record in event['records']:
            try:
                payload = base64.b64decode(record['data']).decode('utf-8')
                try:
                    data = json.loads(payload)
                except json.JSONDecodeError:
                    data = payload

                if isinstance(data, dict) and 'data' in data and 'metadata' in data:
                    table_name = data['metadata'].get('table-name', '').lower()
                    if table_name in UPDYNAMO_TABLES:
                        record_json = data['data']
                        # Replace with your DynamoDB table name
                        dynamo_table_name = DYNAMO_DICT_NAME
                        upsert_dynamo_record(table_name, record_json, dynamo_table_name)
            except Exception as e:
                print(f"DynamoDB upsert error: {e}")



    return {"ok": True}