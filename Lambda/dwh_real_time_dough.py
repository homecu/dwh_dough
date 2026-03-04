import psycopg2
import psycopg2.extras
import json
import os
import base64
import boto3
from botocore.exceptions import ClientError

_secrets_client = boto3.client("secretsmanager")
_cached = None  # cache entre invocaciones (mismo warm container)

REQUIRED_KEYS = ("host", "port", "username", "password")

dynamodb = boto3.client('dynamodb')



# Lista de tablas para hacer upsert en DynamoDB
UPDYNAMO_TABLES = {
    "olbuseraccountownershiptype",
    "olbaccounttype",
    "olbuserstatus",
    "olbtransactioncategory",
    "olbuserrole",
    "olbfisubrole",
    "blossomcompany"
    }

def upsert_dynamo_record(table_name, record_json, dynamo_table_name):
    item = {}
    #  id: table_name + id
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

def upsert_blossomcompany(blossomcompany_record, conn):
    """
    Update dim_company in Aurora for the given blossomcompany record using an open connection.
    """
    with conn.cursor() as cursor:
        sql = '''
            UPDATE dim_company
            SET idblossomcompany = %s, name = %s, lastUploaded = NOW()
            WHERE idblossomcompany = %s
        '''
        cursor.execute(sql, (blossomcompany_record['id'], blossomcompany_record['name'], blossomcompany_record['id']))
    conn.commit()

def upsert_olbfinancialinstitution(record_json, conn):
    """
    Upsert logic for dim_company: UPDATE if exists, otherwise INSERT.
    """
    # Fetch blossomcompany info from DynamoDB
    blossomcompany_id = record_json.get('idblossomcompany')
    blossomcompany_name = None
    if blossomcompany_id:
        try:
            response = dynamodb.get_item(
                TableName=os.environ["TABLE_OLB_DICTIONARY_DOUGH"],
                Key={
                    'id': {'S': f"blossomcompany{blossomcompany_id}"}
                }
            )
            item = response.get('Item')
            if item:
                blossomcompany_name = item.get('name', {}).get('S')
        except Exception as e:
            print(f"Error fetching blossomcompany from DynamoDB: {e}")

    with conn.cursor() as cursor:
        # Check if record exists
        cursor.execute(
            "SELECT 1 FROM dim_company WHERE idolbfinancialinstitution = %s",
            (record_json['id'],)
        )
        exists = cursor.fetchone()
        idcompany = record_json['id']
        idclient = '1'

        if exists:
            sql = '''
                UPDATE dim_company
                SET idolbfinancialinstitution = %s, idcompany = %s, idclient = %s, idblossomcompany = %s, name = %s, lastUploaded = NOW()
                WHERE idolbfinancialinstitution = %s
            '''
            cursor.execute(sql, (record_json['id'], idcompany, idclient, blossomcompany_id, blossomcompany_name, record_json['id']))
        else:
            sql = '''
                INSERT INTO dim_company (idolbfinancialinstitution, idcompany, idclient, idblossomcompany, name, lastUploaded)
                VALUES (%s, %s, %s, %s, %s, NOW())
            '''
            cursor.execute(sql, (record_json['id'], idcompany, idclient, blossomcompany_id, blossomcompany_name))
    conn.commit()

def get_aurora_connection(aurora_config):
    """
    Returns a psycopg2 connection to Aurora PostgreSQL using the provided config dict.
    """
    hostname = aurora_config['host']
    port = int(aurora_config['port'])
    username = aurora_config['username']
    password = aurora_config['password']
    dbname = os.environ["CONSOLIDATED_DB"]
    print(f"Connecting to DB {dbname} at {hostname}:{port} with user {username}")
    return psycopg2.connect(
        host=hostname,
        port=port,
        user=username,
        password=password,
        dbname=dbname,
        cursor_factory=psycopg2.extras.RealDictCursor
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
    
    # Get Aurora config from secrets
    aurora_config = _load_db_config()
    aurora_config['user'] = aurora_config.pop('username')
    aurora_config['db'] = os.environ.get('CONSOLIDATED_DB', 'AURORA_DB')

    try:
        conn = get_aurora_connection(aurora_config)
    except Exception as conn_e:
        print(f"Error connecting to Aurora: {conn_e}")

    if 'Records' in event:
        try:
            for record in event['Records']:
                try:
                    raw = record['kinesis']['data']
                    payload = base64.b64decode(raw).decode('utf-8')
                    data = json.loads(payload)
                    table_name = data.get('metadata', {}).get('table-name', '').lower()

                    if table_name in UPDYNAMO_TABLES:
                        record_json = data['data']
                        upsert_dynamo_record(table_name, record_json, os.environ["TABLE_OLB_DICTIONARY_DOUGH"])

                    if table_name == 'blossomcompany' and conn is not None:
                        upsert_blossomcompany(record_json, conn)

                    elif table_name == 'olbfinancialinstitution' and conn is not None:
                        upsert_olbfinancialinstitution(record_json, conn)

                    print(payload)
                except Exception as e:
                    print(f"Error decoding record: {e}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception as close_e:
                    print(f"Error closing Aurora connection: {close_e}")
    else:
        print(event)
    return {"ok": True}