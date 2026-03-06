
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

def get_aurora_connection(aurora_config):
    """
    Returns a psycopg2 connection to Aurora PostgreSQL using the provided config dict.
    """
    hostname = aurora_config['host']
    port = int(aurora_config['port'])
    # Some secrets use 'username', others use 'user'. Try both.
    username = aurora_config.get('username') or aurora_config.get('user')
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
            "SELECT 1 FROM dim_company WHERE idolbfinancialinstitution = %s AND idclient = %s",
            (record_json['id'], '1')
        )
        exists = cursor.fetchone()
        idcompany = record_json['id']
        idclient = '1'

        if exists:
            sql = '''
                UPDATE dim_company
                SET idolbfinancialinstitution = %s, idcompany = %s, idclient = %s, idblossomcompany = %s, name = %s, lastUploaded = NOW()
                WHERE idolbfinancialinstitution = %s AND idclient = %s
            '''
            cursor.execute(sql, (record_json['id'], idcompany, idclient, blossomcompany_id, blossomcompany_name, record_json['id'], idclient))
        else:
            sql = '''
                INSERT INTO dim_company (idolbfinancialinstitution, idcompany, idclient, idblossomcompany, name, firstUploaded)
                VALUES (%s, %s, %s, %s, %s, NOW())
            '''
            cursor.execute(sql, (record_json['id'], idcompany, idclient, blossomcompany_id, blossomcompany_name))
    conn.commit()

def upsert_olbuser(record_json, conn):
    """
    Upsert logic for dim_member: UPDATE if exists, otherwise INSERT. Uses info from olbfisubrole and olbuserrole in DynamoDB.
    """
    # Fetch olbfisubrole and olbuserrole info from DynamoDB
    olbfisubrole_id = record_json.get('idolbfisubrole')
    olbfisubrole_type = None
    olbuserrole_type = None
    if olbfisubrole_id:
        try:
            fisubrole_resp = dynamodb.get_item(
                TableName=os.environ["TABLE_OLB_DICTIONARY_DOUGH"],
                Key={
                    'id': {'S': f"olbfisubrole{olbfisubrole_id}"}
                }
            )
            fisubrole_item = fisubrole_resp.get('Item')
            if fisubrole_item:
                olbfisubrole_type = fisubrole_item.get('type', {}).get('S')
                olbuserrole_id = fisubrole_item.get('idolbuserrole', {}).get('S')
                if olbuserrole_id:
                    userrole_resp = dynamodb.get_item(
                        TableName=os.environ["TABLE_OLB_DICTIONARY_DOUGH"],
                        Key={
                            'id': {'S': f"olbuserrole{olbuserrole_id}"}
                        }
                    )
                    userrole_item = userrole_resp.get('Item')
                    if userrole_item:
                        olbuserrole_type = userrole_item.get('type', {}).get('S')
        except Exception as e:
            print(f"Error fetching olbfisubrole/olbuserrole from DynamoDB: {e}")

    # Only upsert if olbuserrole_type == 'MEMBER'
    if olbuserrole_type != 'MEMBER':
        return

    with conn.cursor() as cursor:
        # Check if record exists
        cursor.execute(
            "SELECT 1 FROM dim_member WHERE idmember = %s AND idclient = %s",
            (record_json['id'], '1')
        )
        exists = cursor.fetchone()
        idmember = record_json['id']
        idcompany = record_json.get('idfi')
        idclient = '1'
        createdat = record_json.get('createdat')
        deletedat = record_json.get('deletedat')
        status = 'ACTIVE' if deletedat is None else 'INACTIVE'
        if exists:
            sql = '''
                UPDATE dim_member
                SET idcompany = %s, idclient = %s, createdat = %s, deletedat = %s, status = %s, lastUploaded = NOW()
                WHERE idmember = %s AND idclient = %s
            '''
            cursor.execute(sql, (idcompany, idclient, createdat, deletedat, status, idmember, idclient))
        else:
            sql = '''
                INSERT INTO dim_member (idmember, idcompany, idclient, createdat, deletedat, status, firstUploaded)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
            '''
            cursor.execute(sql, (idmember, idcompany, idclient, createdat, deletedat, status))
    conn.commit()


def upsert_olbuseraccount(record_json, conn):
    """
    Upsert logic for dim_useraccount: UPDATE if exists, otherwise INSERT.
    """
    # Fetch olbuseraccountownershiptype and olbuser info from DynamoDB
    idolbuseraccountownershiptype = record_json.get('idolbuseraccountownershiptype')
    ownership_type = None
    ownership_type_key = None
    if idolbuseraccountownershiptype:
        try:
            ownershiptype_resp = dynamodb.get_item(
                TableName=os.environ["TABLE_OLB_DICTIONARY_DOUGH"],
                Key={
                    'id': {'S': f"olbuseraccountownershiptype{idolbuseraccountownershiptype}"}
                }
            )
            ownershiptype_item = ownershiptype_resp.get('Item')
            if ownershiptype_item:
                ownership_type = ownershiptype_item.get('type', {}).get('S')
                ownership_type_key = ownershiptype_item.get('key', {}).get('S')
        except Exception as e:
            print(f"Error fetching olbuseraccountownershiptype from DynamoDB: {e}")

    # Only upsert if ownership_type == 'MEMBER'
    if ownership_type != 'MEMBER':
        return

    idolbuser = record_json.get('idolbuser')
    idcompany = None
    if idolbuser:
        try:
            user_resp = dynamodb.get_item(
                TableName=os.environ["TABLE_OLB_DICTIONARY_DOUGH"],
                Key={
                    'id': {'S': f"olbuser{idolbuser}"}
                }
            )
            user_item = user_resp.get('Item')
            if user_item:
                idcompany = user_item.get('idfi', {}).get('N') or user_item.get('idfi', {}).get('S')
        except Exception as e:
            print(f"Error fetching olbuser from DynamoDB: {e}")

    with conn.cursor() as cursor:
        # Check if record exists
        cursor.execute(
            "SELECT 1 FROM dim_useraccount WHERE iduseraccount = %s AND idclient = %s",
            (record_json['id'], '1')
        )
        exists = cursor.fetchone()
        iduseraccount = record_json['id']
        idmember = idolbuser
        idaccount = record_json.get('idolbaccountnumber')
        idclient = '1'
        isprimary = record_json.get('primary')
        createdat = record_json.get('createdat')
        deletedat = record_json.get('deletedat')
        status = 'ACTIVE' if deletedat is None else 'INACTIVE'
        if exists:
            sql = '''
                UPDATE dim_useraccount
                SET idmember = %s, idaccount = %s, idcompany = %s, idclient = %s, ownershiptype = %s, isprimary = %s, createdat = %s, deletedat = %s, status = %s, lastUploaded = NOW()
                WHERE iduseraccount = %s AND idclient = %s
            '''
            cursor.execute(sql, (idmember, idaccount, idcompany, idclient, ownership_type_key, isprimary, createdat, deletedat, status, iduseraccount, idclient))
        else:
            sql = '''
                INSERT INTO dim_useraccount (iduseraccount, idmember, idaccount, idcompany, idclient, ownershiptype, isprimary, createdat, deletedat, status, firstUploaded)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            '''
            cursor.execute(sql, (iduseraccount, idmember, idaccount, idcompany, idclient, ownership_type_key, isprimary, createdat, deletedat, status))
    conn.commit()

def upsert_olbaccountnumber(record_json, conn):
    """
    Upsert logic for dim_account: UPDATE if exists, otherwise INSERT.
    """
    with conn.cursor() as cursor:
        # Build fields as per dim_account logic
        idaccount = f"INT{record_json['id']}"
        idcompany = record_json.get('idfi')
        idclient = '1'
        internalexternal = 'Internal'
        # Check if record exists
        cursor.execute(
            "SELECT 1 FROM dim_account WHERE idaccount = %s AND idclient = %s",
            (idaccount, '1')
        )
        exists = cursor.fetchone()
        if exists:
            sql = '''
                UPDATE dim_account
                SET idcompany = %s, idclient = %s, internalexternal = %s, lastUploaded = NOW()
                WHERE idaccount = %s AND idclient = %s
            '''
            cursor.execute(sql, (idcompany, idclient, internalexternal, idaccount, idclient))
        else:
            sql = '''
                INSERT INTO dim_account (idaccount, idcompany, idclient, internalexternal, firstUploaded)
                VALUES (%s, %s, %s, %s, NOW())
            '''
            cursor.execute(sql, (idaccount, idcompany, idclient, internalexternal))
    conn.commit()

def upsert_olbsubaccount(record_json, conn):
    """
    Upsert logic for dim_subaccount: UPDATE if exists, otherwise INSERT.
    """
    # Build fields as per dim_subaccount logic
    idsubaccount = f"SUB{record_json['id']}"
    idaccount = f"INT{record_json.get('idolbaccountnumber')}"
    idclient = '1'
    createdat = record_json.get('createdat')
    deletedat = record_json.get('deletedat')
    status = 'ACTIVE' if deletedat is None else 'INACTIVE'
    defaulttype = ''
    doughtype = ''
    defaultassetliability = 'asset'
    doughassetliability = ''
    currentbalance = record_json.get('amount')
    internalorexternal = 'Internal'
    # Fetch idcompany from dim_account in Aurora
    idcompany = None
    companyname = None

    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT idcompany FROM dim_account WHERE idaccount = %s",
            (idaccount,)
        )
        row = cursor.fetchone()
        if row:
            idcompany = row['idcompany']

    # Fetch olbaccounttype for defaulttype
    idolbaccounttype = record_json.get('idolbaccounttype')
    if idolbaccounttype:
        try:
            at_resp = dynamodb.get_item(
                TableName=os.environ["TABLE_OLB_DICTIONARY_DOUGH"],
                Key={'id': {'S': f"olbaccounttype{idolbaccounttype}"}}
            )
            at_item = at_resp.get('Item')
            if at_item:
                defaulttype = at_item.get('value', {}).get('S')
        except Exception as e:
            print(f"Error fetching olbaccounttype for subaccount: {e}")

    with conn.cursor() as cursor:
        # Check if record exists
        cursor.execute(
            "SELECT 1 FROM dim_subaccount WHERE idsubaccount = %s AND idclient = %s",
            (idsubaccount, '1')
        )
        exists = cursor.fetchone()
        if exists:
            sql = '''
                UPDATE dim_subaccount
                SET idaccount = %s, idcompany = %s, idclient = %s, companyname = %s, internalorexternal = %s, status = %s, defaulttype = %s, doughtype = %s, defaultassetliability = %s, doughassetliability = %s, currentbalance = %s, createdat = %s, deletedat = %s, lastUploaded = NOW()
                WHERE idsubaccount = %s AND idclient = %s
            '''
            cursor.execute(sql, (idaccount, idcompany, idclient, companyname, internalorexternal, status, defaulttype, doughtype, defaultassetliability, doughassetliability, currentbalance, createdat, deletedat, idsubaccount, idclient))
        else:
            sql = '''
                INSERT INTO dim_subaccount (idsubaccount, idaccount, idcompany, idclient, companyname, internalorexternal, status, defaulttype, doughtype, defaultassetliability, doughassetliability, currentbalance, createdat, deletedat, firstUploaded)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            '''
            cursor.execute(sql, (idsubaccount, idaccount, idcompany, idclient, companyname, internalorexternal, status, defaulttype, doughtype, defaultassetliability, doughassetliability, currentbalance, createdat, deletedat))
    conn.commit()


def upsert_olbloan(record_json, conn):
    """
    Upsert logic for dim_subaccount: UPDATE if exists, otherwise INSERT.
    """
    # Build fields as per dim_subaccount logic
    idsubaccount = f"LOAN{record_json['id']}"
    idaccount = f"INT{record_json.get('idolbaccountnumber')}"
    idclient = '1'
    createdat = record_json.get('createdat')
    deletedat = record_json.get('deletedat')
    status = 'ACTIVE' if deletedat is None else 'INACTIVE'
    defaulttype = ''
    doughtype = ''
    defaultassetliability = 'liability'
    doughassetliability = ''
    currentbalance = record_json.get('currentBalance')
    internalorexternal = 'Internal'
    # Fetch idcompany from dim_account in Aurora
    idcompany = None
    companyname = None

    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT idcompany FROM dim_account WHERE idaccount = %s",
            (idaccount,)
        )
        row = cursor.fetchone()
        if row:
            idcompany = row['idcompany']
            
    # Fetch olbaccounttype for defaulttype
    idolbaccounttype = record_json.get('idolbaccounttype')
    if idolbaccounttype:
        try:
            at_resp = dynamodb.get_item(
                TableName=os.environ["TABLE_OLB_DICTIONARY_DOUGH"],
                Key={'id': {'S': f"olbaccounttype{idolbaccounttype}"}}
            )
            at_item = at_resp.get('Item')
            if at_item:
                defaulttype = at_item.get('value', {}).get('S')
        except Exception as e:
            print(f"Error fetching olbaccounttype for subaccount: {e}")

    with conn.cursor() as cursor:
        # Check if record exists
        cursor.execute(
            "SELECT 1 FROM dim_subaccount WHERE idsubaccount = %s AND idclient = %s",
            (idsubaccount, '1')
        )
        exists = cursor.fetchone()
        if exists:
            sql = '''
                UPDATE dim_subaccount
                SET idaccount = %s, idcompany = %s, idclient = %s, companyname = %s, internalorexternal = %s, status = %s, defaulttype = %s, doughtype = %s, defaultassetliability = %s, doughassetliability = %s, currentbalance = %s, createdat = %s, deletedat = %s, lastUploaded = NOW()
                WHERE idsubaccount = %s AND idclient = %s
            '''
            cursor.execute(sql, (idaccount, idcompany, idclient, companyname, internalorexternal, status, defaulttype, doughtype, defaultassetliability, doughassetliability, currentbalance, createdat, deletedat, idsubaccount, idclient))
        else:
            sql = '''
                INSERT INTO dim_subaccount (idsubaccount, idaccount, idcompany, idclient, companyname, internalorexternal, status, defaulttype, doughtype, defaultassetliability, doughassetliability, currentbalance, createdat, deletedat, firstUploaded)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            '''
            cursor.execute(sql, (idsubaccount, idaccount, idcompany, idclient, companyname, internalorexternal, status, defaulttype, doughtype, defaultassetliability, doughassetliability, currentbalance, createdat, deletedat))
    conn.commit()

    # Upsert into fact_balance_subaccount
    idsubaccount = f"LOAN{record_json['id']}"
    idaccount = f"INT{record_json.get('idolbaccountnumber')}"
    idclient = '1'
    # idcompany already fetched above
    currentbalance = record_json.get('currentBalance')
    updatedat = record_json.get('updatedAt')
    date = None
    if updatedat:
        # Extract only the date part (YYYY-MM-DD)
        date = updatedat.split('T')[0] if 'T' in updatedat else updatedat[:10]
    else:
        date = None
    with conn.cursor() as cursor:
        # Check if record exists
        cursor.execute(
            "SELECT 1 FROM fact_balance_subaccount WHERE idSubAccount = %s AND idClient = %s AND date = %s",
            (idsubaccount, idclient, date)
        )
        exists = cursor.fetchone()
        if exists:
            sql = '''
                UPDATE fact_balance_subaccount
                SET idAccount = %s, idCompany = %s, currentBalance = %s, lastUploaded = NOW()
                WHERE idSubAccount = %s AND idClient = %s AND date = %s
            '''
            cursor.execute(sql, (idaccount, idcompany, currentbalance, idsubaccount, idclient, date))
        else:
            sql = '''
                INSERT INTO fact_balance_subaccount (idSubAccount, idAccount, idClient, idCompany, date, currentBalance, firstUploaded)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
            '''
            cursor.execute(sql, (idsubaccount, idaccount, idclient, idcompany, date, currentbalance))
    conn.commit()

def lambda_handler(event, context):
    
    # Get Aurora config from secrets
    aurora_config = _load_db_config()
    print(f"aurora_config: {aurora_config}")

    # If 'username' exists, also set 'user' for compatibility
    if 'username' in aurora_config:
        aurora_config['user'] = aurora_config['username']
    aurora_config['db'] = os.environ.get('CONSOLIDATED_DB', 'AURORA_DB')

    conn = None
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

                    elif table_name == 'olbuser' and conn is not None:
                        upsert_olbuser(record_json, conn)

                    elif table_name == 'olbuseraccount' and conn is not None:
                        upsert_olbuseraccount(record_json, conn)

                    elif table_name == 'olbaccountnumber' and conn is not None:
                        upsert_olbaccountnumber(record_json, conn)

                    elif table_name == 'olb_subaccount' and conn is not None:
                        upsert_olbsubaccount(record_json, conn)

                    elif table_name == 'olbloan' and conn is not None:
                        upsert_olbloan(record_json, conn)

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