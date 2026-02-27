import json
import os
import base64
import boto3
from botocore.exceptions import ClientError

_secrets_client = boto3.client("secretsmanager")
_cached = None  # cache entre invocaciones (mismo warm container)

REQUIRED_KEYS = ("host", "port", "username", "password")

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

    # Ejemplo: usarlo para conectar (sin exponer credenciales en logs)
    hostname = cfg["host"]
    port = int(cfg["port"])
    username = cfg["username"]
    password = cfg["password"]
    db = os.environ["CONSOLIDATED_DB"]

    print(f"Connecting to DB at {hostname}:{port} with user {username}")

    return {"ok": True}