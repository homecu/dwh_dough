
import sys
import logging
from typing import List
import tempfile
import datetime

import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import re



logger = logging.getLogger("create_iceberg_tables")
logger.setLevel(logging.INFO)
logging.basicConfig(
	level=logging.INFO,
	format="%(asctime)s %(levelname)s %(name)s: %(message)s",
	stream=sys.stdout,
)
if not logger.handlers:
	handler = logging.StreamHandler(sys.stdout)
	handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
	logger.addHandler(handler)

# Collect log messages for S3 upload
LOG_MESSAGES = []
def log_and_store(level, msg):
	LOG_MESSAGES.append(f"{datetime.datetime.utcnow().isoformat()} {level}: {msg}")
	getattr(logger, level.lower())(msg)

import botocore

cfg = botocore.config.Config(
    connect_timeout=5,
    read_timeout=20,
    retries={"max_attempts": 3, "mode": "standard"},
)

def get_all_tables(glue_client, database_name: str) -> List[str]:
	tables = []
	paginator = glue_client.get_paginator("get_tables")
	for page in paginator.paginate(DatabaseName=database_name):
		for t in page.get("TableList", []):
			name = t.get("Name")
			if name:
				tables.append(name)
	return tables


def table_exists_in_glue(glue_client, database: str, table: str) -> bool:
	try:
		glue_client.get_table(DatabaseName=database, Name=table)
		return True
	except glue_client.exceptions.EntityNotFoundException:
		return False


def configure_spark_for_iceberg(spark: SparkSession, warehouse_s3_path: str):
	# Configure Iceberg to use AWS Glue Catalog and S3FileIO
	spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
	spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
	spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
	spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", warehouse_s3_path)
	# Establece el catálogo por defecto para evitar prefijos en SQL
	spark.conf.set("spark.sql.defaultCatalog", "glue_catalog")
	# Nota: 'spark.sql.extensions' es una config estática en Spark/Glue.
	# No se modifica en tiempo de ejecución para evitar errores.


def main():

	log_and_store("info", "Iniciando job de creación de tablas Iceberg…")
	args = getResolvedOptions(
		sys.argv,
		[
			"TARGET_BUCKET",
			"TARGET_PREFIX",
			"GLUE_INPUT_DATABASE",
			"GLUE_OUTPUT_DATABASE",
			"JOB_NAME",
		],
	)

	target_bucket = args["TARGET_BUCKET"].rstrip("/")
	target_prefix = args["TARGET_PREFIX"].strip("/")
	input_db = args["GLUE_INPUT_DATABASE"]
	output_db = args["GLUE_OUTPUT_DATABASE"]
	sanitized_output_db = re.sub(r"[^A-Za-z0-9_]", "_", output_db)
	if sanitized_output_db != output_db:
		log_and_store("warning",
			f"El nombre de la base de datos destino '{output_db}' contiene caracteres no soportados por Iceberg. "
			f"Usando '{sanitized_output_db}' para operaciones Iceberg."
		)
	job_name = args["JOB_NAME"]
	log_and_store("info", f"DEBUG: target_bucket={target_bucket}, target_prefix={target_prefix}, job_name={job_name}")

	sc = SparkContext()
	glue_context = GlueContext(sc)
	spark = glue_context.spark_session
	spark.sparkContext.setLogLevel("WARN")
	job = Job(glue_context)
	job.init(job_name, args)

	warehouse_path = f"s3://{target_bucket}/{target_prefix}"
	configure_spark_for_iceberg(spark, warehouse_path)
	log_and_store("info", "Configuración de Iceberg aplicada. Consultando tablas de Glue…")

	glue_client = boto3.client("glue", config=cfg)
	log_and_store("info", "Glue client set")

	try:
		glue_client.get_database(Name=sanitized_output_db)
		log_and_store("info", f"Catálogo: base de datos destino '{sanitized_output_db}' ya existe")
	except glue_client.exceptions.EntityNotFoundException:
		log_and_store("info", f"Creando base de datos destino '{sanitized_output_db}' en Glue Catalog")
		glue_client.create_database(
			DatabaseInput={
				"Name": sanitized_output_db,
				"Description": "Base de datos Iceberg generada automáticamente",
				"Parameters": {"catalog": "iceberg", "warehouse": warehouse_path},
			}
		)

	tables = get_all_tables(glue_client, input_db)
	if not tables:
		log_and_store("warning", f"No se encontraron tablas en la base de datos de entrada '{input_db}'. Verifique que la base de datos existe y contiene tablas.")
		log_and_store("info", f"DEBUG: GLUE_INPUT_DATABASE '{input_db}' está vacía o no existe.")
		# Log upload will happen in finally
		return

	log_and_store("info", f"Encontradas {len(tables)} tablas en '{input_db}': {tables}")

	try:
		for tbl in tables:
			try:
				target_location = f"s3://{target_bucket}/{target_prefix}/{tbl}"
				full_table = f"{sanitized_output_db}`.`{tbl}"
				source_table = f"spark_catalog.`{input_db}`.`{tbl}`"
				if table_exists_in_glue(glue_client, sanitized_output_db, tbl):
					log_and_store("info", f"La tabla Iceberg {sanitized_output_db}.{tbl} ya existe. Realizando upsert (MERGE INTO)...")
					df = spark.table(source_table)
					columns = df.columns
					set_clause = ", ".join([f"target.{col}=source.{col}" for col in columns if col != "id"])
					insert_cols = ", ".join(columns)
					insert_vals = ", ".join([f"source.{col}" for col in columns])
					merge_sql = f"""
						MERGE INTO `{sanitized_output_db}`.`{tbl}` AS target
						USING {source_table} AS source
						ON target.id = source.id
						WHEN MATCHED THEN UPDATE SET {set_clause}
						WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
					"""
					log_and_store("info", f"Ejecutando MERGE INTO para {tbl}: {merge_sql.strip()}")
					spark.sql(merge_sql)
				else:
					log_and_store("info", f"Creando tabla Iceberg {sanitized_output_db}.{tbl} en {target_location}")
					ctas_sql = (
						f"""
						CREATE TABLE `{sanitized_output_db}`.`{tbl}`
						USING iceberg
						LOCATION '{target_location}'
						TBLPROPERTIES (
						  'format-version'='2',
						  'write.parquet.compression-codec'='zstd',
						  'identifier-fields'='id'
						)
						AS SELECT * FROM spark_catalog.`{input_db}`.`{tbl}`
						"""
					)
					log_and_store("info", f"Ejecutando CTAS SQL para {tbl}: {ctas_sql.strip()}")
					spark.sql(ctas_sql)

			except Exception as e:
				log_and_store("error", f"Error creando/actualizando tabla Iceberg para {tbl}: {e}")
	finally:
		# Always attempt log upload
		try:
			s3 = boto3.client("s3")
			timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
			log_key = f"{target_prefix}/logs/iceberg/{job_name}-{timestamp}.log"
			log_and_store("info", f"Intentando guardar log en s3://{target_bucket}/{log_key}")
			log_and_store("info", f"DEBUG: LOG_MESSAGES content before upload: {LOG_MESSAGES}")
			with tempfile.NamedTemporaryFile("w", delete=False) as tmpf:
				tmpf.write("\n".join(LOG_MESSAGES))
				tmpf.flush()
				s3.upload_file(tmpf.name, target_bucket, log_key)
			log_and_store("info", f"Log guardado en s3://{target_bucket}/{log_key}")
		except Exception as e:
			log_and_store("error", f"Error guardando log en S3: {e}")

	job.commit()


if __name__ == "__main__":
	main()