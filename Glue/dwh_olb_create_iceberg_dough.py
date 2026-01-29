import sys
import logging
from typing import List

import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import re


logger = logging.getLogger("create_iceberg_tables")
logger.setLevel(logging.INFO)
# Asegura que los logs vayan a stdout con formato y nivel INFO
logging.basicConfig(
	level=logging.INFO,
	format="%(asctime)s %(levelname)s %(name)s: %(message)s",
	stream=sys.stdout,
)
if not logger.handlers:
	handler = logging.StreamHandler(sys.stdout)
	handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
	logger.addHandler(handler)

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
	logger.info("Iniciando job de creación de tablas Iceberg…")
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
	# Sanitiza el nombre de la base de datos de salida para Iceberg (evita guiones)
	sanitized_output_db = re.sub(r"[^A-Za-z0-9_]", "_", output_db)
	if sanitized_output_db != output_db:
		logger.warning(
			f"El nombre de la base de datos destino '{output_db}' contiene caracteres no soportados por Iceberg. "
			f"Usando '{sanitized_output_db}' para operaciones Iceberg."
		)
	job_name = args["JOB_NAME"]

	sc = SparkContext()
	glue_context = GlueContext(sc)
	spark = glue_context.spark_session
	# Reduce ruido de logs Spark
	spark.sparkContext.setLogLevel("WARN")
	job = Job(glue_context)
	job.init(job_name, args)

	warehouse_path = f"s3://{target_bucket}/{target_prefix}"
	configure_spark_for_iceberg(spark, warehouse_path)
	logger.info("Configuración de Iceberg aplicada. Consultando tablas de Glue…")


	glue_client = boto3.client("glue", config=cfg)
	
	logger.info("Glue client set")
	# Ensure sanitized output database exists in Glue Catalog
	try:
		glue_client.get_database(Name=sanitized_output_db)
		logger.info(f"Catálogo: base de datos destino '{sanitized_output_db}' ya existe")
	except glue_client.exceptions.EntityNotFoundException:
		logger.info(f"Creando base de datos destino '{sanitized_output_db}' en Glue Catalog")
		glue_client.create_database(
			DatabaseInput={
				"Name": sanitized_output_db,
				"Description": "Base de datos Iceberg generada automáticamente",
				"Parameters": {"catalog": "iceberg", "warehouse": warehouse_path},
			}
		)

	# List source tables from Glue
	tables = get_all_tables(glue_client, input_db)
	if not tables:
		logger.warning(f"No se encontraron tablas en la base de datos de entrada '{input_db}'")
		job.commit()
		return

	logger.info(f"Encontradas {len(tables)} tablas en '{input_db}': {tables}")

	# Create each Iceberg table via CTAS from the source table
	for tbl in tables:
		try:
			target_location = f"s3://{target_bucket}/{target_prefix}/{tbl}"
			if table_exists_in_glue(glue_client, sanitized_output_db, tbl):
				logger.info(f"La tabla Iceberg {sanitized_output_db}.{tbl} ya existe. Omitiendo creación.")
				continue

			logger.info(f"Creando tabla Iceberg {sanitized_output_db}.{tbl} en {target_location}")

			# Create unpartitioned Iceberg table with format-version=2 and CTAS from source
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
			logger.info(f"Ejecutando CTAS SQL para {tbl}: {ctas_sql.strip()}")
			spark.sql(ctas_sql)

			# Los identifier fields se establecen vía TBLPROPERTIES en CTAS

		except Exception as e:
			logger.error(f"Error creando tabla Iceberg para {tbl}: {e}")

	job.commit()


if __name__ == "__main__":
	main()