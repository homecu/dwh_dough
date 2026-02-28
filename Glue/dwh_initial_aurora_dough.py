import sys
import logging
import boto3
from datetime import datetime, timezone
from io import StringIO
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from py4j.java_gateway import java_import
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- Logging setup ---
log_stream = StringIO()
handler = logging.StreamHandler(log_stream)
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger = logging.getLogger("glue_create_table")
logger.setLevel(logging.INFO)
logger.addHandler(handler)

S3_LOG_PREFIX = "logs/glue_create_table"

# --- Glue job init ---
args = getResolvedOptions(sys.argv, ["JOB_NAME", "connection_name", "dynamo_dict_name", "GLUE_INPUT_DATABASE", "TARGET_BUCKET"],)

def upload_log():
    s3 = boto3.client("s3")
    log_bucket = args.get("TARGET_BUCKET", "blossom-analytics-dwh-dough-dev") if 'args' in globals() else "blossom-analytics-dwh-dough-dev"
    log_key = f"{S3_LOG_PREFIX}/{datetime.now(timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')}.log"
    s3.put_object(Bucket=log_bucket, Key=log_key, Body=log_stream.getvalue())
    print(f"Log uploaded to s3://{log_bucket}/{log_key}")
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

try:
    logger.info("Job started")

    # Get the JDBC connection properties from the Glue connection configured in job settings.
    connection_name = args.get('connection_name','')
    connection_props = glueContext.extract_jdbc_conf(connection_name)

    jdbc_url = connection_props["fullUrl"]
    user = connection_props["user"]
    password = connection_props["password"]

    # Create tables using a direct JDBC connection
    driver = "org.postgresql.Driver"

    initial_ext_catalog = args["GLUE_INPUT_DATABASE"]
    jdbc_properties = {"user": user, "password": password, "driver": driver}

    # Log connection info without exposing full URL
    logger.info(f"External catalog: {initial_ext_catalog}")
    logger.info("JDBC connection established (URL redacted for security)")

    # --- Catalog table cache to avoid repeated reads ---
    _catalog_cache = {}

    def catalog_table(table_name):
        if table_name not in _catalog_cache:
            logger.info(f"Reading catalog table: {table_name}")
            df = glueContext.create_dynamic_frame.from_catalog(
                database=initial_ext_catalog, table_name=table_name
            ).toDF()
            logger.info(f"Schema for {table_name}: {df.schema}")
            _catalog_cache[table_name] = df
        return _catalog_cache[table_name]

    # =========================================================================
    # DDL DEFINITIONS
    # =========================================================================

    create_dim_client = """
        CREATE TABLE IF NOT EXISTS public.dim_client (
            idClient SERIAL PRIMARY KEY,
            name VARCHAR(255),
            period TEXT,
            config JSONB,
            firstUploaded TIMESTAMP,
            lastUploaded TIMESTAMP
        )
    """

    create_dim_company = """
        CREATE TABLE IF NOT EXISTS public.dim_company (
            idCompany TEXT,
            idClient  INT NOT NULL,
            name TEXT,
            config JSONB,
            firstUploaded TIMESTAMP,
            lastUploaded TIMESTAMP,
            PRIMARY KEY (idCompany, idClient),
            FOREIGN KEY (idClient) REFERENCES public.dim_client(idClient)
        )
    """

    create_dim_member = """
        CREATE TABLE IF NOT EXISTS public.dim_member (
            idMember TEXT,
            idClient INT NOT NULL,
            idCompany TEXT NOT NULL,
            status TEXT,
            createdAt TIMESTAMP,
            deletedAt TIMESTAMP,
            firstUploaded TIMESTAMP,
            lastUploaded TIMESTAMP,
            PRIMARY KEY (idMember, idClient, idCompany),
            FOREIGN KEY (idClient) REFERENCES public.dim_client(idClient),
            FOREIGN KEY (idCompany, idClient) REFERENCES public.dim_company(idCompany, idClient)
        )
    """

    create_dim_account = """
        CREATE TABLE IF NOT EXISTS public.dim_account (
            idAccount TEXT,
            idCompany TEXT,
            idClient INT NOT NULL,
            internalExternal TEXT,
            firstUploaded TIMESTAMP,
            lastUploaded TIMESTAMP,
            PRIMARY KEY (idAccount, idCompany, idClient),
            FOREIGN KEY (idClient) REFERENCES public.dim_client(idClient)
        )
    """

    create_dim_subaccount = """
        CREATE TABLE IF NOT EXISTS public.dim_subaccount (
            idSubAccount TEXT,
            idAccount TEXT NOT NULL,
            idClient INT NOT NULL,
            idCompany TEXT NOT NULL,
            companyName TEXT,
            internalOrExternal TEXT,
            defaultType TEXT,
            doughType TEXT,
            defaultAssetLiability TEXT,
            doughAssetLiability TEXT,
            status TEXT,
            currentBalance DECIMAL(18, 2),
            availableBalance DECIMAL(18, 2),
            createdAt TIMESTAMP,
            deletedAt TIMESTAMP,
            firstUploaded TIMESTAMP,
            lastUploaded TIMESTAMP,
            PRIMARY KEY (idSubAccount, idAccount, idClient, idCompany),
            FOREIGN KEY (idClient) REFERENCES public.dim_client(idClient),
            FOREIGN KEY (idCompany, idClient) REFERENCES public.dim_company(idCompany, idClient),
            FOREIGN KEY (idAccount, idCompany, idClient) REFERENCES public.dim_account(idAccount, idCompany, idClient)
        )
    """

    create_bridge_member_account = """
        CREATE TABLE IF NOT EXISTS public.bridge_member_account (
            idMemberAccount TEXT,
            idClient INT NOT NULL,
            idCompany TEXT NOT NULL,
            idMember TEXT NOT NULL,
            idAccount TEXT NOT NULL,
            ownershipType TEXT,
            isPrimary BOOLEAN,
            status TEXT,
            createdAt TIMESTAMP,
            deletedAt TIMESTAMP,
            firstUploaded TIMESTAMP,
            lastUploaded TIMESTAMP,
            PRIMARY KEY (idMemberAccount, idClient),
            FOREIGN KEY (idClient) REFERENCES public.dim_client(idClient),
            FOREIGN KEY (idCompany, idClient) REFERENCES public.dim_company(idCompany, idClient),
            FOREIGN KEY (idMember, idClient, idCompany) REFERENCES public.dim_member(idMember, idClient, idCompany),
            FOREIGN KEY (idAccount, idCompany, idClient) REFERENCES public.dim_account(idAccount, idCompany, idClient)
        )
    """

    create_fact_balance_subaccount = """
        CREATE TABLE IF NOT EXISTS public.fact_balance_subaccount (
            idSubAccount TEXT NOT NULL,
            idAccount TEXT NOT NULL,
            idClient INT NOT NULL,
            idCompany TEXT NOT NULL,
            date DATE NOT NULL,
            currentBalance DECIMAL(18, 2),
            availableBalance DECIMAL(18, 2),
            firstUploaded TIMESTAMP,
            lastUploaded TIMESTAMP,
            PRIMARY KEY (idSubAccount, idAccount, idClient, idCompany, date),
            FOREIGN KEY (idClient) REFERENCES public.dim_client(idClient),
            FOREIGN KEY (idCompany, idClient) REFERENCES public.dim_company(idCompany, idClient)
        )
    """

    create_fact_transactions = """
        CREATE TABLE IF NOT EXISTS public.fact_transactions (
            idTransaction TEXT,
            idClient INT NOT NULL,
            idCompany TEXT NOT NULL,
            idSubAccount TEXT,
            idAccount TEXT,
            amount DECIMAL(18, 2),
            currency TEXT DEFAULT 'USD',
            originalAmount DECIMAL(18, 2),
            timestamp TIMESTAMP,
            idDate DATE,
            incomeExpenditure TEXT,
            status TEXT,
            type TEXT,
            balance DECIMAL(18, 2),
            enrichment JSONB,
            firstUploaded TIMESTAMP,
            lastUploaded TIMESTAMP,
            PRIMARY KEY (idTransaction, idClient),
            FOREIGN KEY (idClient) REFERENCES public.dim_client(idClient),
            FOREIGN KEY (idCompany, idClient) REFERENCES public.dim_company(idCompany, idClient)
        )
    """

    create_dim_calendar = """
        CREATE TABLE IF NOT EXISTS public.dim_calendar (
            id        INT PRIMARY KEY,
            full_date DATE NOT NULL UNIQUE,
            day       SMALLINT NOT NULL,
            month     SMALLINT NOT NULL,
            year      SMALLINT NOT NULL
        )
    """

    delete_dim_calendar = """
        DELETE FROM public.dim_calendar
    """

    populate_dim_calendar = """
        INSERT INTO public.dim_calendar (id, full_date, day, month, year)
        SELECT
            (EXTRACT(YEAR  FROM d)::int * 10000
           + EXTRACT(MONTH FROM d)::int * 100
           + EXTRACT(DAY   FROM d)::int) AS id,
            d::date AS full_date,
            EXTRACT(DAY   FROM d)::smallint AS day,
            EXTRACT(MONTH FROM d)::smallint AS month,
            EXTRACT(YEAR  FROM d)::smallint AS year
        FROM generate_series(
            (CURRENT_DATE - INTERVAL '2 years')::date,
            (CURRENT_DATE + INTERVAL '2 years')::date,
            INTERVAL '1 day'
        ) AS d
    """

    delete_dim_client = """
        DELETE FROM public.dim_client
    """

    populate_dim_client = """
        INSERT INTO public.dim_client (idclient, name, firstUploaded)
        VALUES (1, 'Blossom', NOW())
        ON CONFLICT (idclient) DO NOTHING
    """

    # Drop statements in reverse dependency order
    drop_fact_transactions = "DROP TABLE IF EXISTS public.fact_transactions CASCADE"
    drop_fact_subaccount_legacy = "DROP TABLE IF EXISTS public.fact_subaccount CASCADE"
    drop_fact_balance_subaccount = "DROP TABLE IF EXISTS public.fact_balance_subaccount CASCADE"
    drop_bridge_member_account = "DROP TABLE IF EXISTS public.bridge_member_account CASCADE"
    drop_dim_subaccount = "DROP TABLE IF EXISTS public.dim_subaccount CASCADE"
    drop_dim_account = "DROP TABLE IF EXISTS public.dim_account CASCADE"
    drop_dim_member = "DROP TABLE IF EXISTS public.dim_member CASCADE"
    drop_dim_company = "DROP TABLE IF EXISTS public.dim_company CASCADE"
    drop_dim_calendar = "DROP TABLE IF EXISTS public.dim_calendar CASCADE"
    drop_dim_client = "DROP TABLE IF EXISTS public.dim_client CASCADE"

    # =========================================================================
    # EXECUTE DDL/DML VIA JDBC WITH TRANSACTION SAFETY
    # =========================================================================

    java_import(sc._jvm, "java.sql.*")

    conn = sc._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
    conn.setAutoCommit(False)
    stmt = conn.createStatement()

    # Ordered queries: drops (reverse dependency), creates (forward dependency),
    # then seed data. bridge_member_account is created AFTER dim_account.
    queries = [
        # --- Drops (reverse dependency order) ---
        ("drop_fact_transactions", drop_fact_transactions),
        ("drop_fact_subaccount_legacy", drop_fact_subaccount_legacy),
        ("drop_fact_balance_subaccount", drop_fact_balance_subaccount),
        ("drop_bridge_member_account", drop_bridge_member_account),
        ("drop_dim_subaccount", drop_dim_subaccount),
        ("drop_dim_account", drop_dim_account),
        ("drop_dim_member", drop_dim_member),
        ("drop_dim_company", drop_dim_company),
        ("drop_dim_calendar", drop_dim_calendar),
        ("drop_dim_client", drop_dim_client),
        # --- Creates (forward dependency order) ---
        ("create_dim_client", create_dim_client),
        ("create_dim_company", create_dim_company),
        ("create_dim_member", create_dim_member),
        ("create_dim_account", create_dim_account),
        ("create_dim_subaccount", create_dim_subaccount),
        ("create_bridge_member_account", create_bridge_member_account),
        ("create_fact_balance_subaccount", create_fact_balance_subaccount),
        ("create_fact_transactions", create_fact_transactions),
        # --- Seed data ---
        ("create_dim_calendar", create_dim_calendar),
        ("populate_dim_calendar", populate_dim_calendar),
        ("populate_dim_client", populate_dim_client),
    ]

    try:
        for query_name, query in queries:
            logger.info(f"Executing query: {query_name}")
            stmt.execute(query)
            logger.info(f"Completed query: {query_name}")

        conn.commit()
        logger.info("All static queries executed and committed.")
    except Exception as ddl_err:
        conn.rollback()
        logger.error(f"DDL execution failed, rolled back: {ddl_err}")
        raise
    finally:
        stmt.close()
        conn.close()
        logger.info("JDBC connection closed.")

    # =========================================================================
    # CATALOG-BASED POPULATES (Spark DataFrames → JDBC append)
    # =========================================================================

    job_run_ts = F.current_timestamp()

    df_company = (
        catalog_table("olbfinancialinstitution").alias("ofi")
        .join(catalog_table("blossomcompany").alias("bc"), F.col("ofi.idblossomcompany") == F.col("bc.id"), "left")
        .select(
            F.col("ofi.id").cast("string").alias("idCompany"),
            F.lit(1).alias("idClient"),
            F.col("bc.name").alias("name"),
            job_run_ts.alias("firstUploaded"),
            F.lit(None).cast("timestamp").alias("lastUploaded"),
        )
    )

    df_member = (
        catalog_table("olbuser").alias("ou")
        .join(catalog_table("olbfisubrole").alias("ofsr"), F.col("ou.idolbfisubrole") == F.col("ofsr.id"), "left")
        .join(catalog_table("olbuserrole").alias("our"), F.col("ofsr.idolbuserrole") == F.col("our.id"), "left")
        .filter(F.col("our.type") == "MEMBER")
        .select(
            F.col("ou.id").cast("string").alias("idMember"),
            F.lit(1).alias("idClient"),
            F.col("ou.idfi").cast("string").alias("idCompany"),
            F.when(F.col("ou.deletedat").isNull(), "ACTIVE").otherwise("INACTIVE").alias("status"),
            F.col("ou.createdat").cast("timestamp").alias("createdAt"),
            F.col("ou.deletedat").cast("timestamp").alias("deletedAt"),
            job_run_ts.alias("firstUploaded"),
            F.lit(None).cast("timestamp").alias("lastUploaded"),
        )
    )

    # dim_account: directly from OLBAccountNumber (idFi is on the table itself)
    df_account = (
        catalog_table("olbaccountnumber").alias("oan")
        .select(
            F.concat(F.lit("INT"), F.col("oan.id").cast("string")).alias("idAccount"),
            F.col("oan.idfi").cast("string").alias("idCompany"),
            F.lit(1).alias("idClient"),
            F.lit("Internal").alias("internalExternal"),
            job_run_ts.alias("firstUploaded"),
            F.lit(None).cast("timestamp").alias("lastUploaded"),
        )
    )

    # bridge_member_account: links members to accounts with ownership and primary flag
    # Filter: only MEMBER ownership type
    df_bridge_member_account = (
        catalog_table("olbuseraccount").alias("oua")
        .join(catalog_table("olbuseraccountownershiptype").alias("oot"),
            F.col("oua.idolbuseraccountownershiptype") == F.col("oot.id"), "left")
        .join(catalog_table("olbuser").alias("ou"), F.col("oua.idolbuser") == F.col("ou.id"), "left")
        .filter(F.col("oot.type") == "MEMBER")
        .select(
            F.col("oua.id").cast("string").alias("idMemberAccount"),
            F.lit(1).alias("idClient"),
            F.col("ou.idfi").cast("string").alias("idCompany"),
            F.col("oua.idolbuser").cast("string").alias("idMember"),
            F.concat(F.lit("INT"), F.col("oua.idolbaccountnumber").cast("string")).alias("idAccount"),
            F.col("oot.key").alias("ownershipType"),
            F.col("oua.primary").cast("boolean").alias("isPrimary"),
            F.when(F.col("oua.deletedat").isNull(), "ACTIVE").otherwise("INACTIVE").alias("status"),
            F.col("oua.createdat").cast("timestamp").alias("createdAt"),
            F.col("oua.deletedat").cast("timestamp").alias("deletedAt"),
            job_run_ts.alias("firstUploaded"),
            F.lit(None).cast("timestamp").alias("lastUploaded"),
        )
    )

    # Keep only bridge rows whose member exists in dim_member (role-filtered)
    df_bridge_member_account = df_bridge_member_account.join(
        df_member.select("idMember", "idClient", "idCompany").distinct(),
        on=["idMember", "idClient", "idCompany"],
        how="left_semi",
    )

    # dim_subaccount: Loan path
    # Join chain: OLBLoan → OLBAccountType + OLBAccountNumber → OLBFinancialInstitution → BlossomCompany
    df_subaccount_loan = (
        catalog_table("olbloan").alias("ol")
        .join(catalog_table("olbaccounttype").alias("oat"), F.col("ol.idolbaccounttype") == F.col("oat.id"), "left")
        .join(catalog_table("olbaccountnumber").alias("oan"), F.col("ol.idolbaccountnumber") == F.col("oan.id"), "left")
        .join(catalog_table("olbfinancialinstitution").alias("ofi"), F.col("oan.idfi") == F.col("ofi.id"), "left")
        .join(catalog_table("blossomcompany").alias("bc"), F.col("ofi.idblossomcompany") == F.col("bc.id"), "left")
        .select(
            F.concat(F.lit("LOAN"), F.col("ol.id").cast("string")).alias("idSubAccount"),
            F.concat(F.lit("INT"), F.col("ol.idolbaccountnumber").cast("string")).alias("idAccount"),
            F.lit(1).alias("idClient"),
            F.col("oan.idfi").cast("string").alias("idCompany"),
            F.col("bc.name").alias("companyName"),
            F.lit("Internal").alias("internalOrExternal"),
            F.col("oat.value").alias("defaultType"),
            F.lit("").alias("doughType"),
            F.lit("liability").alias("defaultAssetLiability"),
            F.lit("").alias("doughAssetLiability"),
            F.when(F.col("ol.deletedat").isNull(), "ACTIVE").otherwise("INACTIVE").alias("status"),
            F.col("ol.currentbalance").cast("decimal(18,2)").alias("currentBalance"),
            F.lit(None).cast("decimal(18,2)").alias("availableBalance"),
            F.col("ol.createdat").cast("timestamp").alias("createdAt"),
            F.col("ol.deletedat").cast("timestamp").alias("deletedAt"),
            job_run_ts.alias("firstUploaded"),
            F.lit(None).cast("timestamp").alias("lastUploaded"),
        )
    )

    # dim_subaccount: SubAccount path
    # Join chain: OLBSubAccount → OLBAccountType + OLBAccountNumber → OLBFinancialInstitution → BlossomCompany
    df_subaccount_sub = (
        catalog_table("olbsubaccount").alias("osa")
        .join(catalog_table("olbaccounttype").alias("oat"), F.col("osa.idolbaccounttype") == F.col("oat.id"), "left")
        .join(catalog_table("olbaccountnumber").alias("oan"), F.col("osa.idolbaccountnumber") == F.col("oan.id"), "left")
        .join(catalog_table("olbfinancialinstitution").alias("ofi"), F.col("oan.idfi") == F.col("ofi.id"), "left")
        .join(catalog_table("blossomcompany").alias("bc"), F.col("ofi.idblossomcompany") == F.col("bc.id"), "left")
        .select(
            F.concat(F.lit("SUB"), F.col("osa.id").cast("string")).alias("idSubAccount"),
            F.concat(F.lit("INT"), F.col("osa.idolbaccountnumber").cast("string")).alias("idAccount"),
            F.lit(1).alias("idClient"),
            F.col("oan.idfi").cast("string").alias("idCompany"),
            F.col("bc.name").alias("companyName"),
            F.lit("Internal").alias("internalOrExternal"),
            F.col("oat.value").alias("defaultType"),
            F.lit("").alias("doughType"),
            F.lit("asset").alias("defaultAssetLiability"),
            F.lit("").alias("doughAssetLiability"),
            F.when(F.col("osa.deletedat").isNull(), "ACTIVE").otherwise("INACTIVE").alias("status"),
            F.col("osa.amount").cast("decimal(18,2)").alias("currentBalance"),
            F.lit(None).cast("decimal(18,2)").alias("availableBalance"),
            F.col("osa.createdat").cast("timestamp").alias("createdAt"),
            F.col("osa.deletedat").cast("timestamp").alias("deletedAt"),
            job_run_ts.alias("firstUploaded"),
            F.lit(None).cast("timestamp").alias("lastUploaded"),
        )
    )

    # Union both into dim_subaccount
    df_subaccount = df_subaccount_loan.unionByName(df_subaccount_sub)

    # fact_balance_subaccount: Daily date series per loan/subaccount from 2023-05-12 to today
    # Gets last transaction per day and forward-fills missing balances

    # Generate date range
    date_df = spark.sql("""
        SELECT explode(sequence(
            to_date('2023-05-12'),
            current_date(),
            interval 1 day
        )) AS date
    """)

    # --- Loan path ---
    loan_list = catalog_table("olbloantransaction").filter(F.col("status").isNull()).filter(F.col("deletedat").isNull()).select("idolbloan").distinct()
    loan_dates = loan_list.crossJoin(date_df)

    w_loan_day = Window.partitionBy("idolbloan", "date").orderBy(F.col("createdat").desc())
    loan_last_per_day = (
        catalog_table("olbloantransaction")
        .filter(F.col("status").isNull())
        .filter(F.col("deletedat").isNull())
        .withColumn("rn", F.row_number().over(w_loan_day))
        .filter(F.col("rn") == 1)
        .select("idolbloan", "date", "balance")
    )

    loan_info = catalog_table("olbloan").select(
        F.col("id").alias("loan_id"),
        F.col("idolbaccountnumber"),
    )

    w_fill_loan = (
        Window.partitionBy("idolbloan")
        .orderBy("date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    df_fact_loan = (
        loan_dates.alias("ld")
        .join(loan_last_per_day.alias("ft"),
            (F.col("ld.idolbloan") == F.col("ft.idolbloan")) & (F.col("ld.date") == F.col("ft.date")), "left")
        .join(loan_info.alias("li"), F.col("ld.idolbloan") == F.col("li.loan_id"), "left")
        .join(catalog_table("olbaccountnumber").alias("oan"), F.col("li.idolbaccountnumber") == F.col("oan.id"), "left")
        .select(
            F.col("ld.idolbloan"),
            F.col("ld.date"),
            F.col("ft.balance").cast("double").alias("raw_balance"),
            F.col("li.idolbaccountnumber"),
            F.col("oan.idfi"),
        )
        .withColumn("currentBalance",
            F.coalesce(
                F.col("raw_balance"),
                F.last("raw_balance", ignorenulls=True).over(w_fill_loan),
                F.lit(0.0),
            )
        )
        .select(
            F.concat(F.lit("LOAN"), F.col("idolbloan").cast("string")).alias("idSubAccount"),
            F.concat(F.lit("INT"), F.col("idolbaccountnumber").cast("string")).alias("idAccount"),
            F.lit(1).alias("idClient"),
            F.col("idfi").cast("string").alias("idCompany"),
            F.col("date").cast("date").alias("date"),
            F.col("currentBalance").cast("decimal(18,2)").alias("currentBalance"),
            F.lit(None).cast("decimal(18,2)").alias("availableBalance"),
            job_run_ts.alias("firstUploaded"),
            F.lit(None).cast("timestamp").alias("lastUploaded"),
        )
    )

    # --- SubAccount path ---
    sub_list = catalog_table("olbsubaccounttransaction").filter(F.col("status").isNull()).filter(F.col("deletedat").isNull()).select("idsubaccount").distinct()
    sub_dates = sub_list.crossJoin(date_df)

    w_sub_day = Window.partitionBy("idsubaccount", "date").orderBy(F.col("createdat").desc())
    sub_last_per_day = (
        catalog_table("olbsubaccounttransaction")
        .filter(F.col("status").isNull())
        .filter(F.col("deletedat").isNull())
        .withColumn("rn", F.row_number().over(w_sub_day))
        .filter(F.col("rn") == 1)
        .select("idsubaccount", "date", "balance")
    )

    sub_info = catalog_table("olbsubaccount").select(
        F.col("id").alias("sub_id"),
        F.col("idolbaccountnumber"),
    )

    w_fill_sub = (
        Window.partitionBy("idsubaccount")
        .orderBy("date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    df_fact_sub = (
        sub_dates.alias("sd")
        .join(sub_last_per_day.alias("sft"),
            (F.col("sd.idsubaccount") == F.col("sft.idsubaccount")) & (F.col("sd.date") == F.col("sft.date")), "left")
        .join(sub_info.alias("si"), F.col("sd.idsubaccount") == F.col("si.sub_id"), "left")
        .join(catalog_table("olbaccountnumber").alias("oan2"), F.col("si.idolbaccountnumber") == F.col("oan2.id"), "left")
        .select(
            F.col("sd.idsubaccount"),
            F.col("sd.date"),
            F.col("sft.balance").cast("double").alias("raw_balance"),
            F.col("si.idolbaccountnumber"),
            F.col("oan2.idfi"),
        )
        .withColumn("currentBalance",
            F.coalesce(
                F.col("raw_balance"),
                F.last("raw_balance", ignorenulls=True).over(w_fill_sub),
                F.lit(0.0),
            )
        )
        .select(
            F.concat(F.lit("SUB"), F.col("idsubaccount").cast("string")).alias("idSubAccount"),
            F.concat(F.lit("INT"), F.col("idolbaccountnumber").cast("string")).alias("idAccount"),
            F.lit(1).alias("idClient"),
            F.col("idfi").cast("string").alias("idCompany"),
            F.col("date").cast("date").alias("date"),
            F.col("currentBalance").cast("decimal(18,2)").alias("currentBalance"),
            F.lit(None).cast("decimal(18,2)").alias("availableBalance"),
            job_run_ts.alias("firstUploaded"),
            F.lit(None).cast("timestamp").alias("lastUploaded"),
        )
    )

    # Union both into fact_balance_subaccount
    df_fact_balance_subaccount = df_fact_loan.unionByName(df_fact_sub)

    catalog_populates = [
        ("public.dim_company", df_company),
        ("public.dim_member", df_member),
        ("public.dim_account", df_account),
        ("public.dim_subaccount", df_subaccount),
        ("public.bridge_member_account", df_bridge_member_account),
        ("public.fact_balance_subaccount", df_fact_balance_subaccount),
    ]

    for target_table, df in catalog_populates:
        logger.info(f"Writing to {target_table} - Schema: {df.schema}")
        df.write.jdbc(url=jdbc_url, table=target_table, mode="append", properties=jdbc_properties)
        logger.info(f"Completed writing to {target_table}")

    # =========================================================================
    # DICTIONARY TABLES → DynamoDB
    # =========================================================================

    dynamodb = boto3.resource("dynamodb")
    dynamo_table = dynamodb.Table(args["dynamo_dict_name"])

    dictionary_tables = [
        "olbuseraccountownershiptype",
        "olbaccounttype",
        "olbuserstatus",
        "olbtransactioncategory",
    ]

    for dict_table_name in dictionary_tables:
        logger.info(f"Loading dictionary table: {dict_table_name}")
        df_dict = catalog_table(dict_table_name)
        rows = df_dict.collect()
        logger.info(f"  {dict_table_name}: {len(rows)} rows")

        for row in rows:
            item = row.asDict()
            item["dict_id"] = f"{dict_table_name}{item.get('id', '')}"
            clean = {}
            for k, v in item.items():
                if v is None:
                    continue
                elif isinstance(v, datetime):
                    clean[k] = v.isoformat()
                elif isinstance(v, (int, float)):
                    clean[k] = str(v)
                else:
                    clean[k] = v
            dynamo_table.put_item(Item=clean)

        logger.info(f"  Written {len(rows)} items for {dict_table_name}")

    logger.info("Dictionary tables written to DynamoDB.")

    logger.info("All tables created and populated successfully.")
    print("All tables created successfully.")
    job.commit()

except Exception as e:
    logger.error(f"Job failed: {e}", exc_info=True)
    raise
finally:
    upload_log()
