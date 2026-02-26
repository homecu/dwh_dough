WITH idolbloan_list AS (
  SELECT DISTINCT idolbloan FROM "OLBLoanTransaction"
),
date_series AS (
  SELECT idolbloan, date_add('day', n, DATE '2023-05-12') AS date
  FROM idolbloan_list
  CROSS JOIN UNNEST(SEQUENCE(0, DATE_DIFF('day', DATE '2023-05-12', CURRENT_DATE))) AS t(n)
),
last_txn_per_day AS (
  SELECT
    idolbloan,
    date,
    balance,
    ROW_NUMBER() OVER (
      PARTITION BY idolbloan, date
      ORDER BY createdat DESC
    ) AS rn
  FROM "OLBLoanTransaction"
),
filtered_txn AS (
  SELECT idolbloan, date, balance
  FROM last_txn_per_day
  WHERE rn = 1
),
loan_info AS (
  SELECT id AS loan_id, idolbaccountnumber FROM "AwsDataCatalog"."dwh_olb_iceberg"."olbloan"
),
subaccount_list AS (
  SELECT DISTINCT idsubaccount FROM "OLBSubAccountTransaction"
),
sub_date_series AS (
  SELECT idsubaccount, date_add('day', n, DATE '2023-05-12') AS date
  FROM subaccount_list
  CROSS JOIN UNNEST(SEQUENCE(0, DATE_DIFF('day', DATE '2023-05-12', CURRENT_DATE))) AS t(n)
),
sub_last_txn_per_day AS (
  SELECT
    idsubaccount,
    date,
    balance,
    ROW_NUMBER() OVER (
      PARTITION BY idsubaccount, date
      ORDER BY createdat DESC
    ) AS rn
  FROM "OLBSubAccountTransaction"
),
sub_filtered_txn AS (
  SELECT idsubaccount, date, balance
  FROM sub_last_txn_per_day
  WHERE rn = 1
),
subaccount_info AS (
  SELECT id AS subaccount_id, idolbaccountnumber FROM "AwsDataCatalog"."dwh_olb_iceberg"."olbsubaccount"
)
SELECT
    concat('LOAN', cast(ds.idolbloan AS varchar)) AS idSubAccount
  , concat('INT', cast(li.idolbaccountnumber AS varchar)) AS idAccount
  , 1 AS idClient
  , acn.idfi AS idCompany
  , ds.date
  , COALESCE(
        CAST(ft.balance AS double)
      , MAX(CAST(ft.balance AS double)) OVER (
            PARTITION BY ds.idolbloan
            ORDER BY ds.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
      , 0.0
    ) AS balance
FROM date_series ds
LEFT JOIN filtered_txn ft
  ON ft.idolbloan = ds.idolbloan AND ft.date = ds.date
LEFT JOIN loan_info li
  ON ds.idolbloan = li.loan_id
LEFT JOIN "AwsDataCatalog"."dwh_olb_iceberg"."olbaccountnumber" acn
  ON li.idolbaccountnumber = acn.id
LEFT JOIN "AwsDataCatalog"."dwh_olb_iceberg"."blossomcompany" bc
  ON acn.idfi = bc.id

UNION ALL

SELECT
    concat('SUB', cast(sds.idsubaccount AS varchar)) AS idSubAccount
  , concat('INT', cast(si.idolbaccountnumber AS varchar)) AS idAccount
  , 1 AS idClient
  , acn.idfi AS idCompany
  , sds.date
  , COALESCE(
        CAST(sft.balance AS double)
      , MAX(CAST(sft.balance AS double)) OVER (
            PARTITION BY sds.idsubaccount
            ORDER BY sds.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
      , 0.0
    ) AS balance
FROM sub_date_series sds
LEFT JOIN sub_filtered_txn sft
  ON sft.idsubaccount = sds.idsubaccount AND sft.date = sds.date
LEFT JOIN subaccount_info si
  ON sds.idsubaccount = si.subaccount_id
LEFT JOIN "AwsDataCatalog"."dwh_olb_iceberg"."olbaccountnumber" acn
  ON si.idolbaccountnumber = acn.id
LEFT JOIN "AwsDataCatalog"."dwh_olb_iceberg"."blossomcompany" bc
  ON acn.idfi = bc.id

ORDER BY idSubAccount, date;