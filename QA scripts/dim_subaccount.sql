
SELECT
    CONCAT('LOAN', CAST(olbloan.id AS VARCHAR)) AS idSubaccount
    , CONCAT('INT', CAST(olbloan.idolbaccountnumber AS VARCHAR)) AS idAccount
    , '1' AS idClient
    , olbaccountnumber.idfi AS idCompany
    , blossomcompany.name AS companyName
    , 'Internal' AS internalOrExternal
    , CASE WHEN olbloan.deletedat IS NULL THEN 'ACTIVE' ELSE 'INACTIVE' END AS status
    , olbaccounttype.value AS defaultType
    , '' AS doughType
    , 'liability' AS defaultAssetLiability
    , '' AS doughAssetLiability
    , olbloan.currentbalance AS currentBalance
    , CASE WHEN olbloan.deletedat IS NULL THEN 'ACTIVE' ELSE 'INACTIVE' END AS status
    , olbloan.createdat
    , olbloan.deletedat
FROM "AwsDataCatalog"."dwh_olb_iceberg"."olbloan" AS olbloan
LEFT JOIN "AwsDataCatalog"."dwh_olb_iceberg"."olbaccounttype" AS olbaccounttype
    ON olbloan.idolbaccounttype = olbaccounttype.id
LEFT JOIN "AwsDataCatalog"."dwh_olb_iceberg"."olbaccountnumber" AS olbaccountnumber
    ON olbloan.idolbaccountnumber = olbaccountnumber.id
LEFT JOIN "AwsDataCatalog"."dwh_olb_iceberg"."olbfinancialinstitution" AS olbfinancialinstitution
    ON olbaccountnumber.idfi = olbfinancialinstitution.id
LEFT JOIN "AwsDataCatalog"."dwh_olb_iceberg"."blossomcompany" AS blossomcompany
    ON olbfinancialinstitution.idblossomcompany = blossomcompany.id

UNION ALL

SELECT
    CONCAT('SUB', CAST(olbsubaccount.id AS VARCHAR)) AS idSubaccount
    , CONCAT('INT', CAST(olbsubaccount.idolbaccountnumber AS VARCHAR)) AS idAccount
    , '1' AS idClient
    , olbaccountnumber.idfi AS idCompany
    , blossomcompany.name AS companyName
    , 'Internal' AS internalOrExternal
    , CASE WHEN olbsubaccount.deletedat IS NULL THEN 'ACTIVE' ELSE 'INACTIVE' END AS status
    , at.value AS defaultType
    , '' AS doughType
    , 'asset' AS defaultAssetLiability
    , '' AS doughAssetLiability
    , olbsubaccount.amount AS currentBalance
    , CASE WHEN olbsubaccount.deletedat IS NULL THEN 'ACTIVE' ELSE 'INACTIVE' END AS status
    , olbsubaccount.createdat
    , olbsubaccount.deletedat
FROM "AwsDataCatalog"."dwh_olb_iceberg"."olbsubaccount" AS olbsubaccount
LEFT JOIN "AwsDataCatalog"."dwh_olb_iceberg"."olbaccounttype" AS at
    ON olbsubaccount.idolbaccounttype = at.id
LEFT JOIN "AwsDataCatalog"."dwh_olb_iceberg"."olbaccountnumber" AS olbaccountnumber
    ON olbsubaccount.idolbaccountnumber = olbaccountnumber.id
LEFT JOIN "AwsDataCatalog"."dwh_olb_iceberg"."olbfinancialinstitution" AS olbfinancialinstitution
    ON olbaccountnumber.idfi = olbfinancialinstitution.id
LEFT JOIN "AwsDataCatalog"."dwh_olb_iceberg"."blossomcompany" AS blossomcompany
    ON olbfinancialinstitution.idblossomcompany = blossomcompany.id
