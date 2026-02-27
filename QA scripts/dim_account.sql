select 
concat('INT',cast(id as varchar)) as idAccount
, idfi as idCompany
, '1' as idClient
, 'Internal' as InternalExternal
FROM "AwsDataCatalog"."dwh_olb_iceberg"."olbaccountnumber" AS olbaccountnumber
