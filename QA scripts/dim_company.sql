SELECT 
    olbfinancialinstitution.id as idcompany,
    '1' as idclient,
    blossomcompany.name as name 
FROM "AwsDataCatalog"."dwh_olb_iceberg"."olbfinancialinstitution" as olbfinancialinstitution
LEFT JOIN "AwsDataCatalog"."dwh_olb_iceberg"."blossomcompany" as blossomcompany
ON olbfinancialinstitution.idblossomcompany = blossomcompany.id