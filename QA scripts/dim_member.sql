SELECT
   olbuser.id AS idmember
   ,olbuser.idfi AS idcompany
   ,'1' AS idclient
   ,olbuser.createdat AS createdat
   ,olbuser.deletedat AS deletedat
   ,CASE
      WHEN olbuser.deletedat IS NULL THEN 'ACTIVE'
      ELSE 'INACTIVE'
   END AS status
FROM "AwsDataCatalog"."dwh_olb_iceberg"."olbuser" AS olbuser
LEFT JOIN "AwsDataCatalog"."dwh_olb_iceberg"."olbfisubrole" AS olbfisubrole
   ON olbuser.idolbfisubrole = olbfisubrole.id
LEFT JOIN "AwsDataCatalog"."dwh_olb_iceberg"."olbuserrole" AS olbuserrole
   ON olbfisubrole.idolbuserrole = olbuserrole.id
WHERE olbuserrole.type = 'MEMBER'
