SELECT
 olbuseraccount.idolbuser AS idMember
 , olbuseraccount.idolbaccountnumber AS idAccount
 , olbuser.idfi AS idCompany
 , '1' AS idClient
 , olbuseraccountownershiptype."key" AS ownershipType
 , olbuseraccount.primary AS isPrimary
 , CASE
      WHEN olbuseraccount.deletedat IS NULL THEN 'ACTIVE'
      ELSE 'INACTIVE'
   END AS status
 , olbuseraccount.createdat AS createdat
 , olbuseraccount.deletedat AS deletedat
FROM "AwsDataCatalog"."dwh_olb_iceberg"."olbuseraccount" AS olbuseraccount
LEFT JOIN  "AwsDataCatalog"."dwh_olb_iceberg"."olbuseraccountownershiptype" AS olbuseraccountownershiptype
ON olbuseraccount.idolbuseraccountownershiptype = olbuseraccountownershiptype.id 
LEFT JOIN "AwsDataCatalog"."dwh_olb_iceberg"."olbuser" AS olbuser
ON olbuseraccount.idolbuser = olbuser.id
WHERE olbuseraccountownershiptype.type = 'MEMBER'
