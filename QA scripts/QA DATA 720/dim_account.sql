select
    concat('LOAN', cast(olbloan.id as varchar)) as id,
    '1' as idClient,
    olbuser.idfi as idCompany,
    olbuseraccount.idolbuser as idMember,
    'Internal' as internalOrExternal,
    blossomcompany.name as CompanyName,
    case when olbloan.deletedat is null then 'ACTIVE' else 'INACTIVE' end as status,
    olbaccounttype."value"  as "type",
    '' as subtype,
    'liability' as assetOrLiability,
    olbuseraccount.id as idOLBUserAccount
from dwl_olb_iceberg.olbloan as olbloan
left join dwl_olb_iceberg.OLBAccountType as OLBAccountType
    on olbloan.idOLBaccountType =OLBAccountType.id
left join dwl_olb_iceberg.olbuserloan as olbuserloan
    on olbloan.id = olbuserloan.idolbloan
left join dwl_olb_iceberg.olbuseraccount as olbuseraccount
    on olbuserloan.idolbuseraccount = olbuseraccount.id
left join dwl_olb_iceberg.olbuser as olbuser
    on olbuser.id = olbuseraccount.idolbuser
left join dwl_olb_iceberg.olbfinancialinstitution as olbfinancialinstitution
    on olbuser.idfi = olbfinancialinstitution.id
left join dwl_olb_iceberg.blossomcompany as blossomcompany
    on olbfinancialinstitution.idblossomcompany = blossomcompany.id
UNION ALL
select
    concat('SUB', cast(olbsubaccount.id as varchar)) as id,
    '1' as idClient,
    olbuser.idfi as idCompany,
    olbuseraccount.idolbuser as idMember,
    'Internal' as internalOrExternal,
    blossomcompany.name as CompanyName,
    case when olbsubaccount.deletedat is null then 'ACTIVE' else 'INACTIVE' end as status,
    olbaccounttype."value"  as "type",
    '' as subtype,
    'asset' as assetOrLiability,
    olbuseraccount.id as idOLBUserAccount
from dwl_olb_iceberg.olbsubaccount as olbsubaccount
left join dwl_olb_iceberg.OLBAccountType as OLBAccountType
    on olbsubaccount.idOLBaccountType =OLBAccountType.id
left join dwl_olb_iceberg.olbsubaccountuser as olbsubaccountuser
    on olbsubaccount.id = olbsubaccountuser.idsubaccount
left join dwl_olb_iceberg.olbuseraccount as olbuseraccount
    on olbsubaccountuser.idolbuseraccount = olbuseraccount.id
left join dwl_olb_iceberg.olbuser as olbuser
    on olbuser.id = olbuseraccount.idolbuser
left join dwl_olb_iceberg.olbfinancialinstitution as olbfinancialinstitution
    on olbuser.idfi = olbfinancialinstitution.id
left join dwl_olb_iceberg.blossomcompany as blossomcompany
    on olbfinancialinstitution.idblossomcompany = blossomcompany.id
