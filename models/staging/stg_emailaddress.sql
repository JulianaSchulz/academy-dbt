with
    rennamed as (
        select
            cast(emailaddressid as int) as pk_email
            , cast(emailaddress as varchar) as email
            , cast(businessentityid as varchar) as id_entid_comercial_email
            --rowguid --will not be used now            
            --modifieddate            
        from {{ source("erp", "emailaddress") }}
    )
select *
from
    rennamed