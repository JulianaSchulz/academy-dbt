with
    rennamed as (
        select
            cast(emailaddressid as int) as pk_email
            , cast(emailaddress as varchar) as email
            , cast(businessentityid as varchar) as id_entid_comercial_email
            --rowguid --não será usado agora           
            --modifieddate            
        from {{ source("erp", "emailaddress") }}
    )
select *
from
    rennamed