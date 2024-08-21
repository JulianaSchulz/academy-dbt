with
    rennamed as (
        select
            cast(businessentityid as varchar) as pk_loja
            , cast(name as varchar) as nome_loja
            , cast(salespersonid as int) as fk_vendedor
            --demographics --will not be used now
            --rowguid
            --modifieddate 
        from {{ source("erp", "store") }}
    )
select *
from
    rennamed