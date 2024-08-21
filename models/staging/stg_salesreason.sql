with
    rennamed as (
        select
            cast(salesreasonid as int) as pk_motivo_venda
            , cast(name as varchar) as motivo_venda
            , cast(reasontype as varchar) as tipo_motivo_venda
            --modifieddate --will not be used now
        from {{ source("erp", "salesreason") }}
    )
select *
from
    rennamed