with
    rennamed as (
        select
            cast(territoryid as int) as pk_territorio
            , cast(name as varchar) as nome_territorio
            , cast(countryregioncode as varchar) as fk_pais     
            , cast(salesytd as int) as vendas_acumuladas
            --saleslastyear
            --costyts
            --costlastyear
            --rowguid --will not be used now            
            --modifieddate            
        from {{ source("erp", "salesterritory") }}
    )
select *
from
    rennamed