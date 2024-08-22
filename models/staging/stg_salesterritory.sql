with
    rennamed as (
        select
            cast(territoryid as int) as pk_territorio
            , cast(name as varchar) as nome_territorio
            , cast(countryregioncode as varchar) as fk_pais     
            , cast(salesytd as int) as vendas_acumuladas
            --saleslastyear --não será usado agora
            --costyts
            --costlastyear
            --rowguid         
            --modifieddate            
        from {{ source("erp", "salesterritory") }}
    )
select *
from
    rennamed