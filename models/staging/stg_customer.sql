with
    rennamed as (
        select
            cast(customerid as int) as pk_cliente
            , cast(storeid as varchar) as fk_loja
            , cast(territoryid as varchar) as fk_territorio
            , cast(personid as varchar) as fk_pessoa
            --rowguide --não será usado agora
            --modifieddata 
        from {{ source('erp', 'customer') }}
    )
select *
from rennamed