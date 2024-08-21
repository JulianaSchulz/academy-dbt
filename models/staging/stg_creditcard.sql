with
    rennamed as (
        select
            cast(creditcardid as int) as pk_cartao_credito 
            , cast(cardtype as varchar) as tipo_cartao
            , cast(cardnumber as varchar) as numero_cartao
            --expmonth -- will not be used now
            --expyear
            --modifieddata
        from {{ source('erp', 'creditcard') }}
    )
select *
from rennamed