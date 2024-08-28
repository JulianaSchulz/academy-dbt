with stg_salesorderheader as (
    select 
        distinct(fk_cartao_credito)
    from {{ref('stg_salesorderheader')}}
    where fk_cartao_credito is not null
)

, stg_creditcard as (
    select *
    from {{ref('stg_creditcard')}}
)

, transformed as (
    select 
        -- criado a chave surrogate autoincremental para o id do cartao de credito.
        row_number() over (order by stg_salesorderheader.fk_cartao_credito) as cartao_credito_sk 
        , stg_salesorderheader.fk_cartao_credito as id_cartao_credito
        , stg_creditcard.tipo_cartao
    from stg_salesorderheader 
    left join stg_creditcard on stg_salesorderheader.fk_cartao_credito = stg_creditcard.pk_cartao_credito
)

select *
from transformed