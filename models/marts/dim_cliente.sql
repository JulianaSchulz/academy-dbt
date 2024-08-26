with 
    person as (
        select
            id_entid_comercial_pessoa
            , nome_completo
        from {{ref('stg_person')}}
    )

    , store as (
        select
            pk_loja
            , nome_loja
        from {{ref('stg_store')}}
    )

    , customer as (
        select 
            pk_cliente
            , fk_pessoa
            , fk_loja
        from {{ref('stg_customer')}}
    )

    , joined as (
            select
                customer.pk_cliente 
                , customer.fk_pessoa
                , person.nome_completo
                , customer.fk_loja
                , store.nome_loja
            from customer
            left join person on person.id_entid_comercial_pessoa = customer.pk_cliente
            left join store on store.pk_loja = customer.fk_loja
    )

    , transformed as (
        select
        -- criado a chave surrogate autoincremental para o id do cliente.
        SEQ8() as cliente_sk  -- Chave surrogate usando SEQ8()
        , pk_cliente
        , nome_completo
        , nome_loja
        , case 
            when fk_pessoa is null and fk_loja is not null then 'Store'
            when fk_pessoa is not null and fk_loja is null then 'Natural Person'
            when fk_pessoa is not null and fk_loja is not null then 'Store Contact'
            end as tipo_pessoa
        from joined
    )

select *
from transformed