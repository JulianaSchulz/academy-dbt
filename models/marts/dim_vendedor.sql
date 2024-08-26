with
    employee as (
        select
            id_funcionario
            , cargo
            , genero
            , data_contratacao 
        from {{ref('stg_employee')}}
    )

    , salesperson as (
        select
            id_vendedor 
            , id_territorio 
            , cota_vendas
            , bonus
            , vendas_acumuladas
            , vendas_ano_passado
        from {{ref('stg_salesperson')}}
    )

    , transformed as (
        select 
            SEQ8() as sk_vendedor  -- Chave surrogate usando SEQ8()
            , salesperson.id_vendedor
            , employee.cargo
            , employee.genero
            , salesperson.cota_vendas
            , salesperson.vendas_acumuladas
            , salesperson.vendas_ano_passado
        from salesperson
        left join employee on employee.id_funcionario = salesperson.id_vendedor
    )

select *
from transformed