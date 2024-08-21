with
    rennamed as (
        select
            cast(salesorderid as int) as fk_pedido
            , cast(salesorderdetailid as varchar) as pk_pedido_detalhado
            , cast(orderqty as int) as qtd_pedido
            , cast(productid as varchar) as fk_produto
            , cast(unitprice as float) as preco_unitario
            , cast(unitpricediscount as float) as desconto_preco_unitario
            --carriertrackingnumber -- 
            --specialofferid
            --rowguid
            --modifiedate            
        from {{ source('erp', 'salesorderdetail') }}
    )
select *
from rennamed