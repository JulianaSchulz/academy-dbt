with
    rennamed as (
        select
            cast(salesorderid as int) as pk_pedido
            , cast(orderdate as date) as data_pedido
            , shipdate::date as data_envio
            , cast(status as varchar) as status_pedido            
            , cast(purchaseordernumber as varchar) as numero_pedido
            , cast(customerid as varchar) as fk_cliente
            , cast(salespersonid as varchar) as fk_vendedor 
            , cast(territoryid as varchar) as fk_territorio
            , cast(shiptoaddressid as varchar) as fk_endereco_envio
            , cast(shipmethodid as varchar) as metodo_envio_id
            , cast(creditcardid as varchar) as fk_cartao_credito
            , cast(subtotal as float) as subtotal_vendas
            , cast(taxamt as float) as imposto
            , cast(freight as float) as frete
            , cast(totaldue as float) as total_devido
            --revisionnumber --não será usado agora.
            --duedate
            --onlineorderflag
            --accountnumber
            --billtoaddressid
            --creditcardapprovalcode
            --currencyrateid
            --rowguid
            --modifieddate 
        from {{ source("erp", "salesorderheader") }}
    )
select *
from
    rennamed