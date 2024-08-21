with 
    rennamed as (
        select
            cast(addressid as int) as pk_endereco
            , cast(addressline1 as varchar) as endereco
            , cast(city as varchar) as cidade
            , cast(stateprovinceid as varchar) as fk_estado
            , cast(postalcode as varchar) as codigo_postal
            -- addressline2 -- will not be used now
            -- spatiallocation        
            -- rowguid   
            -- modifieddate      
        from {{ source('erp', 'address') }}
)
select *
from rennamed