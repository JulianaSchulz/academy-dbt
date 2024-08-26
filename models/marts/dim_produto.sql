with
    product as (
        select
            pk_produto
            , nome_produto
            , id_subcategoria_produto 
        from {{ref('stg_product')}}
    )

    , productsubcategory as (
        select
            id_subcategoria_produto 
            , id_categoria_produto 
            , nome_subcategoria
        from {{ref('stg_productsubcategory')}}
    )

    , productcategory as (
        select
            id_categoria_produto 
            , nome_categoria
        from {{ref('stg_productcategory')}}
    )

    , salesorderdetail as (
        select
            fk_produto 
            , preco_unitario
        from {{ref('stg_salesorderdetail')}}
    )

    , transformed as (
        select 
            SEQ8() as sk_produto  -- Chave surrogate usando SEQ8()
            , product.pk_produto
            , product.nome_produto
            , productcategory.nome_categoria
            , productsubcategory.nome_subcategoria
            , salesorderdetail.preco_unitario
        from product
        left join productsubcategory on product.id_subcategoria_produto = productsubcategory.id_subcategoria_produto
        left join productcategory on productsubcategory.id_categoria_produto = productcategory.id_categoria_produto
        left join salesorderdetail on salesorderdetail.fk_produto = product.pk_produto
    )

select *
from transformed