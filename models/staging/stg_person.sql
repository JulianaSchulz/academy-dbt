with
    rennamed as (
        select
            cast(businessentityid as varchar) as id_entid_comercial_pessoa
            --Função concat() adotada para concatenar nomes, nomes do meio e sobrenomes.
            , concat(ifnull(firstname,' '),' ',ifnull(middlename,' '),' ',ifnull(lastname,' ')) as nome_completo
            , cast(persontype as varchar) as tipo_pessoa
            --namestyle
            --suffix
            --emailpromotion
            --additionalcontactinfo
            --demographics
            --rowguid
            --modifieddate
        from {{ source('erp', 'person') }}
    )
select *
from rennamed