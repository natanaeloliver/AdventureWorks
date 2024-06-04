with
    fonte_estoque_produtos as (
        select
            cast(PRODUCTSUBCATEGORYID as int)
                as PRODUCTSUBCATEGORYID
            , cast(PRODUCTCATEGORYID as int)
                as PRODUCTCATEGORYID
            , NAME as nm_subcategoria_produto
        from {{ source('erp', 'PRODUCTSUBCATEGORY') }}
    )
select *
from fonte_estoque_produtos