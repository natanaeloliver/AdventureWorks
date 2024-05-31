with
    fonte_categorias_produtos as (
        select
            cast(PRODUCTCATEGORYID as int)
                as PRODUCTCATEGORYID
            , case
                when NAME = 'Bikes'         then 'Bicicletas'
                when NAME = 'Components'    then 'Componentes'
                when NAME = 'Clothing'      then 'Roupas'
                when NAME = 'Accessories'   then 'Acessórios'
            end as nm_categoria_produto
        from {{ source('erp', 'PRODUCTCATEGORY') }}
    )
select *
from fonte_categorias_produtos