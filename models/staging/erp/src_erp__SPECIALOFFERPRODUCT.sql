with
    fonte_pedidos_ofertas as (
        select
            cast(SPECIALOFFERID as int)
                as SPECIALOFFERID
            , cast(PRODUCTID as int)
                as PRODUCTID
        from {{ source('erp', 'SPECIALOFFERPRODUCT') }}
    )
select *
from fonte_pedidos_ofertas