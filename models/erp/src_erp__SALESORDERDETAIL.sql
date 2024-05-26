with
    fonte_pedidos_itens as (
        select
            cast(SALESORDERID as int)
                as SALESORDERID
            , cast(SALESORDERDETAILID as int)
                as SALESORDERDETAILID
            , cast(ORDERQTY as int)
                as qtd_produto
            , cast(PRODUCTID as int)
                as PRODUCTID
            , cast(SPECIALOFFERID as int)
                as SPECIALOFFERID
            , cast(UNITPRICE as numeric(18,2))
                as preco_unitario
            , cast(UNITPRICEDISCOUNT as numeric(18,2))
                as desconto_perc
        from {{ source('erp', 'SALESORDERDETAIL') }}
    )
select *
from fonte_pedidos_itens