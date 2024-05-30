with
    fonte_pedidos as (
        select
            cast(SALESORDERID as int)
                as SALESORDERID
            , cast(REVISIONNUMBER as int)
                as REVISIONNUMBER
            , cast(ORDERDATE as date)
                as dt_pedido
            , case
                when STATUS = 1 then 'Em processamento'
                when STATUS = 2 then 'Aprovado'
                when STATUS = 3 then 'Em espera'
                when STATUS = 4 then 'Rejeitado'
                when STATUS = 5 then 'Enviado'
                when STATUS = 6 then 'Cancelado'
            end as status_pedido
            , case
                when ONLINEORDERFLAG = True then 'Pedido feito pelo cliente'
                when ONLINEORDERFLAG = False then 'Pedido feito pelo setor de vendas'
            end as tp_pedido
            , cast(CUSTOMERID as int)
                as CUSTOMERID
            , cast(SALESPERSONID as int)
                as SALESPERSONID
            , cast(TERRITORYID as int)
                as TERRITORYID
            , cast(SHIPMETHODID as int)
                as SHIPMETHODID
            , cast(CREDITCARDID as int)
                as CREDITCARDID
            , cast(SUBTOTAL as numeric(18,2))
                as check_subtotal
            , cast(TAXAMT as numeric(18,2))
                as taxas
            , cast(FREIGHT as numeric(18,2))
                as frete
            , cast(TOTALDUE as numeric(18,2))
                as check_total_pedido
        from {{ source('erp', 'SALESORDERHEADER') }}
    )
select *
from fonte_pedidos