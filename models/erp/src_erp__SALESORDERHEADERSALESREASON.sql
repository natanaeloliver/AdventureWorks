with
    fonte_pedidos_motivos_venda as (
        select
            cast(SALESORDERID as int)
                as SALESORDERID
            , cast(SALESREASONID as int)
                as SALESREASONID
        from {{ source('erp', 'SALESORDERHEADERSALESREASON') }}
    )
select *
from fonte_pedidos_motivos_venda