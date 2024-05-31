with
    fonte_motivo_vendas as (
        select
            cast(SALESREASONID as int)
                as SALESREASONID
            , cast(NAME as string)
                as ds_motivo_venda
            , cast(REASONTYPE as string)
                as tp_motivo_venda
        from {{ source('erp', 'SALESREASON') }}
    )
select *
from fonte_motivo_vendas