with
    fonte_territorio_vendas as (
        select
            cast(BUSINESSENTITYID as int)
                as BUSINESSENTITYID
            , cast(TERRITORYID as int)
                as TERRITORYID
            , cast(STARTDATE as date)
                as dt_territorio_venda_inicial
            , cast(ENDDATE as date)
                as dt_territorio_venda_final
        from {{ source('erp', 'SALESTERRITORYHISTORY') }}
    )
select *
from fonte_territorio_vendas