with
    fonte_vendedores as (
        select
            cast(BUSINESSENTITYID as int)
                as BUSINESSENTITYID
            , cast(TERRITORYID as int)
                as TERRITORYID
            , cast(SALESQUOTA as numeric(18,2))
                as meta_venda
            , cast(BONUS as numeric(18,2))
                as bonus_meta
            , cast(COMMISSIONPCT as numeric(18,2))
                as comicao_perc
        from {{ source('erp', 'SALESPERSON') }}
    )
select *
from fonte_vendedores