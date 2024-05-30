with
    fonte_transportadoras as (
        select
            cast(SHIPMETHODID as int)
                as SHIPMETHODID
            , cast(NAME as string)
                as nm_transportadora
            , cast(SHIPBASE as numeric(18,2))
                as frete_base
            , cast(SHIPRATE as numeric(18,2))
                as taxa_frete
        from {{ source('erp', 'SHIPMETHOD') }}
    )
select *
from fonte_transportadoras