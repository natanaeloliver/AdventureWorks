with
    fonte_enderecos as (
        select
            cast(ADDRESSID as int)
                as ADDRESSID
            , CITY as cidade
            , cast(STATEPROVINCEID as int)
                as STATEPROVINCEID
        from {{ source('erp', 'ADDRESS') }}
    )
select *
from fonte_enderecos