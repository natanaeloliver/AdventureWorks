with
    fonte_clientes as (
        select
            cast(CUSTOMERID as int)
                as CUSTOMERID
            , cast(PERSONID as int)
                as PERSONID
            , cast(STOREID as int)
                as STOREID
            , cast(TERRITORYID as int)
                as TERRITORYID
        from {{ source('erp', 'CUSTOMER') }}
    )
select *
from fonte_clientes