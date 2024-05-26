with
    fonte_lojas as (
        select
            cast(BUSINESSENTITYID as int)
                as BUSINESSENTITYID
            , cast(NAME as string)
                as nm_loja
            , cast(SALESPERSONID as int)
                as SALESPERSONID
        from {{ source('erp', 'STORE') }}
    )
select *
from fonte_lojas