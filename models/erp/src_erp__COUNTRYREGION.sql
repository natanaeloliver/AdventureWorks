with
    fonte_paises as (
        select
            COUNTRYREGIONCODE
            , NAME as nm_pais
        from {{ source('erp', 'COUNTRYREGION') }}
    )
select *
from fonte_paises