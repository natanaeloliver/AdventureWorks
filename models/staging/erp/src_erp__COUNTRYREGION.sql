with
    fonte_paises as (
        select
            cast(COUNTRYREGIONCODE as string)
                as COUNTRYREGIONCODE
            , cast(NAME as string)
                as nm_pais
        from {{ source('erp', 'COUNTRYREGION') }}
    )
select *
from fonte_paises