with
    fonte_estados as (
        select
            cast(STATEPROVINCEID as int)
                as STATEPROVINCEID
            , cast(STATEPROVINCECODE as string)
                as cd_estado
            , cast(COUNTRYREGIONCODE as string)
                as COUNTRYREGIONCODE
            , ISONLYSTATEPROVINCEFLAG as sn_pais_sem_estado
            , cast(NAME as string)
                as nm_estado
            , cast(TERRITORYID as int)
                as TERRITORYID
        from {{ source('erp', 'STATEPROVINCE') }}
    )
select *
from fonte_estados