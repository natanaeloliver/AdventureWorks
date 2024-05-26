with
    fonte_estados as (
        select
            cast(STATEPROVINCEID as int)
                as STATEPROVINCEID
            , STATEPROVINCECODE as cd_estado
            , COUNTRYREGIONCODE
            , ISONLYSTATEPROVINCEFLAG as sn_pais_sem_estado
            , NAME as nm_estado
            , cast(TERRITORYID as int)
                as TERRITORYID
        from {{ source('erp', 'STATEPROVINCE') }}
    )
select *
from fonte_estados