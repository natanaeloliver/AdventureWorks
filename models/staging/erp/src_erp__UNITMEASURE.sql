with
    fonte_unidades as (
        select
            cast(UNITMEASURECODE as string)
                as UNITMEASURECODE
            , cast(NAME as string)
                as nm_unidade
        from {{ source('erp', 'UNITMEASURE') }}
    )
select *
from fonte_unidades