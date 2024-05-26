with
    fonte_unidades as (
        select
            UNITMEASURECODE
            , cast(NAME as string)
                as nm_unidade
        from {{ source('erp', 'UNITMEASURE') }}
    )
select *
from fonte_unidades