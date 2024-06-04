with
    fonte_entidade_negocio as (
        select
            cast(BUSINESSENTITYID as int)
                as BUSINESSENTITYID
        from {{ source('erp', 'BUSINESSENTITY') }}
    )
select *
from fonte_entidade_negocio