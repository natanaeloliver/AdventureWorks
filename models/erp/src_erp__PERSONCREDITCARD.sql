with
    fonte_entidade_negocio_cartoes as (
        select
            cast(BUSINESSENTITYID as int)
                as BUSINESSENTITYID
            , cast(CREDITCARDID as int)
                as CREDITCARDID
        from {{ source('erp', 'PERSONCREDITCARD') }}
    )
select *
from fonte_entidade_negocio_cartoes