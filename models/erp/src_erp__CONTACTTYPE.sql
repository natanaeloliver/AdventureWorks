with
    fonte_tipo_contato as (
        select
            cast(CONTACTTYPEID as int)
                as CONTACTTYPEID
            , NAME as tp_contato
        from {{ source('erp', 'CONTACTTYPE') }}
    )
select *
from fonte_tipo_contato