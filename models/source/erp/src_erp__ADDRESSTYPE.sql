with
    fonte_tipo_enderecos as (
        select
            cast(ADDRESSTYPEID as int)
                as ADDRESSTYPEID
            , NAME as tp_endereco
        from {{ source('erp', 'ADDRESSTYPE') }}
    )
select *
from fonte_tipo_enderecos