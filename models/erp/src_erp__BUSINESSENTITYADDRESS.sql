with
    fonte_entidade_negocio_endereco as (
        select
            cast(BUSINESSENTITYID as int)
                as BUSINESSENTITYID
            , cast(ADDRESSID as int)
                as ADDRESSID
            , cast(ADDRESSTYPEID as int)
                as ADDRESSTYPEID
        from {{ source('erp', 'BUSINESSENTITYADDRESS') }}
    )
select *
from fonte_entidade_negocio_endereco