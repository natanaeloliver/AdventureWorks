with
    fonte_entidade_negocio_pessoa_tipo_contato as (
        select
            cast(BUSINESSENTITYID as int)
                as BUSINESSENTITYID
            , cast(PERSONID as int)
                as PERSONID
            , cast(CONTACTTYPEID as int)
                as CONTACTTYPEID
        from {{ source('erp', 'BUSINESSENTITYCONTACT') }}
    )
select *
from fonte_entidade_negocio_pessoa_tipo_contato