with
    int_enderecos as (
        select *
        from {{ ref('int_enderecos') }}
    )

    , tabela as (
        select
            PK_ENDERECO
            , CD_PAIS
            , CD_ESTADO
            , CD_TERRITORIO
            , CD_CIDADE
            , CD_TP_ENDERECO
            , CD_ENDERECO
            , CD_ENTIDADE_NEGOCIO
            , NM_PAIS
            , NM_ESTADO
            , CIDADE
            , TP_ENDERECO
            , SN_PAIS_SEM_ESTADO
            , LATITUDE
            , LONGITUDE
            , TP_CIDADE
        from int_enderecos
    )

select *
from tabela