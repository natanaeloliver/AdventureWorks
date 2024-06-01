with
    int_enderecos as (
        select *
        from {{ ref('int_enderecos') }}
    )

    , tabela as (
        select
            PK_ENDERECO
            , FK_PESSOA
            , CD_PAIS
            , CD_ESTADO
            , CD_TERRITORIO
            , CD_CIDADE
            , CD_TP_ENDERECO
            , CD_ENDERECO
            , CD_PESSOA
            , CHAVE_PESSOA_ENDERECO
            , NM_PAIS
            , NM_ESTADO
            , CIDADE
            , TP_ENDERECO
            , SN_PAIS_SEM_ESTADO
            , LATITUDE
            , LONGITUDE
            , TP_CIDADE
            , POPULATION
        from int_enderecos
    )

select *
from tabela