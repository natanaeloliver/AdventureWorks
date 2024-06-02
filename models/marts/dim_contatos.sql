with
    int_contatos as (
        select *
        from {{ ref('int_contatos') }}
    )

    , tabela as (
        select
            PK_CONTATO
            , FK_CLIENTE
            , CD_TERRITORIO
            , CD_CLIENTE
            , CD_LOJA
            , CD_PESSOA
            , CD_TP_CONTATO
            , CHAVE_LOJA_PESSOA
            , CHAVE_CLIENTE_PESSOA_LOJA_TP_CONTATO
            , TP_CONTATO
            , CD_TP_PESSOA_CONTATO
            , TP_PESSOA_CONTATO
            , NM_CONTATO
        from int_contatos
    )

select *
from tabela