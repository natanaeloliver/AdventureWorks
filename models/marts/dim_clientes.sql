with
    int_clientes as (
        select *
        from {{ ref('int_clientes') }}
    )

    , tabela as (
        select
            PK_CLIENTE
            , CD_CLIENTE
            , CD_LOJA
            , CD_PESSOA
            , CD_TP_PESSOA
            , CD_ENTIDADE_NEGOCIO
            , CD_VENDEDOR_ATRIBUIDO
            , NM_VENDEDOR_ATRIBUIDO
            , NM_CLIENTE_LOJA
            , TP_CLIENTE
            , NM_CLIENTE_PESSOA
            , TP_PESSOA_CLIENTE
            , TP_CONTATO
        from int_clientes
    )

select *
from tabela