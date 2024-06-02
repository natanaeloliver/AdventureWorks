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
            , CD_ENTIDADE_NEGOCIO
            , CD_VENDEDOR_ATRIBUIDO
            , CD_CARTAO
            , CHAVE_CLIENTE_CARTAO
            , TP_CLIENTE
            , NM_VENDEDOR_ATRIBUIDO
        from int_clientes
    )

select *
from tabela