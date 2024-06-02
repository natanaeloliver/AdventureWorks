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
            , CD_VENDEDOR_ATRIBUIDO
            , TP_CLIENTE
            , NM_VENDEDOR_ATRIBUIDO
        from int_clientes
    )

select *
from tabela