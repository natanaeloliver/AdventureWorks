with
    int_pedidos as (
        select *
        from {{ ref('int_pedidos') }}
    )

    , tabela as (
        select
            PK_PEDIDO
            , FK_CLIENTE
            , FK_ENDERECO
            , FK_CARTAO
            , FK_VENDEDOR
            , CD_TERRITORIO
            , CD_TRANSPORTADORA
            , CD_CLIENTE
            , CD_PESSOA
            , CD_VENDEDOR_ATRIBUIDO
            , CD_ENDERECO
            , CHAVE_CLIENTE_CARTAO
            , CHAVE_PESSOA_ENDERECO
            , CD_CARTAO
            , CD_VENDEDOR
            , CD_PEDIDO
            , CD_TP_PEDIDO
            , CD_STATUS_PEDIDO
            , NUMERO_REVISAO
            , DT_PEDIDO
            , STATUS_PEDIDO
            , TP_PEDIDO
            , CHECK_SUBTOTAL
            , TAXAS_PEDIDO
            , FRETE_PEDIDO
            , CHECK_TOTAL_PEDIDO
            , NM_TRANSPORTADORA
            , FRETE_BASE
            , TAXA_FRETE
        from int_pedidos
    )

select *
from tabela