with
    int_pedidos_itens as (
        select *
        from {{ ref('int_pedidos_itens') }}
    )

    , tabela as (
        select
            PK_ITEM_PEDIDO
            , FK_CLIENTE
            , FK_ENDERECO
            , FK_CARTAO
            , FK_VENDEDOR
            , FK_PEDIDO
            , FK_PRODUTO
            , CD_TERRITORIO
            , CD_TRANSPORTADORA
            , CD_CLIENTE
            , CD_ENDERECO
            , CD_CARTAO
            , CD_VENDEDOR
            , CD_OFERTA
            , CD_PEDIDO
            , CD_TP_PEDIDO
            , CD_STATUS_PEDIDO
            , CD_ITEM_PEDIDO
            , NUMERO_REVISAO
            , VALOR_NEGOCIADO
            , VALOR_NEGOCIADO_LIQUIDO
            , VALOR_TOTAL_ITEM
            , TP_PEDIDO
            , DT_PEDIDO
            , STATUS_PEDIDO
            , CD_PRODUTO_NUM
            , PRECO_UNITARIO
            , QTD_PRODUTO
            , DESCONTO_PERC
            , NM_TRANSPORTADORA
            , TP_OFERTA
            , DS_OFERTA
            , CATEGORIA_OFERTA
            , CHECK_SUBTOTAL_DISTRIBUIDO
            , TAXAS_DISTRIBUIDAS
            , TAXA_FRETE
            , CHECK_FRETE_DISTRIBUIDO
            , FRETE_BASE_DISTRIBUIDO
            , CHECK_TOTAL_PEDIDO_DISTRIBUIDO
            , DESCONTO_PERC_OFERTA
            , DT_OFERTA_INICIAL
            , DT_OFERTA_FINAL
            , QTD_MIN_OFERTA_DISTRIBUIDA
            , QTD_MAX_OFERTA_DISTRIBUIDA
        from int_pedidos_itens
    )

select *
from tabela