with
    int_pedidos_motivos_compra as (
        select *
        from {{ ref('int_pedidos_motivos_venda') }}
    )

    , tabela as (
        select
            PK_PEDIDO_MOTIVO_VENDA
            , FK_PEDIDO
            , CHAVE_PEDIDO_MOTIVO_VENDA
            , CD_PEDIDO
            , CD_MOTIVO_VENDA
            , DS_MOTIVO_VENDA
            , TP_MOTIVO_VENDA
        from int_pedidos_motivos_compra
    )

select *
from tabela