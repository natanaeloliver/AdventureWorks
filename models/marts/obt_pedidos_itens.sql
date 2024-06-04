with
    fact_pedidos_itens as (
        select *
        from {{ ref('fact_pedidos_itens') }}
    )

    , dim_cartoes as (
        select *
        from {{ ref('dim_cartoes') }}
    )

    , dim_clientes as (
        select *
        from {{ ref('dim_clientes') }}
    )

    , dim_enderecos as (
        select *
        from {{ ref('dim_enderecos') }}
    )

    , dim_produtos as (
        select *
        from {{ ref('dim_produtos') }}
    )

    , dim_vendedores as (
        select *
        from {{ ref('dim_vendedores') }}
    )

    , one_big_table as (
        select
            fact_pedidos_itens.PK_ITEM_PEDIDO
            , fact_pedidos_itens.FK_CLIENTE
            , fact_pedidos_itens.FK_ENDERECO
            , fact_pedidos_itens.FK_CARTAO
            , fact_pedidos_itens.FK_VENDEDOR
            , fact_pedidos_itens.FK_PEDIDO
            , fact_pedidos_itens.FK_PRODUTO
            , fact_pedidos_itens.CD_TERRITORIO as cd_territorio_pedido
            , fact_pedidos_itens.CD_TRANSPORTADORA
            , fact_pedidos_itens.CD_CLIENTE
            , fact_pedidos_itens.CD_ENDERECO
            , fact_pedidos_itens.CD_CARTAO
            , fact_pedidos_itens.CD_VENDEDOR
            , fact_pedidos_itens.CD_OFERTA
            , fact_pedidos_itens.CD_PEDIDO
            , fact_pedidos_itens.CD_TP_PEDIDO
            , fact_pedidos_itens.CD_STATUS_PEDIDO
            , fact_pedidos_itens.CD_ITEM_PEDIDO
            , fact_pedidos_itens.NUMERO_REVISAO
            , fact_pedidos_itens.VALOR_NEGOCIADO
            , fact_pedidos_itens.VALOR_NEGOCIADO_LIQUIDO
            , fact_pedidos_itens.VALOR_TOTAL_ITEM
            , fact_pedidos_itens.TP_PEDIDO
            , fact_pedidos_itens.DT_PEDIDO
            , fact_pedidos_itens.STATUS_PEDIDO
            , fact_pedidos_itens.CD_PRODUTO_NUM
            , fact_pedidos_itens.PRECO_UNITARIO
            , fact_pedidos_itens.QTD_PRODUTO
            , fact_pedidos_itens.DESCONTO_PERC
            , fact_pedidos_itens.NM_TRANSPORTADORA
            , fact_pedidos_itens.TP_OFERTA
            , fact_pedidos_itens.DS_OFERTA
            , fact_pedidos_itens.CATEGORIA_OFERTA
            , fact_pedidos_itens.CHECK_SUBTOTAL_DISTRIBUIDO
            , fact_pedidos_itens.TAXAS_DISTRIBUIDAS
            , fact_pedidos_itens.TAXA_FRETE
            , fact_pedidos_itens.CHECK_FRETE_DISTRIBUIDO
            , fact_pedidos_itens.FRETE_BASE_DISTRIBUIDO
            , fact_pedidos_itens.CHECK_TOTAL_PEDIDO_DISTRIBUIDO
            , fact_pedidos_itens.DESCONTO_PERC_OFERTA
            , fact_pedidos_itens.DT_OFERTA_INICIAL
            , fact_pedidos_itens.DT_OFERTA_FINAL
            , fact_pedidos_itens.QTD_MIN_OFERTA_DISTRIBUIDA
            , fact_pedidos_itens.QTD_MAX_OFERTA_DISTRIBUIDA
            , dim_cartoes.NM_CARTAO
            , dim_cartoes.CD_PESSOA_CARTAO
            , dim_cartoes.TP_PESSOA_CARTAO
            , dim_cartoes.CD_TP_PESSOA_CARTAO
            , dim_cartoes.NM_PESSOA_CARTAO
            , dim_clientes.CD_LOJA
            , dim_clientes.CD_PESSOA
            , dim_clientes.CD_VENDEDOR_ATRIBUIDO
            , dim_clientes.TP_CLIENTE
            , dim_clientes.NM_VENDEDOR_ATRIBUIDO
            , dim_clientes.NM_CLIENTE
            , dim_enderecos.CD_PAIS
            , dim_enderecos.CD_ESTADO
            , dim_enderecos.CD_TERRITORIO
            , dim_enderecos.CD_CIDADE
            , dim_enderecos.CD_TP_ENDERECO
            , dim_enderecos.NM_PAIS
            , dim_enderecos.NM_ESTADO
            , dim_enderecos.CIDADE
            , dim_enderecos.TP_ENDERECO
            , dim_enderecos.SN_PAIS_SEM_ESTADO
            , dim_enderecos.LATITUDE
            , dim_enderecos.LONGITUDE
            , dim_enderecos.TP_CIDADE
            , dim_produtos.CD_PRODUTO_CATEGORIA
            , dim_produtos.CD_PRODUTO_SUBCATEGORIA
            , dim_produtos.CD_PRODUTO_MODELO
            , dim_produtos.CD_PRODUTO
            , dim_produtos.NM_CATEGORIA_PRODUTO
            , dim_produtos.NM_SUBCATEGORIA_PRODUTO
            , dim_produtos.LINHA_DO_PRODUTO
            , dim_produtos.NM_MODELO
            , dim_produtos.NM_PRODUTO
            , dim_produtos.DESCRICAO
            , dim_produtos.PRODUTO_COR
            , dim_produtos.PRODUTO_TAMANHO
            , dim_produtos.PRODUTO_PESO
            , dim_produtos.PRODUTO_DIAS_FABRICACAO
            , dim_produtos.PRODUTO_ESTILO
            , dim_produtos.SN_PODE_SER_VENDIDO
            , dim_produtos.SN_FABRICADO_PELA_EMPRESA
            , dim_vendedores.CD_DEPARTAMENTO
            , dim_vendedores.CD_TP_PESSOA_VENDEDOR
            , dim_vendedores.CD_TURNO
            , dim_vendedores.NM_GRUPO_DEPARTAMENTO
            , dim_vendedores.NM_DEPARTAMENTO
            , dim_vendedores.FUNCIONARIO_CARGO
            , dim_vendedores.DS_TURNO
            , dim_vendedores.TP_PESSOA
        from fact_pedidos_itens
        left join dim_cartoes
        on fact_pedidos_itens.FK_CARTAO = dim_cartoes.PK_CARTAO
        left join dim_clientes
        on fact_pedidos_itens.FK_CLIENTE = dim_clientes.PK_CLIENTE
        left join dim_enderecos
        on fact_pedidos_itens.FK_ENDERECO = dim_enderecos.PK_ENDERECO
        left join dim_produtos
        on fact_pedidos_itens.FK_PRODUTO = dim_produtos.PK_PRODUTO
        left join dim_vendedores
        on fact_pedidos_itens.FK_VENDEDOR = dim_vendedores.PK_VENDEDOR
    )

select *
from one_big_table