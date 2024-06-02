with
    src_erp__SALESORDERDETAIL as (
        select *
        from {{ ref('src_erp__SALESORDERDETAIL') }}
    )

    , src_erp__SALESORDERHEADER as (
        select *
        from {{ ref('src_erp__SALESORDERHEADER') }}
    )

    , src_erp__SPECIALOFFERPRODUCT as (
        select *
        from {{ ref('src_erp__SPECIALOFFERPRODUCT') }}
    )

    , src_erp__SPECIALOFFER as (
        select *
        from {{ ref('src_erp__SPECIALOFFER') }}
    )

    , src_erp__SHIPMETHOD as (
        select *
        from {{ ref('src_erp__SHIPMETHOD') }}
    )

    , int_clientes as (
        select *
        from {{ ref('int_clientes') }}
    )

    , pedidos_chave_cliente as (
        select
            src_erp__SALESORDERHEADER.*
            , case
                when CREDITCARDID is null then concat(CUSTOMERID,'|0')
                else concat(CUSTOMERID,'|',CREDITCARDID)
            end as chave_cliente_cartao
        from src_erp__SALESORDERHEADER
    )

    , tabela_qtd_itens_pedido as (
        select distinct
            SALESORDERID
            , count(SALESORDERDETAILID) as qtd_itens_pedido
        from src_erp__SALESORDERDETAIL
        group by SALESORDERID
    )

    , chave_oferta_item_pedido as (
        select
            src_erp__SALESORDERDETAIL.*
            , concat(src_erp__SALESORDERDETAIL.SPECIALOFFERID,'|',src_erp__SALESORDERDETAIL.PRODUCTID) as chave_oferta_produto
        from src_erp__SALESORDERDETAIL
    )

    , chave_oferta_produto_tabela as (
        select
            src_erp__SPECIALOFFERPRODUCT.*
            , concat(src_erp__SPECIALOFFERPRODUCT.SPECIALOFFERID,'|',src_erp__SPECIALOFFERPRODUCT.PRODUCTID) as chave_oferta_produto
        from src_erp__SPECIALOFFERPRODUCT
    )

    , uniao_tabelas as (
        select
            chave_oferta_item_pedido.SALESORDERID
            , chave_oferta_item_pedido.SALESORDERDETAILID
            , chave_oferta_item_pedido.QTD_PRODUTO
            , chave_oferta_item_pedido.PRODUCTID
            , chave_oferta_item_pedido.SPECIALOFFERID
            , chave_oferta_item_pedido.PRECO_UNITARIO
            , chave_oferta_item_pedido.DESCONTO_PERC
            , pedidos_chave_cliente.REVISIONNUMBER
            , pedidos_chave_cliente.DT_PEDIDO
            , pedidos_chave_cliente.STATUS_PEDIDO
            , pedidos_chave_cliente.CD_STATUS_PEDIDO
            , pedidos_chave_cliente.TP_PEDIDO
            , pedidos_chave_cliente.CD_TP_PEDIDO
            , pedidos_chave_cliente.CUSTOMERID
            , case
                when pedidos_chave_cliente.SALESPERSONID is null then 0
                else pedidos_chave_cliente.SALESPERSONID
            end as SALESPERSONID
            , pedidos_chave_cliente.TERRITORYID
            , pedidos_chave_cliente.SHIPMETHODID
            , pedidos_chave_cliente.ADDRESSID
            , case
                when pedidos_chave_cliente.CREDITCARDID is null then 0
                else pedidos_chave_cliente.CREDITCARDID
            end as CREDITCARDID
            , pedidos_chave_cliente.CHECK_SUBTOTAL
            , pedidos_chave_cliente.TAXAS
            , pedidos_chave_cliente.FRETE
            , pedidos_chave_cliente.CHECK_TOTAL_PEDIDO
            , pedidos_chave_cliente.CHAVE_CLIENTE_CARTAO
            , src_erp__SPECIALOFFER.DS_OFERTA
            , src_erp__SPECIALOFFER.DESCONTO_PERC
                as DESCONTO_PERC_OFERTA
            , src_erp__SPECIALOFFER.TP_OFERTA
            , src_erp__SPECIALOFFER.CATEGORIA_OFERTA
            , src_erp__SPECIALOFFER.DT_OFERTA_INICIAL
            , src_erp__SPECIALOFFER.DT_OFERTA_FINAL
            , src_erp__SPECIALOFFER.QTD_MIN
            , src_erp__SPECIALOFFER.QTD_MAX
            , src_erp__SHIPMETHOD.NM_TRANSPORTADORA
            , src_erp__SHIPMETHOD.FRETE_BASE
            , src_erp__SHIPMETHOD.TAXA_FRETE
            , tabela_qtd_itens_pedido.qtd_itens_pedido
            , int_clientes.CD_PESSOA
            , int_clientes.CD_LOJA
            , int_clientes.CD_VENDEDOR_ATRIBUIDO
            , int_clientes.TP_CLIENTE
        from chave_oferta_item_pedido
        left join pedidos_chave_cliente
        on chave_oferta_item_pedido.SALESORDERID = pedidos_chave_cliente.SALESORDERID
        left join chave_oferta_produto_tabela
        on chave_oferta_item_pedido.CHAVE_OFERTA_PRODUTO = chave_oferta_produto_tabela.CHAVE_OFERTA_PRODUTO
        left join src_erp__SPECIALOFFER
        on chave_oferta_item_pedido.SPECIALOFFERID = src_erp__SPECIALOFFER.SPECIALOFFERID
        left join src_erp__SHIPMETHOD
        on pedidos_chave_cliente.SHIPMETHODID = src_erp__SHIPMETHOD.SHIPMETHODID 
        left join int_clientes
        on pedidos_chave_cliente.CHAVE_CLIENTE_CARTAO = int_clientes.chave_cliente_cartao
        left join tabela_qtd_itens_pedido
        on pedidos_chave_cliente.SALESORDERID = tabela_qtd_itens_pedido.SALESORDERID
    )

    , chaves_e_distribuicao_valores as (
        select
            hash(SALESORDERDETAILID)
                as pk_item_pedido
            , hash(concat(CUSTOMERID,'|',CREDITCARDID))
                as fk_cliente
            , case 
                when TP_CLIENTE = 'Individual (varejo)' then hash(concat(CD_PESSOA,'|',ADDRESSID))
                else hash(concat(CD_LOJA,'|',ADDRESSID))
            end as fk_endereco
            , hash(SALESORDERID)
                as fk_pedido
            , hash(CREDITCARDID)
                as fk_cartao
            , hash(SALESPERSONID)
                as fk_vendedor
            , hash(PRODUCTID)
                as fk_produto
            , TERRITORYID
                as cd_territorio
            , SHIPMETHODID
                as cd_transportadora
            , CUSTOMERID
                as cd_cliente
            , CD_PESSOA
            , ADDRESSID
                as cd_endereco
            , CHAVE_CLIENTE_CARTAO
            , case 
                when TP_CLIENTE = 'Individual (varejo)' then concat(CD_PESSOA,'|',ADDRESSID)
                else concat(CD_LOJA,'|',ADDRESSID)
            end as chave_pessoa_endereco
            , CREDITCARDID
                as cd_cartao
            , SALESPERSONID
                as cd_vendedor
            , SPECIALOFFERID
                as cd_oferta
            , SALESORDERID
                as cd_pedido
            , CD_TP_PEDIDO
            , CD_STATUS_PEDIDO
            , SALESORDERDETAILID
                as cd_item_pedido
            , REVISIONNUMBER
                as numero_revisao
            , TP_PEDIDO
            , DT_PEDIDO
            , STATUS_PEDIDO
            , PRODUCTID
                as cd_produto_num
            , PRECO_UNITARIO
            , QTD_PRODUTO
            , DESCONTO_PERC
            , NM_TRANSPORTADORA
            , TP_OFERTA
            , DS_OFERTA
            , CATEGORIA_OFERTA
            , CHECK_SUBTOTAL/QTD_ITENS_PEDIDO
                as check_subtotal_distribuido
            , TAXAS/QTD_ITENS_PEDIDO
                as taxas_distribuidas
            , TAXA_FRETE
            , FRETE/QTD_ITENS_PEDIDO
                as check_frete_distribuido
            , FRETE_BASE/QTD_ITENS_PEDIDO
                as frete_base_distribuido
            , CHECK_TOTAL_PEDIDO/QTD_ITENS_PEDIDO
                as check_total_pedido_distribuido
            , DESCONTO_PERC_OFERTA
            , DT_OFERTA_INICIAL
            , DT_OFERTA_FINAL
            , QTD_MIN/QTD_ITENS_PEDIDO
                as qtd_min_oferta_distribuida
            , QTD_MAX/QTD_ITENS_PEDIDO
                as qtd_max_oferta_distribuida
        from uniao_tabelas
    )

select *
from chaves_e_distribuicao_valores