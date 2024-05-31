with
    src_erp__SALESORDERHEADER as (
        select *
        from {{ ref('src_erp__SALESORDERHEADER') }}
    )

    , src_erp__SHIPMETHOD as (
        select *
        from {{ ref('src_erp__SHIPMETHOD') }}
    )

    , int_clientes as (
        select 
                pk_cliente
                    as fk_cliente
                , fk_endereco
                , cd_cliente
                , cd_endereco
        from {{ ref('int_clientes') }}
    )

    , uniao_tabelas as (
        select
            src_erp__SALESORDERHEADER.SALESORDERID
            , src_erp__SALESORDERHEADER.REVISIONNUMBER
            , src_erp__SALESORDERHEADER.DT_PEDIDO
            , src_erp__SALESORDERHEADER.STATUS_PEDIDO
            , src_erp__SALESORDERHEADER.TP_PEDIDO
            , src_erp__SALESORDERHEADER.CUSTOMERID
            , src_erp__SALESORDERHEADER.SALESPERSONID
            , src_erp__SALESORDERHEADER.TERRITORYID
            , src_erp__SALESORDERHEADER.SHIPMETHODID
            , src_erp__SALESORDERHEADER.CREDITCARDID
            , src_erp__SALESORDERHEADER.CHECK_SUBTOTAL
            , src_erp__SALESORDERHEADER.TAXAS
            , src_erp__SALESORDERHEADER.FRETE
            , src_erp__SALESORDERHEADER.CHECK_TOTAL_PEDIDO
            , src_erp__SHIPMETHOD.NM_TRANSPORTADORA
            , src_erp__SHIPMETHOD.FRETE_BASE
            , src_erp__SHIPMETHOD.TAXA_FRETE
            , int_clientes.fk_cliente
            , int_clientes.fk_endereco
            , int_clientes.cd_endereco
        from src_erp__SALESORDERHEADER
        left join src_erp__SHIPMETHOD
        on src_erp__SALESORDERHEADER.SHIPMETHODID = src_erp__SHIPMETHOD.SHIPMETHODID 
        left join int_clientes
        on src_erp__SALESORDERHEADER.CUSTOMERID = int_clientes.cd_cliente
    )

    , chaves as (
        select
            hash(SALESORDERID)
                as pk_pedidos
            , FK_CLIENTE
            , FK_ENDERECO
            , hash(CREDITCARDID)
                as fk_cartao
            , hash(SALESPERSONID)
                as fk_vendedor
            , TERRITORYID
                as cd_territorio
            , SHIPMETHODID
                as cd_transportadora
            , CUSTOMERID
                as cd_cliente
            , CD_ENDERECO
            , SALESPERSONID
                as cd_vendedor
            , SALESORDERID
                as cd_pedido
            , REVISIONNUMBER
                as numero_revisao
            , DT_PEDIDO
            , STATUS_PEDIDO
            , TP_PEDIDO
            , CHECK_SUBTOTAL
            , TAXAS
                as taxas_pedido
            , FRETE
                as frete_pedido
            , CHECK_TOTAL_PEDIDO
            , NM_TRANSPORTADORA
            , FRETE_BASE
            , TAXA_FRETE
        from uniao_tabelas   
    )

select *
from chaves
