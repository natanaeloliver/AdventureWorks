with
    src_erp__SALESORDERHEADER as (
        select *
        from {{ ref('src_erp__SALESORDERHEADER') }}
    )

    , src_erp__SHIPMETHOD as (
        select *
        from {{ ref('src_erp__SHIPMETHOD') }}
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
            , src_erp__SALESORDERHEADER.ADDRESSID
            , src_erp__SALESORDERHEADER.CREDITCARDID
            , src_erp__SALESORDERHEADER.CHECK_SUBTOTAL
            , src_erp__SALESORDERHEADER.TAXAS
            , src_erp__SALESORDERHEADER.FRETE
            , src_erp__SALESORDERHEADER.CHECK_TOTAL_PEDIDO
            , src_erp__SHIPMETHOD.NM_TRANSPORTADORA
            , src_erp__SHIPMETHOD.FRETE_BASE
            , src_erp__SHIPMETHOD.TAXA_FRETE
        from src_erp__SALESORDERHEADER
        left join src_erp__SHIPMETHOD
        on src_erp__SALESORDERHEADER.SHIPMETHODID = src_erp__SHIPMETHOD.SHIPMETHODID 

    )

    , chaves as (
        select
            hash(SALESORDERID)
                as pk_pedido
            , hash(CUSTOMERID)
                as fk_cliente
            , hash(concat(CUSTOMERID,'|',ADDRESSID))
                as fk_endereco
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
            , ADDRESSID
                as cd_endereco
            , concat(CUSTOMERID,'|',ADDRESSID)
                as chave_pessoa_endereco
            , CREDITCARDID
                as cd_cartao
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