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

    , uniao_tabelas as (
        select
            pedidos_chave_cliente.SALESORDERID
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
            , src_erp__SHIPMETHOD.NM_TRANSPORTADORA
            , src_erp__SHIPMETHOD.FRETE_BASE
            , src_erp__SHIPMETHOD.TAXA_FRETE
            , int_clientes.CD_PESSOA
            , int_clientes.CD_LOJA
            , int_clientes.CD_VENDEDOR_ATRIBUIDO
            , int_clientes.TP_CLIENTE
        from pedidos_chave_cliente
        left join src_erp__SHIPMETHOD
        on pedidos_chave_cliente.SHIPMETHODID = src_erp__SHIPMETHOD.SHIPMETHODID
        left join int_clientes
        on pedidos_chave_cliente.CHAVE_CLIENTE_CARTAO = int_clientes.chave_cliente_cartao
        left join int_cartoes
        on pedidos_chave_cliente.CREDITCARDID = int_cartoes.CD_CARTAO

    )

    , chaves as (
        select
            hash(SALESORDERID)
                as pk_pedido
            , hash(concat(CUSTOMERID,'|',CREDITCARDID))
                as fk_cliente
            , case 
                when TP_CLIENTE = 'Individual (varejo)' then hash(concat(CD_PESSOA,'|',ADDRESSID))
                else hash(concat(CD_LOJA,'|',ADDRESSID))
            end as fk_endereco
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
            , CD_PESSOA
            , CD_VENDEDOR_ATRIBUIDO
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
            , SALESORDERID
                as cd_pedido
            , CD_TP_PEDIDO
            , CD_STATUS_PEDIDO
            , REVISIONNUMBER
                as numero_revisao
            , TP_PEDIDO
            , DT_PEDIDO
            , STATUS_PEDIDO
            , CHECK_SUBTOTAL
            , TAXAS
                as taxas_pedido
            , FRETE
                as frete_pedido
            , CHECK_TOTAL_PEDIDO
            , NM_TRANSPORTADORA
            , FRETE_BASE
            , TAXA_FRETE
            , case
                when SALESPERSONID = CD_VENDEDOR_ATRIBUIDO then true
                else false
            end as sn_venda_vendedor_atribuido
        from uniao_tabelas   
    )

select *
from chaves
where cd_cliente = 29484