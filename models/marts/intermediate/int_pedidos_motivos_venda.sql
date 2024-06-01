with
    src_erp__SALESORDERHEADERSALESREASON as (
        select *
        from {{ ref('src_erp__SALESORDERHEADERSALESREASON') }}
    )

    , src_erp__SALESREASON as (
        select *
        from {{ ref('src_erp__SALESREASON') }}
    )

    , uniao_tabelas as (
        select
            src_erp__SALESORDERHEADERSALESREASON.SALESORDERID
            , src_erp__SALESORDERHEADERSALESREASON.SALESREASONID
            , src_erp__SALESREASON.DS_MOTIVO_VENDA
            , src_erp__SALESREASON.TP_MOTIVO_VENDA
        from src_erp__SALESORDERHEADERSALESREASON
        left join src_erp__SALESREASON
        on src_erp__SALESORDERHEADERSALESREASON.SALESREASONID = src_erp__SALESREASON.SALESREASONID
    )

    , chaves as (
        select
            hash(concat(SALESORDERID,'|',SALESREASONID)) as pk_pedido_motivo_venda
            , hash(SALESORDERID) as fk_pedido
            , concat(SALESORDERID,'|',SALESREASONID) as chave_pedido_motivo_venda
            , SALESORDERID as cd_pedido
            , SALESREASONID as cd_motivo_venda
            , DS_MOTIVO_VENDA
            , TP_MOTIVO_VENDA
        from uniao_tabelas
    )

select *
from chaves
