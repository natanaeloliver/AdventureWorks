with
    src_erp__CUSTOMER as (
        select 
            src_erp__CUSTOMER.*
            , concat(STOREID,'|',PERSONID)
                as chave_loja_pessoa
        from {{ ref('src_erp__CUSTOMER') }}
    )

    , src_erp__BUSINESSENTITYCONTACT as (
        select 
            src_erp__BUSINESSENTITYCONTACT.*
            , concat(BUSINESSENTITYID,'|',PERSONID)
                as chave_loja_pessoa
        from {{ ref('src_erp__BUSINESSENTITYCONTACT') }}
    )

    , src_erp__CONTACTTYPE as (
        select *
        from {{ ref('src_erp__CONTACTTYPE') }}
    )

    , src_erp__PERSON as (
        select *
        from {{ ref('src_erp__PERSON') }}
    )

    , src_erp__PERSONCREDITCARD as (
        select *
        from {{ ref('src_erp__PERSONCREDITCARD') }}
    )

    , src_erp__STORE as (
        select *
        from {{ ref('src_erp__STORE') }}
    )

    , contato_loja_pessoa as (
        select
            src_erp__CUSTOMER.CUSTOMERID
            , src_erp__CUSTOMER.PERSONID
            , src_erp__CUSTOMER.STOREID
            , src_erp__CUSTOMER.TERRITORYID
            , src_erp__CUSTOMER.CHAVE_LOJA_PESSOA
            , src_erp__BUSINESSENTITYCONTACT.CONTACTTYPEID
            , src_erp__CONTACTTYPE.TP_CONTATO
            , src_erp__PERSON.TP_PESSOA
                as tp_pessoa_contato
            , src_erp__PERSON.CD_TP_PESSOA
                as cd_tp_pessoa_contato
            , src_erp__PERSON.NM_PESSOA
                as nm_contato
            , case
                when src_erp__PERSONCREDITCARD.CREDITCARDID is null then 0
                else src_erp__PERSONCREDITCARD.CREDITCARDID
            end as CREDITCARDID
        from src_erp__CUSTOMER
        left join src_erp__BUSINESSENTITYCONTACT
        on src_erp__CUSTOMER.CHAVE_LOJA_PESSOA = src_erp__BUSINESSENTITYCONTACT.CHAVE_LOJA_PESSOA
        left join src_erp__CONTACTTYPE
        on src_erp__BUSINESSENTITYCONTACT.CONTACTTYPEID = src_erp__CONTACTTYPE.CONTACTTYPEID
        left join src_erp__PERSON
        on src_erp__CUSTOMER.PERSONID = src_erp__PERSON.BUSINESSENTITYID
        left join src_erp__PERSONCREDITCARD
        on src_erp__CUSTOMER.STOREID = src_erp__PERSONCREDITCARD.BUSINESSENTITYID
        where src_erp__CUSTOMER.CHAVE_LOJA_PESSOA is not null
    )

    , contato_loja_empresa as (
        select
            src_erp__CUSTOMER.CUSTOMERID
            , 0 as PERSONID
            , src_erp__CUSTOMER.STOREID
            , src_erp__CUSTOMER.TERRITORYID
            , concat(STOREID,'|0') as CHAVE_LOJA_PESSOA
            , src_erp__BUSINESSENTITYCONTACT.CONTACTTYPEID
            , src_erp__CONTACTTYPE.TP_CONTATO
            , 'Loja' as tp_pessoa_contato
            , 0 as cd_tp_pessoa_contato
            , src_erp__STORE.NM_LOJA
                as nm_contato
            , case
                when src_erp__PERSONCREDITCARD.CREDITCARDID is null then 0
                else src_erp__PERSONCREDITCARD.CREDITCARDID
            end as CREDITCARDID
        from src_erp__CUSTOMER
        left join src_erp__BUSINESSENTITYCONTACT
        on src_erp__CUSTOMER.STOREID = src_erp__BUSINESSENTITYCONTACT.BUSINESSENTITYID
        left join src_erp__CONTACTTYPE
        on src_erp__BUSINESSENTITYCONTACT.CONTACTTYPEID = src_erp__CONTACTTYPE.CONTACTTYPEID
        left join src_erp__PERSON
        on src_erp__CUSTOMER.STOREID = src_erp__PERSON.BUSINESSENTITYID
        left join src_erp__PERSONCREDITCARD
        on src_erp__CUSTOMER.STOREID = src_erp__PERSONCREDITCARD.BUSINESSENTITYID
        left join src_erp__STORE
        on src_erp__CUSTOMER.STOREID = src_erp__STORE.BUSINESSENTITYID
        where src_erp__CUSTOMER.PERSONID is null
    )

    , contato_individual as (
        select
            src_erp__CUSTOMER.CUSTOMERID
            , src_erp__CUSTOMER.PERSONID
            , 0 as STOREID
            , src_erp__CUSTOMER.TERRITORYID
            , concat('0|',PERSONID) as CHAVE_LOJA_PESSOA
            , 0 as CONTACTTYPEID
            , 'Individual Customer' as tp_contato
            , src_erp__PERSON.TP_PESSOA
                as tp_pessoa_contato
            , src_erp__PERSON.CD_TP_PESSOA
                as cd_tp_pessoa_contato
            , src_erp__PERSON.NM_PESSOA
                as nm_contato
            , case
                when src_erp__PERSONCREDITCARD.CREDITCARDID is null then 0
                else src_erp__PERSONCREDITCARD.CREDITCARDID
            end as CREDITCARDID
        from src_erp__CUSTOMER
        left join src_erp__PERSON
        on src_erp__CUSTOMER.PERSONID = src_erp__PERSON.BUSINESSENTITYID
        left join src_erp__PERSONCREDITCARD
        on src_erp__CUSTOMER.PERSONID = src_erp__PERSONCREDITCARD.BUSINESSENTITYID
        where src_erp__CUSTOMER.STOREID is null
    )

    , uniao_tabelas as (
        select *
        from contato_loja_pessoa
        union all
        select *
        from contato_loja_empresa
        union all
        select *
        from contato_individual
    )

    , chaves as (
        select
            hash(concat(CUSTOMERID,'|',PERSONID,'|',STOREID))
                as pk_contato
            , hash(concat(CUSTOMERID,'|',CREDITCARDID))
                as fk_cliente
            , TERRITORYID
            , CUSTOMERID
            , STOREID
            , PERSONID
            , CREDITCARDID
            , CONTACTTYPEID
            , CHAVE_LOJA_PESSOA
            , concat(CUSTOMERID,'|',PERSONID,'|',STOREID)
                as chave_cliente_pessoa_loja
            , concat(CUSTOMERID,'|',CREDITCARDID)
                as chave_cliente_cartao
            , TP_CONTATO
            , CD_TP_PESSOA_CONTATO
            , TP_PESSOA_CONTATO
            , NM_CONTATO
        from uniao_tabelas
    )

select *
from chaves