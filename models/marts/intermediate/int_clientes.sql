with
    src_erp__CUSTOMER as (
        select *
        from {{ ref('src_erp__CUSTOMER') }}
    )

    , src_erp__PERSON as (
        select *
        from {{ ref('src_erp__PERSON') }}
    )

    , src_erp__BUSINESSENTITYADDRESS as (
        select *
        from {{ ref('src_erp__BUSINESSENTITYADDRESS') }}
    )

    , src_erp__STORE as (
        select *
        from {{ ref('src_erp__STORE') }}
    )

    , src_erp__BUSINESSENTITYCONTACT as (
        select *
        from {{ ref('src_erp__BUSINESSENTITYCONTACT') }}
    )

    , src_erp__CONTACTTYPE as (
        select *
        from {{ ref('src_erp__CONTACTTYPE') }}
    )


    , cliente_pessoa as (
        select
            src_erp__CUSTOMER.CUSTOMERID
            , src_erp__CUSTOMER.STOREID    
            , src_erp__CUSTOMER.PERSONID    
            , 'Individual (varejo)'
                as TP_CLIENTE
            , src_erp__PERSON.BUSINESSENTITYID
            , src_erp__PERSON.NM_PESSOA
                as NM_LOJA
            , null
                as cd_vendedor_atribuido
            , null
                as nm_vendedor_atribuido
            , src_erp__PERSON.TP_PESSOA
            , src_erp__PERSON.CD_TP_PESSOA
            , src_erp__PERSON.NM_PESSOA
            , null
                as TP_CONTATO
            , src_erp__BUSINESSENTITYADDRESS.ADDRESSID
        from src_erp__CUSTOMER
        inner join src_erp__PERSON
        on src_erp__CUSTOMER.PERSONID = src_erp__PERSON.BUSINESSENTITYID
        inner join src_erp__BUSINESSENTITYADDRESS
        on src_erp__PERSON.BUSINESSENTITYID = src_erp__BUSINESSENTITYADDRESS.BUSINESSENTITYID

    )

    , cliente_loja as (
        select
            src_erp__CUSTOMER.CUSTOMERID
            , src_erp__CUSTOMER.STOREID
            , src_erp__BUSINESSENTITYCONTACT.PERSONID
            , 'Empresarial (loja)'
                as TP_CLIENTE
            , src_erp__STORE.BUSINESSENTITYID
            , src_erp__STORE.NM_LOJA
            , src_erp__STORE.SALESPERSONID
                as cd_vendedor_atribuido
            , src_erp__PERSON_vendedor.nm_pessoa
                as nm_vendedor_atribuido
            , src_erp__PERSON.TP_PESSOA
            , src_erp__PERSON.CD_TP_PESSOA
            , src_erp__PERSON.NM_PESSOA
            , src_erp__CONTACTTYPE.TP_CONTATO
            , src_erp__BUSINESSENTITYADDRESS.ADDRESSID
        from src_erp__CUSTOMER
        inner join src_erp__STORE
        on src_erp__CUSTOMER.STOREID = src_erp__STORE.BUSINESSENTITYID
        inner join src_erp__BUSINESSENTITYCONTACT
        on src_erp__STORE.BUSINESSENTITYID = src_erp__BUSINESSENTITYCONTACT.BUSINESSENTITYID
        inner join src_erp__PERSON
        on src_erp__BUSINESSENTITYCONTACT.PERSONID = src_erp__PERSON.BUSINESSENTITYID
        inner join src_erp__CONTACTTYPE
        on src_erp__BUSINESSENTITYCONTACT.CONTACTTYPEID = src_erp__CONTACTTYPE.CONTACTTYPEID
        inner join src_erp__BUSINESSENTITYADDRESS
        on src_erp__CUSTOMER.STOREID = src_erp__BUSINESSENTITYADDRESS.BUSINESSENTITYID
        left join src_erp__PERSON as src_erp__PERSON_vendedor
        on src_erp__STORE.SALESPERSONID = src_erp__PERSON_vendedor.BUSINESSENTITYID
        where src_erp__CUSTOMER.PERSONID is null
    )

    , cliente_loja_pessoa as (
        select
            src_erp__CUSTOMER.CUSTOMERID
            , src_erp__CUSTOMER.STOREID
            , src_erp__CUSTOMER.PERSONID
            , 'Empresarial (Pessoa)'
                as TP_CLIENTE
            , src_erp__STORE.BUSINESSENTITYID
            , src_erp__STORE.NM_LOJA
            , src_erp__STORE.SALESPERSONID
                as cd_vendedor_atribuido
            , src_erp__PERSON_vendedor.nm_pessoa
                as nm_vendedor_atribuido
            , src_erp__PERSON.TP_PESSOA
            , src_erp__PERSON.CD_TP_PESSOA
            , src_erp__PERSON.NM_PESSOA
            , src_erp__CONTACTTYPE.TP_CONTATO
            , src_erp__BUSINESSENTITYADDRESS.ADDRESSID 
        from src_erp__CUSTOMER
        inner join src_erp__STORE
        on src_erp__CUSTOMER.STOREID = src_erp__STORE.BUSINESSENTITYID
        inner join src_erp__BUSINESSENTITYCONTACT
        on src_erp__STORE.BUSINESSENTITYID = src_erp__BUSINESSENTITYCONTACT.BUSINESSENTITYID
        inner join src_erp__PERSON
        on src_erp__CUSTOMER.PERSONID = src_erp__PERSON.BUSINESSENTITYID
        inner join src_erp__CONTACTTYPE
        on src_erp__BUSINESSENTITYCONTACT.CONTACTTYPEID = src_erp__CONTACTTYPE.CONTACTTYPEID
        inner join src_erp__BUSINESSENTITYADDRESS
        on src_erp__CUSTOMER.STOREID = src_erp__BUSINESSENTITYADDRESS.BUSINESSENTITYID
        left join src_erp__PERSON as src_erp__PERSON_vendedor
        on src_erp__STORE.SALESPERSONID = src_erp__PERSON_vendedor.BUSINESSENTITYID
        where src_erp__CUSTOMER.PERSONID is not null
    )

    , uniao_tabelas as (
        select *
        from cliente_pessoa
        union all
        select *
        from cliente_loja
        union all
        select *
        from cliente_loja_pessoa
    )
    
    , chaves as (
        select
            hash(CUSTOMERID)
                as pk_cliente
            , hash(ADDRESSID)
                as fk_endereco
            , CUSTOMERID
                as cd_cliente
            , ADDRESSID
                as cd_endereco
            , STOREID
                as cd_loja
            , PERSONID
                as cd_pessoa
            , CD_TP_PESSOA
            , BUSINESSENTITYID
                as cd_entidade_negocio
            , CD_VENDEDOR_ATRIBUIDO
            , NM_VENDEDOR_ATRIBUIDO
            , NM_LOJA
                as nm_cliente_loja
            , TP_CLIENTE
            , NM_PESSOA
                as nm_cliente_pessoa
            , TP_PESSOA
                as tp_pessoa_cliente
            , TP_CONTATO
        from uniao_tabelas
    )

select *
from chaves