with
    src_erp__CUSTOMER as (
        select *
        from {{ ref('src_erp__CUSTOMER') }}
    )

    , src_erp__PERSON as (
        select *
        from {{ ref('src_erp__PERSON') }}
    )

    , src_erp__STORE as (
        select *
        from {{ ref('src_erp__STORE') }}
    )

    , src_erp__BUSINESSENTITYADDRESS as (
        select *
        from {{ ref('src_erp__BUSINESSENTITYADDRESS') }}
    )

    , cliente_pessoa as (
        select
            src_erp__CUSTOMER.CUSTOMERID
            , 0 as STOREID    
            , src_erp__CUSTOMER.PERSONID
            , 'Individual (varejo)'
                as TP_CLIENTE
            , src_erp__PERSON.NM_PESSOA
                as NM_LOJA
            , 0
                as cd_vendedor_atribuido
            , 'Venda feita pelo cliente'
                as nm_vendedor_atribuido
        from src_erp__CUSTOMER
        inner join src_erp__PERSON
        on src_erp__CUSTOMER.PERSONID = src_erp__PERSON.BUSINESSENTITYID
        left join src_erp__PERSONCREDITCARD
        on src_erp__CUSTOMER.PERSONID = src_erp__PERSONCREDITCARD.BUSINESSENTITYID 
        where src_erp__CUSTOMER.STOREID is null
    )

    , cliente_loja as (
        select
            src_erp__CUSTOMER.CUSTOMERID
            , src_erp__CUSTOMER.STOREID
            , 0 as PERSONID
            , 'Empresarial (loja)'
                as TP_CLIENTE
            , src_erp__STORE.NM_LOJA
            , case
                when src_erp__STORE.SALESPERSONID is null then 0
                else src_erp__STORE.SALESPERSONID
            end as cd_vendedor_atribuido
            , case
                when src_erp__PERSON_vendedor.nm_pessoa is null then 'Venda feita pelo cliente'
                else src_erp__PERSON_vendedor.nm_pessoa
            end as nm_vendedor_atribuido
        from src_erp__CUSTOMER
        inner join src_erp__STORE
        on src_erp__CUSTOMER.STOREID = src_erp__STORE.BUSINESSENTITYID
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
            , src_erp__STORE.NM_LOJA
            , case
                when src_erp__STORE.SALESPERSONID is null then 0
                else src_erp__STORE.SALESPERSONID
            end as cd_vendedor_atribuido
            , case
                when src_erp__PERSON_vendedor.nm_pessoa is null then 'Venda feita pelo cliente'
                else src_erp__PERSON_vendedor.nm_pessoa
            end as nm_vendedor_atribuido
        from src_erp__CUSTOMER
        inner join src_erp__STORE
        on src_erp__CUSTOMER.STOREID = src_erp__STORE.BUSINESSENTITYID
        inner join src_erp__PERSON
        on src_erp__CUSTOMER.PERSONID = src_erp__PERSON.BUSINESSENTITYID
        left join src_erp__PERSON as src_erp__PERSON_vendedor
        on src_erp__STORE.SALESPERSONID = src_erp__PERSON_vendedor.BUSINESSENTITYID
        where src_erp__CUSTOMER.PERSONID is not null
        and src_erp__CUSTOMER.STOREID is not null
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
            , CUSTOMERID
                as cd_cliente
            , STOREID
                as cd_loja
            , PERSONID
                as cd_pessoa
            , CD_VENDEDOR_ATRIBUIDO
            , TP_CLIENTE
            , NM_VENDEDOR_ATRIBUIDO
            , NM_LOJA as nm_cliente
        from uniao_tabelas
    )
select *
from chaves
