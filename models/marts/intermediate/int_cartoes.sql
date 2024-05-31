with
    src_erp__CREDITCARD as (
        select *
        from {{ ref('src_erp__CREDITCARD') }}
    )

    , src_erp__PERSONCREDITCARD as (
        select *
        from {{ ref('src_erp__PERSONCREDITCARD') }}
    )

    , src_erp__PERSON as (
        select *
        from {{ ref('src_erp__PERSON') }}
    )

    , uniao_tabelas as (
        select
            src_erp__CREDITCARD.CREDITCARDID
            , src_erp__CREDITCARD.TP_CARTAO
            , src_erp__PERSONCREDITCARD.BUSINESSENTITYID
            , src_erp__PERSON.TP_PESSOA
            , src_erp__PERSON.CD_TP_PESSOA
            , src_erp__PERSON.NM_PESSOA
        from src_erp__CREDITCARD
        left join src_erp__PERSONCREDITCARD
        on src_erp__CREDITCARD.CREDITCARDID = src_erp__PERSONCREDITCARD.CREDITCARDID
        left join src_erp__PERSON
        on src_erp__PERSONCREDITCARD.BUSINESSENTITYID = src_erp__PERSON.BUSINESSENTITYID
    )

    , chaves as (
        select
            hash(CREDITCARDID)
                as pk_cartao
            , CREDITCARDID
                as cd_cartao
            , TP_CARTAO
            , BUSINESSENTITYID
                as cd_pessoa_cartao
            , TP_PESSOA
                as tp_pessoa_cartao
            , CD_TP_PESSOA
                as cd_tp_pessoa_cartao
            , NM_PESSOA
                as nm_pessoa_cartao
        from uniao_tabelas
    )

select *
from chaves