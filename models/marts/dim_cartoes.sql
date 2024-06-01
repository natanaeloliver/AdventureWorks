with
    int_cartoes as (
        select *
        from {{ ref('int_cartoes') }}
    )

    , tabela as (
        select
            PK_CARTAO
            , CD_CARTAO
            , NM_CARTAO
            , CD_PESSOA_CARTAO
            , TP_PESSOA_CARTAO
            , CD_TP_PESSOA_CARTAO
            , NM_PESSOA_CARTAO
        from int_cartoes
    )

select *
from tabela