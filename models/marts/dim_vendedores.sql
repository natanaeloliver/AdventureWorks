with
    int_vendedores as (
        select *
        from {{ ref('int_vendedores') }}
    )

    , tabela as (
        select
            PK_VENDEDOR
            , CD_ENTIDADE_NEGOCIO
            , CD_DEPARTAMENTO
            , CD_TP_PESSOA_VENDEDOR
            , CD_TURNO
            , NM_GRUPO_DEPARTAMENTO
            , NM_DEPARTAMENTO
            , FUNCIONARIO_CARGO
            , DS_TURNO
            , TP_PESSOA
        from int_vendedores
    )

select *
from tabela