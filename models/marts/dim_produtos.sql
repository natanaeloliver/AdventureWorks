with
    int_produtos as (
        select *
        from {{ ref('int_produtos') }}
    )

    , tabela as (
        select
            PK_PRODUTO
            , CD_PRODUTO_NUM
            , CD_PRODUTO_CATEGORIA
            , CD_PRODUTO_SUBCATEGORIA
            , CD_PRODUTO_MODELO
            , CD_PRODUTO
            , NM_CATEGORIA_PRODUTO
            , NM_SUBCATEGORIA_PRODUTO
            , LINHA_DO_PRODUTO
            , NM_MODELO
            , NM_PRODUTO
            , DESCRICAO
            , PRODUTO_COR
            , PRODUTO_TAMANHO
            , PRODUTO_PESO
            , PRODUTO_DIAS_FABRICACAO
            , PRODUTO_ESTILO
            , SN_PODE_SER_VENDIDO
            , SN_FABRICADO_PELA_EMPRESA
        from int_produtos
    )

select *
from tabela