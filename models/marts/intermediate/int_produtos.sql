with
    src_erp__PRODUCT as (
        select *
        from {{ ref('src_erp__PRODUCT') }}
    )

    , src_erp__PRODUCTSUBCATEGORY as (
        select *
        from {{ ref('src_erp__PRODUCTSUBCATEGORY') }}
    )

    , src_erp__PRODUCTCATEGORY as (
        select *
        from {{ ref('src_erp__PRODUCTCATEGORY') }}
    )

    , src_erp__PRODUCTMODEL as (
        select *
        from {{ ref('src_erp__PRODUCTMODEL') }}
    )

    , uniao_tabelas as (
        select
            src_erp__PRODUCT.PRODUCTID
            , src_erp__PRODUCT.NM_PRODUTO
            , src_erp__PRODUCT.CD_PRODUTO
            , src_erp__PRODUCT.SN_FABRICADO_PELA_EMPRESA
            , src_erp__PRODUCT.SN_PODE_SER_VENDIDO
            , src_erp__PRODUCT.PRODUTO_COR
            , src_erp__PRODUCT.PRODUTO_TAMANHO
            , src_erp__PRODUCT.PRODUTO_PESO
            , src_erp__PRODUCT.PRODUTO_DIAS_FABRICACAO
            , src_erp__PRODUCT.LINHA_DO_PRODUTO
            , src_erp__PRODUCT.PRODUTO_TAMANHO_CLASSE
            , src_erp__PRODUCT.PRODUTO_ESTILO
            , src_erp__PRODUCT.PRODUCTSUBCATEGORYID
            , src_erp__PRODUCT.PRODUCTMODELID
            , src_erp__PRODUCTSUBCATEGORY.PRODUCTCATEGORYID
            , src_erp__PRODUCTSUBCATEGORY.nm_subcategoria_produto
            , src_erp__PRODUCTCATEGORY.NM_CATEGORIA_PRODUTO
            , src_erp__PRODUCTMODEL.NM_MODELO
            , src_erp__PRODUCTMODEL.DESCRICAO
        from src_erp__PRODUCT
        left join src_erp__PRODUCTSUBCATEGORY
        on src_erp__PRODUCT.PRODUCTSUBCATEGORYID = src_erp__PRODUCTSUBCATEGORY.PRODUCTSUBCATEGORYID
        left join src_erp__PRODUCTCATEGORY
        on src_erp__PRODUCTSUBCATEGORY.PRODUCTCATEGORYID = src_erp__PRODUCTCATEGORY.PRODUCTCATEGORYID
        left join src_erp__PRODUCTMODEL
        on src_erp__PRODUCT.PRODUCTMODELID = src_erp__PRODUCTMODEL.PRODUCTMODELID
    )

    , chaves as (
        select
            hash(PRODUCTID)
                as pk_produto
            , PRODUCTID
                as cd_produto_num
            , PRODUCTCATEGORYID
                as cd_produto_categoria
            , PRODUCTSUBCATEGORYID
                as cd_produto_subcategoria
            , PRODUCTMODELID
                as cd_produto_modelo
            , CD_PRODUTO
            , NM_CATEGORIA_PRODUTO
            , nm_subcategoria_produto
            , LINHA_DO_PRODUTO
            , NM_MODELO
            , NM_PRODUTO
            , DESCRICAO
            , PRODUTO_COR
            , PRODUTO_TAMANHO
            , PRODUTO_PESO
            , PRODUTO_DIAS_FABRICACAO
            , PRODUTO_TAMANHO_CLASSE
            , PRODUTO_ESTILO
            , SN_PODE_SER_VENDIDO
            , SN_FABRICADO_PELA_EMPRESA
        from uniao_tabelas
    )
select *
from chaves