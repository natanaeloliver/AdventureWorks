with
    fonte_produtos as (
        select
            cast(PRODUCTID as int)
                as PRODUCTID
            , NAME as nm_produto
            , PRODUCTNUMBER as cd_produto
            , MAKEFLAG as sn_fabricado_pela_empresa
            , FINISHEDGOODSFLAG as sn_pode_ser_vendido
            , case
                when COLOR = 'Black'        then 'Preto'
                when COLOR = 'Red'          then 'Vermelho'
                when COLOR = 'White'        then 'Branco'
                when COLOR = 'Blue'         then 'Azul'
                when COLOR = 'Multi'        then 'Multicolorido'
                when COLOR = 'Yellow'       then 'Amarelo'
                when COLOR = 'Grey'         then 'Cinza'
                when COLOR = 'Silver/Black' then 'Prata/Preto'
                when COLOR = 'Silver'       then 'Prata'
                else 'Não se aplica'
            end as produto_cor
            , case
                when SIZEUNITMEASURECODE is null and SIZE is null then 'Não se aplica'
                when SIZEUNITMEASURECODE is null then SIZE
                else SIZE||' '||SIZEUNITMEASURECODE
            end as produto_tamanho
            , case
                when WEIGHT is not null then WEIGHT||' '||WEIGHTUNITMEASURECODE
                else 'Não se aplica'
            end as produto_peso
            , DAYSTOMANUFACTURE as produto_dias_fabricacao
            , case
                when PRODUCTLINE = 'R' then 'Estrada'
                when PRODUCTLINE = 'M' then 'Montanha'
                when PRODUCTLINE = 'T' then 'Passeio'
                when PRODUCTLINE = 'S' then 'Padrão'
                else 'Não se aplica'
            end as linha_do_produto
            , case
                when CLASS = 'H' then 'Grande'
                when CLASS = 'M' then 'Médio'
                when CLASS = 'L' then 'Pequeno'
                else 'Não se aplica'
            end as produto_tamanho_classe
            , case
                when STYLE = 'W' then 'Feminino'
                when STYLE = 'M' then 'Masculino'
                when STYLE = 'U' then 'Universal'
                else 'Não se aplica'
            end as produto_estilo
            , cast(PRODUCTSUBCATEGORYID as int)
                as PRODUCTSUBCATEGORYID
            , cast(PRODUCTMODELID as int)
                as PRODUCTMODELID
        from {{ source('erp', 'PRODUCT') }}
    )
select *
from fonte_produtos