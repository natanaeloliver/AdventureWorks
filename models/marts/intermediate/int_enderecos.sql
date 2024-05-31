with
    src_epr__ADDRESS as (
        select *
        from {{ ref('src_epr__ADDRESS') }}
    )

    , src_erp__BUSINESSENTITYADDRESS as (
        select *
        from {{ ref('src_erp__BUSINESSENTITYADDRESS') }}
    )

    , src_erp__ADDRESSTYPE as (
        select *
        from {{ ref('src_erp__ADDRESSTYPE') }}
    )

    , src_erp__STATEPROVINCE as (
        select *
        from {{ ref('src_erp__STATEPROVINCE') }}
    )

    , src_erp__COUNTRYREGION as (
        select *
        from {{ ref('src_erp__COUNTRYREGION') }}
    )

    , src_web__WORLDCITIES as (
        select *
        from {{ ref('src_web__WORLDCITIES') }}
    )

    , src_web__CIDADES_FALTANTES as (
        select *
        from {{ ref('src_web__CIDADES_FALTANTES') }}
    )

    , chave_cidades_web as (
        select
            concat(upper(ISO2),'|',upper(translate(CIDADE_ASCII,'áéíóúâêôãõüçàèìòù','aeiouaeoaoucaeiou'))) as cd_cidade
            , LATITUDE
            , LONGITUDE
            , TP_CIDADE
            , POPULATION
        from src_web__WORLDCITIES
        union all
        select
            CD_CIDADE
            , LATITUDE
            , LONGITUDE
            , case
                when CIDADE = 'QUEBEC' then 'Capital do país'
                else 'Cidade'
            end as TP_CIDADE
            , null POPULATION
        from src_web__CIDADES_FALTANTES
    )

    , uniao_tabelas as (
        select
            src_epr__ADDRESS.ADDRESSID
            , src_epr__ADDRESS.CIDADE
            , src_erp__STATEPROVINCE.CD_ESTADO
            , src_erp__STATEPROVINCE.COUNTRYREGIONCODE
            , src_erp__STATEPROVINCE.SN_PAIS_SEM_ESTADO
            , src_erp__STATEPROVINCE.NM_ESTADO
            , src_erp__STATEPROVINCE.TERRITORYID
            , src_erp__COUNTRYREGION.NM_PAIS
            , src_erp__BUSINESSENTITYADDRESS.ADDRESSTYPEID
            , src_erp__ADDRESSTYPE.TP_ENDERECO
        from src_epr__ADDRESS
        left join src_erp__STATEPROVINCE
        on src_epr__ADDRESS.STATEPROVINCEID = src_erp__STATEPROVINCE.STATEPROVINCEID
        left join src_erp__COUNTRYREGION
        on src_erp__STATEPROVINCE.COUNTRYREGIONCODE = src_erp__COUNTRYREGION.COUNTRYREGIONCODE
        left join src_erp__BUSINESSENTITYADDRESS
        on src_epr__ADDRESS.ADDRESSID = src_erp__BUSINESSENTITYADDRESS.ADDRESSID
        left join src_erp__ADDRESSTYPE
        on src_erp__BUSINESSENTITYADDRESS.ADDRESSTYPEID = src_erp__ADDRESSTYPE.ADDRESSTYPEID
    )

    , chaves as (
        select
            hash(ADDRESSID)
                as pk_endereco
            , COUNTRYREGIONCODE
                as cd_pais
            , CD_ESTADO
            , TERRITORYID
                as cd_territorio
            , concat(upper(COUNTRYREGIONCODE),'|',upper(translate(CIDADE,'áéíóúâêôãõüçàèìòù','aeiouaeoaoucaeiou')))
                as cd_cidade
            , ADDRESSTYPEID
                as cd_tp_endereco
            , ADDRESSID
                as cd_endereco
            , NM_PAIS
            , NM_ESTADO
            , CIDADE
            , TP_ENDERECO
            , SN_PAIS_SEM_ESTADO
        from uniao_tabelas
    )

    , tabela_com_lat_lng as (
        select
            chaves.PK_ENDERECO
            , chaves.CD_PAIS
            , chaves.CD_ESTADO
            , chaves.CD_TERRITORIO
            , chaves.CD_CIDADE
            , chaves.CD_TP_ENDERECO
            , chaves.CD_ENDERECO
            , chaves.NM_PAIS
            , chaves.NM_ESTADO
            , chaves.CIDADE
            , chaves.TP_ENDERECO
            , chaves.SN_PAIS_SEM_ESTADO
            , chave_cidades_web.LATITUDE
            , chave_cidades_web.LONGITUDE
            , chave_cidades_web.TP_CIDADE
            , chave_cidades_web.POPULATION
        from chaves
        left join chave_cidades_web
        on chaves.cd_cidade = chave_cidades_web.cd_cidade
    )

select *
from tabela_com_lat_lng

