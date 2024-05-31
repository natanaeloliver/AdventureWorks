with
    fonte_cidades_faltantes as (
        select
            cast(CD_CIDADE as string) as CD_CIDADE
            , cast(PAIS as string) as cd_pais
            , cast(CIDADE as string) as CIDADE
            , cast(replace(LATITUDE,'|','') as float) as LATITUDE
            , cast(replace(LONGITUDE,'|','') as float) as LONGITUDE
        from {{ source('web', 'CIDADES_FALTANTES') }}
    )

select *
from fonte_cidades_faltantes
where CD_CIDADE <> 'CD_CIDADE'