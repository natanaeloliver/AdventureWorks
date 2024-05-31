with
    fonte_cidades_mundo as (
        select
            cast(CITY as string) as cidade
            , cast(CITY_ASCII as string) as cidade_ASCII
            , cast(LAT as float) as latitude
            , cast(LNG as float) as longitude
            , cast(COUNTRY as string) as nm_pais
            , cast(ISO2 as string) as ISO2
            , cast(ISO3 as string) as ISO3
            , case
                when CAPITAL = 'admin' then 'Capital do estado'
                when CAPITAL = 'primary' then 'Capital do país'
                else 'Cidade'
            end as tp_cidade
            , cast(POPULATION as string) as POPULATION
            , cast(ID as int) as ID
        from {{ source('web', 'WORLDCITIES') }}
    )

select *
from fonte_cidades_mundo