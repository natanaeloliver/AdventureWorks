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
            , cast(ID as int) as ID
        from {{ source('web', 'WORLDCITIES') }}
    )

    , cidade_mais_atual as (
        select
            max(ID) as ID
            , cidade
            , nm_pais
        from fonte_cidades_mundo
        group by
            cidade
            , nm_pais
    )

    , tabela_completa as (
        select
            fonte_cidades_mundo.*
        from fonte_cidades_mundo
        inner join cidade_mais_atual
        on fonte_cidades_mundo.ID = cidade_mais_atual.ID
    )

select *
from tabela_completa
where cidade = 'Miami'