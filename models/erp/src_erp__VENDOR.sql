with
    fonte_plataforma_venda as (
        select
            cast(BUSINESSENTITYID as int)
                as BUSINESSENTITYID
            , cast(NAME as string)
                as nm_plataforma_venda
            , case
                when CREDITRATING = 1 then 'Superior'
                when CREDITRATING = 2 then 'Excelente'
                when CREDITRATING = 3 then 'Acima da média'
                when CREDITRATING = 4 then 'Médio'
                when CREDITRATING = 5 then 'Abaixo da média'
            end as classificacao_credito
            , CREDITRATING as cd_classificacao_credito
            , PREFERREDVENDORSTATUS as sn_tem_preferencia
            , ACTIVEFLAG as sn_ativo
        from {{ source('erp', 'VENDOR') }}
    )
select *
from fonte_plataforma_venda