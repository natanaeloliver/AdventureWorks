with
    fonte_funcionarios as (
        select
            cast(BUSINESSENTITYID as int)
                as BUSINESSENTITYID
            , cast(JOBTITLE as string)
                as funcionario_cargo
            , cast(BIRTHDATE as date)
                as dt_nascimento_funcionario
            , case
                when MARITALSTATUS = 'S' then 'Solteiro(a)'
                when MARITALSTATUS = 'M' then 'Casado(a)'
            end as estado_civil
            , case
                when GENDER = 'M' then 'Masculino'
                when GENDER = 'F' then 'Feminino'
            end as genero
            , cast(HIREDATE as date)
                as dt_contratado
            , SALARIEDFLAG as sn_assalariado
            , CURRENTFLAG as sn_ativo
            , ORGANIZATIONNODE as nivel_organizacional
            , split_part(ORGANIZATIONNODE,'/',2) as nivel_2
            , split_part(ORGANIZATIONNODE,'/',3) as nivel_3
            , split_part(ORGANIZATIONNODE,'/',4) as nivel_4
            , split_part(ORGANIZATIONNODE,'/',5) as nivel_5
        from {{ source('erp', 'EMPLOYEE') }}
    )
select *
from fonte_funcionarios