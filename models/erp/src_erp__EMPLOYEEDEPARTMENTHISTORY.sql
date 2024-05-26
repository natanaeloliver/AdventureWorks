with
    fonte_funcionarios_departamentos as (
        select
            cast(BUSINESSENTITYID as int)
                as BUSINESSENTITYID
            , cast(DEPARTMENTID as int)
                as DEPARTMENTID
            , cast(SHIFTID as int)
                as SHIFTID
            , cast(STARTDATE as date)
                as dt_departamento_inicio
            , cast(ENDDATE as date)
                as dt_departamento_fim
        from {{ source('erp', 'EMPLOYEEDEPARTMENTHISTORY') }}
    )
select *
from fonte_funcionarios_departamentos