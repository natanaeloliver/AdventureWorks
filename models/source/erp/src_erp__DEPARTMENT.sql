with
    fonte_departamentos as (
        select
            cast(DEPARTMENTID as int)
                as DEPARTMENTID
            , cast(NAME as string)
                as nm_departamento
            , cast(GROUPNAME as string)
                as nm_grupo_departamento
        from {{ source('erp', 'DEPARTMENT') }}
    )
select *
from fonte_departamentos