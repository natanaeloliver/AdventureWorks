with
    src_erp__SALESPERSON as (
        select *
        from {{ ref('src_erp__SALESPERSON') }}
    )

    , src_erp__PERSON as (
        select *
        from {{ ref('src_erp__PERSON') }}
    )

    , src_erp__EMPLOYEE as (
        select *
        from {{ ref('src_erp__EMPLOYEE') }}
    )

    , src_erp__EMPLOYEEDEPARTMENTHISTORY as (
        select *
        from {{ ref('src_erp__EMPLOYEEDEPARTMENTHISTORY') }}
        where
            src_erp__EMPLOYEEDEPARTMENTHISTORY.DT_DEPARTAMENTO_FIM is null
    )

    , src_erp__SALESTERRITORYHISTORY as (
        select *
        from {{ ref('src_erp__SALESTERRITORYHISTORY') }}
        where
            src_erp__SALESTERRITORYHISTORY.DT_TERRITORIO_VENDA_FINAL is null
    )

    , src_erp__DEPARTMENT as (
        select *
        from {{ ref('src_erp__DEPARTMENT') }}
    )

    , src_erp__SHIFT as (
        select *
        from {{ ref('src_erp__SHIFT') }}
    )

    , uniao_tabelas as (
        select
            src_erp__SALESPERSON.BUSINESSENTITYID
            , src_erp__SALESPERSON.TERRITORYID
            , src_erp__SALESPERSON.META_VENDA
            , src_erp__SALESPERSON.BONUS_META
            , src_erp__SALESPERSON.COMICAO_PERC
            , src_erp__PERSON.TP_PESSOA
            , src_erp__PERSON.CD_TP_PESSOA
            , src_erp__PERSON.NM_PESSOA
            , src_erp__EMPLOYEE.FUNCIONARIO_CARGO
            , src_erp__EMPLOYEE.DT_NASCIMENTO_FUNCIONARIO
            , src_erp__EMPLOYEE.ESTADO_CIVIL
            , src_erp__EMPLOYEE.GENERO
            , src_erp__EMPLOYEE.DT_CONTRATADO
            , datediff(YEAR,src_erp__EMPLOYEE.DT_CONTRATADO,current_date()) as senioridade
            , src_erp__EMPLOYEE.SN_ASSALARIADO
            , src_erp__EMPLOYEE.SN_ATIVO
            , src_erp__EMPLOYEE.NIVEL_ORGANIZACIONAL
            , src_erp__EMPLOYEE.NIVEL_2
            , src_erp__EMPLOYEE.NIVEL_3
            , src_erp__EMPLOYEE.NIVEL_4
            , src_erp__EMPLOYEE.NIVEL_5
            , src_erp__EMPLOYEEDEPARTMENTHISTORY.DEPARTMENTID
            , src_erp__DEPARTMENT.NM_DEPARTAMENTO
            , src_erp__DEPARTMENT.NM_GRUPO_DEPARTAMENTO
            , src_erp__SHIFT.SHIFTID
            , src_erp__SHIFT.DS_TURNO
        from src_erp__SALESPERSON
        left join src_erp__PERSON
        on src_erp__SALESPERSON.BUSINESSENTITYID = src_erp__PERSON.BUSINESSENTITYID
        left join src_erp__EMPLOYEE
        on src_erp__SALESPERSON.BUSINESSENTITYID = src_erp__EMPLOYEE.BUSINESSENTITYID
        left join src_erp__EMPLOYEEDEPARTMENTHISTORY
        on src_erp__EMPLOYEE.BUSINESSENTITYID = src_erp__EMPLOYEEDEPARTMENTHISTORY.BUSINESSENTITYID
        left join src_erp__DEPARTMENT
        on src_erp__EMPLOYEEDEPARTMENTHISTORY.DEPARTMENTID = src_erp__DEPARTMENT.DEPARTMENTID
        left join src_erp__SALESTERRITORYHISTORY
        on src_erp__SALESPERSON.BUSINESSENTITYID = src_erp__SALESTERRITORYHISTORY.BUSINESSENTITYID
        left join src_erp__SHIFT
        on src_erp__EMPLOYEEDEPARTMENTHISTORY.SHIFTID = src_erp__SHIFT.SHIFTID
    )

    , chaves as (
        select
            hash(BUSINESSENTITYID) as pk_vendedor
            , BUSINESSENTITYID as cd_entidade_negocio
            , DEPARTMENTID as cd_departamento
            , CD_TP_PESSOA as cd_tp_pessoa_vendedor
            , SHIFTID as cd_turno
            , NM_GRUPO_DEPARTAMENTO
            , NM_DEPARTAMENTO
            , FUNCIONARIO_CARGO
            , DS_TURNO
            , TP_PESSOA
            , NM_PESSOA
                AS NM_VENDEDOR
            , COMICAO_PERC
            , SENIORIDADE
            , SN_ASSALARIADO
            , GENERO
            , ESTADO_CIVIL
            , META_VENDA
            , BONUS_META
            , DT_NASCIMENTO_FUNCIONARIO
            , DT_CONTRATADO
            , NIVEL_ORGANIZACIONAL
            , NIVEL_2
            , NIVEL_3
            , NIVEL_4
            , NIVEL_5
            , SN_ATIVO
        from uniao_tabelas
        union all
        select
            hash(0) as pk_vendedor
            , 0 as cd_entidade_negocio
            , null as cd_departamento
            , null as cd_tp_pessoa_vendedor
            , null as cd_turno
            , null as NM_GRUPO_DEPARTAMENTO
            , null as NM_DEPARTAMENTO
            , null as FUNCIONARIO_CARGO
            , null as DS_TURNO
            , null as TP_PESSOA
            , 'Venda feita pelo cliente' as NM_VENDEDOR
            , 0 as COMICAO_PERC
            , null as SENIORIDADE
            , false as SN_ASSALARIADO
            , null as GENERO
            , null as ESTADO_CIVIL
            , 0 as META_VENDA
            , 0 as BONUS_META
            , null as DT_NASCIMENTO_FUNCIONARIO
            , null as DT_CONTRATADO
            , null as NIVEL_ORGANIZACIONAL
            , null as NIVEL_2
            , null as NIVEL_3
            , null as NIVEL_4
            , null as NIVEL_5
            , true as SN_ATIVO
    )

select *
from chaves
