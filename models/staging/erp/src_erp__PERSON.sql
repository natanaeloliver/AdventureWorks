with
    fonte_pessoas as (
        select
            cast(BUSINESSENTITYID as int)
                as BUSINESSENTITYID
            , case
                when PERSONTYPE = 'SC' then 'Contato da loja'
                when PERSONTYPE = 'IN' then 'Cliente individual (varejo)'
                when PERSONTYPE = 'SP' then 'Vendedor'
                when PERSONTYPE = 'EM' then 'Funcionário (não vendedor)'
                when PERSONTYPE = 'VC' then 'Contato do fornecedor'
                when PERSONTYPE = 'GC' then 'Contato geral'
            end as tp_pessoa
            , case
                when PERSONTYPE = 'SC' then 1
                when PERSONTYPE = 'IN' then 2
                when PERSONTYPE = 'SP' then 3
                when PERSONTYPE = 'EM' then 4
                when PERSONTYPE = 'VC' then 5
                when PERSONTYPE = 'GC' then 6
            end as tp_pessoa_id
            , case 
                when MIDDLENAME is not null then 
                    cast(FIRSTNAME as string)||' '||cast(MIDDLENAME as string)||' '||cast(LASTNAME as string)
                else
                    cast(FIRSTNAME as string)||' '||cast(LASTNAME as string)
            end as nm_pessoa
        from {{ source('erp', 'PERSON') }}
    )
select *
from fonte_pessoas