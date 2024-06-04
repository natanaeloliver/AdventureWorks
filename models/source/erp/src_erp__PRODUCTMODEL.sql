with
    fonte_modelos as (
        select
        cast(PRODUCTMODELID as int) as PRODUCTMODELID
        , cast(NAME as string) as nm_modelo
        , replace(split_part(cast(CATALOGDESCRIPTION as string),'p>',2),'</html:','') as descricao
        from {{ source('erp', 'PRODUCTMODEL') }}
    )

select *
from fonte_modelos