with
    fonte_turnos as (
        select
            cast(SHIFTID as int)
                as SHIFTID
            , case
                when NAME = 'Day'       then 'Matutino'
                when NAME = 'Evening'   then 'Vespertino'
                when NAME = 'Night'     then 'Noturno'
            end as turnos
        from {{ source('erp', 'SHIFT') }}
    )
select *
from fonte_turnos