with
    fonte_ofestas as (
        select
            cast(SPECIALOFFERID as int)
                as SPECIALOFFERID
            , DESCRIPTION as ds_oferta
            , cast(DISCOUNTPCT as numeric(18,2))
                as desconto_perc
            , TYPE as tp_oferta
            , CATEGORY as categoria_oferta
            , cast(STARTDATE as date)
                as dt_oferta_inicial
            , cast(ENDDATE as date)
                as dt_oferta_final
            , cast(MINQTY as int)
                as qtd_min
            , cast(MAXQTY as int)
                as qtd_max
        from {{ source('erp', 'SPECIALOFFER') }}
    )
select *
from fonte_ofestas