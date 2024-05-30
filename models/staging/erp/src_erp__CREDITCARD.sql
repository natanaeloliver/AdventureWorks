with
    fonte_cartoes as (
        select
            cast(CREDITCARDID as int)
                as CREDITCARDID
            , cast(CARDTYPE as string)
                as tp_cartao
        from {{ source('erp', 'CREDITCARD') }}
    )
select *
from fonte_cartoes