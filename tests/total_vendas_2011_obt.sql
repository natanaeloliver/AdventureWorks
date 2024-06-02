with
    vendas_em_2011 as (
        select sum(VALOR_NEGOCIADO) as valor_total_negociado
        from {{ ref('obt_pedidos_itens') }}
        where DT_PEDIDO between '2011-01-01' and '2011-12-31'
    )
select valor_total_negociado
from vendas_em_2011 -- 12646112.1607 esse é o valor conferido pelo time da contabilidade das vendas de 2011
where valor_total_negociado not between 12646112.160 and 12646112.161