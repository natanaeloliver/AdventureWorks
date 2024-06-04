# AdventureWorks
Repositório utilizado para a Certificação Analytics Engineer by Indicium
[Link do dashboard criado](https://app.powerbi.com/view?r=eyJrIjoiZTU5NmY1NWYtZjIxZi00ODhhLThlZGYtZTdmOWZkZmZmOTY1IiwidCI6ImIwNzVmYzFkLWRkZDYtNDQ5Yy04ZTYxLWYzODg2MzgzOTcwMCJ9&pageName=ReportSection)

## Objetivos
### 1 - Diagrama conceitual do data warehouse em formato PDF
Um modelo conceitual com as tabelas de fatos e dimensões necessárias para responder
às perguntas de negócio do item 4. Mostrar de forma resumida quais as tabelas fonte 
que foram utilizadas para criar cada dimensão e a tabela fato. 

### 2 - Configuração de um data warehouse na nuvem e configuração do dbt.

### 3 - ELT
a - documentação das tabelas e colunas nos marts
b - testes de sources
c - testes nas primary keys das tabelas de dimensão e fatos
d - teste de dados (lembro do pedido do Carlos)

### 4 - Painéis de BI
a - Qual o número de pedidos, quantidade comprada, valor total negociado por produto,
tipo de cartão, motivo de venda, data de venda, cliente, status, cidade, estado e país?
b - Quais os produtos com maior ticket médio por mês, ano, cidade, estado e país?
(ticket médio = Faturamento bruto - descontos do produto / número de pedidos no período de análise)
c - Quais os 10 melhores clientes por valor total negociado filtrado por produto,
tipo de cartão, motivo de venda, data de venda, status, cidade, estado e país?
d - Quais as 5 melhores cidades em valor total negociado por produto, tipo de cartão,
motivo de venda, data de venda, cliente, status, cidade, estado e país?
e - Qual o número de pedidos, quantidade comprada, valor total negociado por mês e ano ?
f - Qual produto tem a maior quantidade de unidades compradas para o motivo de venda “Promotion”?

### 5 - Vídeo: você deverá gravar um vídeo apresentando todas as etapas do projeto
Etapas do projeto: DW, EL, transformação em dbt e BI.
Crie com alguma ferramenta de gravação, como o Nimbus ou OBS Studio.
O vídeo não deve ter duração maior que 10 minutos.
a - Uma breve explicação do objetivo do projeto e resultados esperados.
b - Uma explicação das dimensões que foram criadas e a relação delas com a tabela de fatos.
c - Uma demonstração do dbt run onde conseguimos ver que todos os modelos rodam com sucesso. Use dbt run
d - Uma demonstração que todos os tests aplicados na source passam. Use dbt test –select source:*
e - Uma demonstração que todos os tests aplicados nos modelos passam. Use dbt test
f - Demonstração da tabela fatos e joins feitos entre a tabela fatos e as dimensões.
Quais métricas foram criadas na tabela fato e porque.
g - Apresentar o teste de dados.
h - Apresentação do painel, como os filtros funcionam, visualizações, análises possíveis,
apresente também a resposta para o item 4-f.

### Planejamento do projeto de dados em formato PDF
Considere as informações que você obteve nos diagnósticos iniciais e que estão resumidas no contexto.
Em especial, leve em consideração os objetivos esperados, partes interessadas e riscos/contingências do projeto.
Além disso, que outros valores podemos obter em projetos de infraestrutura de dados? Há outros riscos e
contingências que você pense ser relevante incluir no planejamento do projeto.


