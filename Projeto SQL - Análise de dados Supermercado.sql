-- Databricks notebook source
-- MAGIC %md # Sobre o Conjunto de Dados
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Contexto
-- MAGIC
-- MAGIC O crescimento dos supermercados nas cidades mais populosas está aumentando, e a concorrência de mercado também é alta. Este conjunto de dados faz parte das vendas históricas de uma empresa de supermercados localizada em Mianmar, registradas em 3 diferentes filiais durante um período de 3 meses. Métodos de análise de dados preditiva são fáceis de aplicar com este conjunto de dados.
-- MAGIC
-- MAGIC ### Informações sobre os Atributos
-- MAGIC
-- MAGIC     ID da Fatura (Invoice id): Número de identificação da fatura de vendas gerado por computador.
-- MAGIC     Filial (Branch): Filial do supermercado (3 filiais estão disponíveis, identificadas como A, B e C).
-- MAGIC     Cidade (City): Localização dos supermercados.
-- MAGIC     Tipo de Cliente (Customer type): Tipo de clientes, registrado como Membros para clientes que usam cartão de membro e Normal para aqueles sem cartão de membro.
-- MAGIC     Gênero (Gender): Gênero do cliente.
-- MAGIC     Linha de Produtos (Product line): Grupos de categorização de itens gerais - Acessórios eletrônicos, Acessórios de moda, Alimentos e bebidas, Saúde e beleza, Casa e estilo de vida, Esportes e viagens.
-- MAGIC     Preço Unitário (Unit price): Preço de cada produto em dólares ($).
-- MAGIC     Quantidade (Quantity): Número de produtos comprados pelo cliente.
-- MAGIC     Imposto (Tax): Taxa de imposto de 5% para compras do cliente.
-- MAGIC     Total: Preço total, incluindo imposto.
-- MAGIC     Data (Date): Data da compra (Registros disponíveis de janeiro de 2019 a março de 2019).
-- MAGIC     Hora (Time): Horário da compra (10h às 21h).
-- MAGIC     Pagamento (Payment): Método de pagamento usado pelo cliente para a compra (3 métodos disponíveis – Dinheiro, Cartão de Crédito e Carteira Digital).
-- MAGIC     COGS: Custo dos produtos vendidos.
-- MAGIC     Percentual de Margem Bruta (Gross margin percentage): Percentual de margem bruta.
-- MAGIC     Renda Bruta (Gross income): Renda bruta.
-- MAGIC     Avaliação (Rating): Avaliação do cliente sobre sua experiência geral de compra (em uma escala de 1 a 10).
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Análise de dados

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Criando uma visão para a tabela

-- COMMAND ----------

create view vw_vendas_mercado
as
select *
from vendas_mercado

-- COMMAND ----------

select *
from vw_vendas_mercado

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Média de renda bruta por linha de produto
-- MAGIC
-- MAGIC Aqui poderemos ver qual é a linha de produto mais rentável do mercado

-- COMMAND ----------

select
linha_produto,
round(avg(renda_bruta),2) as media_renda_bruta
from vw_vendas_mercado
group by linha_produto
order by 2 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Produtos que geram mais renda
-- MAGIC
-- MAGIC Acima nós pudemos ver que a linha Home and Lifestyle é a que mais gera renda média para o mercado, no entanto, não é a que mais gera renda total. Os produtos que mais geral renda bruta estão na categoria de alimentos e bebidas. A categoria de esportes e viagem performa bem tanto em média quanto total, como podemos ver abaixo

-- COMMAND ----------

select
linha_produto,
round(sum(renda_bruta),2) as soma_renda_bruta
from vw_vendas_mercado
group by linha_produto
order by 2 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Produtos mais vendidos
-- MAGIC
-- MAGIC
-- MAGIC Por fim, resolvi ver quais são os produtos mais vendidos:

-- COMMAND ----------

select
linha_produto,
round(sum(quantidade),2) as soma_quantidade
from vw_vendas_mercado
group by linha_produto
order by 2 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Análise de renda média por gênero
-- MAGIC
-- MAGIC Podemos perceber que as mulheres são quem mais gastam no mercado.

-- COMMAND ----------

select
genero,
round(avg(renda_bruta),2) as media_renda_genero
from vw_vendas_mercado
group by genero
order by 2 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Análise de perfil de cliente
-- MAGIC
-- MAGIC O mercado possui dois tipos de cliente: os membros e os não-membros (normais). Em um primeiro momento, a diferença de renda média bruta gerada entre os dois tipos não parece ser muito grande, como podemos ver abaixo. Mas isso também requer mais exploração de dados, como faremos a seguir.

-- COMMAND ----------

select
tipo_cliente,
round(avg(renda_bruta),2) as media_renda_bruta
from vw_vendas_mercado
group by tipo_cliente
order by 2 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Destrinchando o perfil do cliente
-- MAGIC Como a análise acima não respondeu muita coisa, resolvi separar então os tipos de cliente por gênero. E aqui sim podemos ter um bom insight: mulheres que são membros são as que mais trazem renda bruta para o mercado, enquanto mulheres que não são membros, são as que menos trazem renda. Ou seja, existe alguma vantagem para os membros mulheres que as fazem gastar mais. O mercado poderia ofertar algum tipo de promoção para trazer mais mulheres para o membership do mercado, criar um clube de vantagens e desconto para também fidelizar quem já está lá e aumentar ainda mais o ticket médio desse grupo

-- COMMAND ----------

select
tipo_cliente,
genero,
count(*) as quantidade_cl,
round(avg(renda_bruta),2) as media_renda_bruta,
round(sum(renda_bruta),2) as soma_renda_bruta
from vw_vendas_mercado
group by tipo_cliente,
genero
order by
soma_renda_bruta desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Explorando a renda por filial
-- MAGIC
-- MAGIC Qual filial tem maior renda total e média? A cidade de Naypyitaw tem a maior média e maior renda total.

-- COMMAND ----------

select
cidade,
round(avg(renda_bruta),2) as media_renda_cidade,
round(sum(renda_bruta),2) as soma_renda_cidade
from vw_vendas_mercado
group by cidade
order by 2 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Tipos de clientes por gênero e por cidade
-- MAGIC
-- MAGIC Verificando a quantidade de clientes pro grupos por cidade. Assim, podemos ver qual cidade tem maior potencial para angariar novos membros, principalmente mulheres, como determinamos anteriormente. E podemos perceber que Naypyitaw e Yangon são as cidades com maior potencial.

-- COMMAND ----------

select
tipo_cliente,
genero,
cidade,
count(*) as quantidade_cl
from vw_vendas_mercado
group by tipo_cliente,
genero,
cidade
order by
quantidade_cl desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Média de avaliação por tipo de produto

-- COMMAND ----------

select
linha_produto,
round(avg(avaliacao),2) as media_avaliacao
from vw_vendas_mercado
group by linha_produto
order by 2 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Explorando produtos pela média
-- MAGIC
-- MAGIC Resolvi ver se havia alguma diferença de media de faturamento entre produtos bem avaliados, médio avaliados e mal avaliados e os resultados foram interessantes. Os produtos mais bem avaliados (acima de 7) possuem uma média de faturamento boa (15.4 dólares). Já na faixa média (entre 5 e 7), o faturamento cai. E na faixa mais baixa (abaixo de nota 5), o faturamento chega a quase 17 dólares. É uma quantidade menor de produto, o ideal seria ver se há compra recorrente desses produtos, caso haja, mesmo com a baixa avaliação, são produtos que trazem bom retorno para o mercado. Vale a pena entender o porquê deste produto ser mal avaliado.

-- COMMAND ----------

SELECT 
    avg(renda_bruta) AS media_faturamento,
    count(*) as quantidade_produtos
FROM 
    vw_vendas_mercado
WHERE 
    avaliacao > 7;

-- COMMAND ----------

SELECT 
    avg(renda_bruta) AS media_faturamento,
    count(*) as quantidade_produtos
FROM 
    vw_vendas_mercado
WHERE 
    avaliacao < 7 and avaliacao >5;

-- COMMAND ----------

SELECT 
    avg(renda_bruta) AS media_faturamento,
    count(*) as quantidade_produtos
FROM 
    vw_vendas_mercado
WHERE 
    avaliacao < 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Conclusão
-- MAGIC
-- MAGIC - Buscar crescer o número de membros mulheres;
-- MAGIC - Meios de fidelizar ainda mais os membros já existentes;
-- MAGIC - Entender o motivo de produtos de baixa avaliação terem maior média de renda;
-- MAGIC - Investir em campanhas de membros nas filiais de Naypyitaw e Yangon.
