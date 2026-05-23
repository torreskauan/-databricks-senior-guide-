# Databricks notebook source

# MAGIC %md
# # 04 — Aggregations: groupBy, agg, pivot, cube, rollup
#
# **Analogia:**
# Imagine uma planilha de vendas com milhões de linhas. Você precisa responder perguntas
# como: "quanto cada região vendeu?" (groupBy), "quanto cada produto vendeu por mês, com
# os meses como colunas?" (pivot), "me dê os totais por região, por categoria, e o total
# geral tudo de uma vez" (cube), "me dê uma hierarquia: total geral → por região → por
# categoria" (rollup).
#
# **Conceito técnico:**
# O Spark oferece 4 formas de agregação multidimensional:
# - **groupBy + agg:** agrupa por colunas e aplica funções de agregação — o mais comum.
# - **pivot:** transforma valores distintos de uma coluna em colunas separadas.
# - **cube:** gera todas as combinações possíveis de subtotais (2^N combinações).
# - **rollup:** gera subtotais hierárquicos da esquerda para a direita.
#
# Internamente todas usam o **HashAggregate** (2 fases: parcial nos Executors + final
# após shuffle) ou **SortAggregate** (quando o tipo não suporta hashing).
#
# **Quando usar este conhecimento:**
# - ETL e construção de tabelas analíticas (marts, gold layer)
# - Relatórios com múltiplos níveis de granularidade
# - Cálculos de métricas complexas sem sair do DataFrame API
# - Entrevistas sênior e prova Databricks Associate/Professional

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, min as spark_min, max as spark_max,
    countDistinct, collect_list, collect_set, first, last,
    stddev, variance, percentile_approx, corr, covar_pop,
    when, lit, round as spark_round, expr, grouping, grouping_id
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, DateType
)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Dataset base — vendas de uma rede de lojas
schema = StructType([
    StructField("pedido_id",  LongType(),   False),
    StructField("data",       StringType(), True),
    StructField("regiao",     StringType(), True),
    StructField("categoria",  StringType(), True),
    StructField("produto",    StringType(), True),
    StructField("vendedor",   StringType(), True),
    StructField("quantidade", LongType(),   True),
    StructField("valor",      DoubleType(), True),
])

dados = [
    (1,  "2024-01", "SP", "Eletronicos", "Notebook",  "Ana",   2,  8000.0),
    (2,  "2024-01", "SP", "Eletronicos", "Celular",   "Ana",   5,  5000.0),
    (3,  "2024-01", "RJ", "Moveis",      "Mesa",      "Bruno", 3,  2400.0),
    (4,  "2024-01", "RJ", "Eletronicos", "Notebook",  "Bruno", 1,  4000.0),
    (5,  "2024-02", "SP", "Moveis",      "Cadeira",   "Ana",   10, 3000.0),
    (6,  "2024-02", "SP", "Eletronicos", "Celular",   "Carla", 8,  8000.0),
    (7,  "2024-02", "MG", "Eletronicos", "Notebook",  "Diana", 2,  8000.0),
    (8,  "2024-02", "MG", "Moveis",      "Mesa",      "Diana", 4,  3200.0),
    (9,  "2024-03", "SP", "Eletronicos", "Notebook",  "Ana",   3, 12000.0),
    (10, "2024-03", "RJ", "Moveis",      "Cadeira",   "Bruno", 6,  1800.0),
    (11, "2024-03", "MG", "Eletronicos", "Celular",   "Diana", 7,  7000.0),
    (12, "2024-03", "SP", "Moveis",      "Mesa",      "Carla", 2,  1600.0),
    (13, "2024-03", "RJ", "Eletronicos", "Celular",   "Bruno", 3,  3000.0),
    (14, "2024-03", "MG", "Moveis",      "Cadeira",   "Diana", 5,  1500.0),
    (15, "2024-01", None, "Eletronicos", "Notebook",  "Eduardo", 1, 4000.0),  # regiao nula
]

df = spark.createDataFrame(dados, schema=schema)
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ## 1. groupBy + agg — O núcleo das agregações

# COMMAND ----------

# MAGIC %md
# ### 1a. Funções de agregação essenciais

# COMMAND ----------

# Todas as funções de agregação mais usadas em um único agg()
df.groupBy("regiao").agg(
    count("*").alias("total_pedidos"),              # conta todas as linhas (inclui nulls)
    count("valor").alias("pedidos_com_valor"),      # conta apenas linhas não-nulas
    countDistinct("produto").alias("produtos_distintos"),
    spark_sum("valor").alias("receita_total"),
    avg("valor").alias("ticket_medio"),
    spark_min("valor").alias("menor_pedido"),
    spark_max("valor").alias("maior_pedido"),
    stddev("valor").alias("desvio_padrao"),
    percentile_approx("valor", 0.5).alias("mediana_valor"),
    percentile_approx("valor", [0.25, 0.75]).alias("quartis"),
    collect_list("produto").alias("produtos_lista"),   # lista COM repetição
    collect_set("produto").alias("produtos_set"),      # lista SEM repetição (distinct)
    first("vendedor").alias("primeiro_vendedor"),
    last("vendedor").alias("ultimo_vendedor"),
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ### 1b. Múltiplas colunas no groupBy

# COMMAND ----------

# groupBy por múltiplas colunas
df.groupBy("regiao", "categoria").agg(
    spark_sum("valor").alias("receita"),
    count("*").alias("pedidos"),
    avg("quantidade").alias("qtd_media"),
).orderBy("regiao", "categoria").show()

# COMMAND ----------

# MAGIC %md
# ### 1c. Agregações condicionais — contar/somar apenas quando uma condição é verdadeira

# COMMAND ----------

df.groupBy("regiao").agg(
    # Conta pedidos de Eletronicos
    count(when(col("categoria") == "Eletronicos", 1)).alias("pedidos_eletronicos"),
    # Soma apenas valores acima de 3000
    spark_sum(when(col("valor") > 3000, col("valor")).otherwise(0)).alias("receita_premium"),
    # Proporção de pedidos com valor alto
    (count(when(col("valor") > 5000, 1)) / count("*") * 100).alias("pct_alto_valor"),
).show()

# COMMAND ----------

# MAGIC %md
# ### 1d. Sintaxe alternativa com dict e expr

# COMMAND ----------

# Sintaxe dict — mais concisa para agregações simples
df.groupBy("categoria").agg({
    "valor":      "sum",
    "pedido_id":  "count",
    "quantidade": "avg",
}).show()

# COMMAND ----------

# Sintaxe expr — máxima flexibilidade, usa SQL dentro do Python
df.groupBy("regiao", "categoria").agg(
    expr("sum(valor) / count(*) as ticket_medio"),
    expr("sum(case when valor > 5000 then 1 else 0 end) as pedidos_premium"),
    expr("percentile_approx(valor, 0.9) as percentil_90"),
).show()

# COMMAND ----------

# MAGIC %md
# ### ⚠️ groupByKey vs reduceByKey (nível RDD — evite groupByKey)
#
# No nível DataFrame/SQL, `groupBy` é otimizado automaticamente (HashAggregate parcial).
# No nível RDD:
# - `groupByKey()` → traz TODOS os valores para o Executor antes de agregar → shuffle enorme
# - `reduceByKey()` → combina localmente em cada partição ANTES do shuffle → muito mais eficiente
#
# **Regra:** nunca use `groupByKey()` em RDD se `reduceByKey()` ou `aggregateByKey()` resolverem.

# COMMAND ----------

# MAGIC %md
# ## 2. pivot — Transpor valores em colunas

# COMMAND ----------

# MAGIC %md
# ### Como pivot funciona
#
# ```
# DataFrame original (long format):
# ┌────────┬───────┬────────┐
# │ regiao │ data  │  valor │
# ├────────┼───────┼────────┤
# │ SP     │ 2024-01 │ 13000 │
# │ SP     │ 2024-02 │ 11000 │
# │ RJ     │ 2024-01 │  6400 │
# └────────┴───────┴────────┘
#
# Após pivot por "data":
# ┌────────┬─────────┬─────────┬─────────┐
# │ regiao │ 2024-01 │ 2024-02 │ 2024-03 │
# ├────────┼─────────┼─────────┼─────────┤
# │ SP     │  13000  │  11000  │  13600  │
# │ RJ     │   6400  │    null │   4800  │
# └────────┴─────────┴─────────┴─────────┘
# ```

# COMMAND ----------

# Pivot básico: receita por região × data
pivot_regiao_data = (
    df
    .groupBy("regiao")
    .pivot("data")          # cada valor distinto de "data" vira uma coluna
    .agg(spark_sum("valor"))
)
pivot_regiao_data.show()

# COMMAND ----------

# MAGIC %md
# ### ⚠️ Pivot sem lista de valores = 2 jobs (scan para descobrir valores únicos)
#
# Quando você não especifica os valores do pivot, o Spark executa um job extra
# para descobrir os valores distintos da coluna. Em produção, SEMPRE especifique.

# COMMAND ----------

# Pivot COM lista de valores especificada (1 único job — mais eficiente)
meses = ["2024-01", "2024-02", "2024-03"]

pivot_eficiente = (
    df
    .groupBy("regiao")
    .pivot("data", meses)   # ← especifica os valores: sem job extra
    .agg(spark_sum("valor"))
    .fillna(0)              # substitui nulls por 0 onde não houve vendas
)
pivot_eficiente.show()

# COMMAND ----------

# Pivot com múltiplas agregações — gera colunas compostas: "valor_mes_sum", "valor_mes_count"
pivot_multi = (
    df
    .groupBy("regiao")
    .pivot("data", meses)
    .agg(
        spark_sum("valor").alias("receita"),
        count("*").alias("pedidos"),
    )
)
pivot_multi.show()
# Colunas geradas: 2024-01_receita, 2024-01_pedidos, 2024-02_receita, etc.

# COMMAND ----------

# Unpivot — inverso do pivot (wide → long)
# Spark 3.4+ tem o método nativo stack() via SQL
colunas_meses = [f"`{m}`" for m in meses]

pivot_eficiente.createOrReplaceTempView("pivot_view")
unpivot = spark.sql(f"""
    SELECT regiao, mes, receita
    FROM pivot_view
    UNPIVOT (receita FOR mes IN ({', '.join(colunas_meses)}))
""")
unpivot.show()

# Alternativa para versões anteriores com stack():
# df_wide.select(
#     col("regiao"),
#     expr(f"stack(3, '2024-01', `2024-01`, '2024-02', `2024-02`, '2024-03', `2024-03`) as (mes, receita)")
# ).show()

# COMMAND ----------

# MAGIC %md
# ## 3. cube — Todas as combinações de subtotais

# COMMAND ----------

# MAGIC %md
# ### Como cube funciona
#
# Para N dimensões, `cube` gera 2^N combinações de agrupamentos, incluindo o total geral.
#
# ```
# df.cube("regiao", "categoria").agg(sum("valor"))
#
# Gera agrupamentos para:
# ┌────────────┬────────────┬────────────────────────────────┐
# │  regiao    │  categoria │  Representa                    │
# ├────────────┼────────────┼────────────────────────────────┤
# │  SP        │  Moveis    │  SP + Moveis (específico)      │
# │  SP        │  null      │  Total de SP (todas categorias)│
# │  null      │  Moveis    │  Total de Moveis (todas regiões│
# │  null      │  null      │  Total GERAL                   │
# └────────────┴────────────┴────────────────────────────────┘
#
# Com 3 dimensões: 2^3 = 8 combinações
# ```

# COMMAND ----------

# cube por regiao × categoria
resultado_cube = (
    df
    .cube("regiao", "categoria")
    .agg(
        spark_sum("valor").alias("receita"),
        count("*").alias("pedidos"),
    )
    .orderBy(
        col("regiao").asc_nulls_last(),
        col("categoria").asc_nulls_last()
    )
)

resultado_cube.show(20)

# COMMAND ----------

# Identificar o nível de cada linha com grouping()
# grouping(col) retorna 1 se a coluna foi agregada (é subtotal), 0 se tem valor real
resultado_cube_labeled = (
    df
    .cube("regiao", "categoria")
    .agg(
        spark_sum("valor").alias("receita"),
        grouping("regiao").alias("is_subtotal_regiao"),   # 1 = agrupado, 0 = valor real
        grouping("categoria").alias("is_subtotal_cat"),
    )
    .withColumn("nivel", when(
        (col("is_subtotal_regiao") == 1) & (col("is_subtotal_cat") == 1), lit("TOTAL GERAL")
    ).when(
        (col("is_subtotal_regiao") == 1) & (col("is_subtotal_cat") == 0), lit("Total por Categoria")
    ).when(
        (col("is_subtotal_regiao") == 0) & (col("is_subtotal_cat") == 1), lit("Total por Região")
    ).otherwise(lit("Detalhe")))
    .orderBy(col("regiao").asc_nulls_last(), col("categoria").asc_nulls_last())
)

resultado_cube_labeled.show(20, truncate=False)

# COMMAND ----------

# grouping_id() — combinação de todos os grouping() em um único inteiro (bitmask)
# Útil para filtrar um nível específico
resultado_cube_id = (
    df
    .cube("regiao", "categoria")
    .agg(
        spark_sum("valor").alias("receita"),
        grouping_id("regiao", "categoria").alias("gid"),
        # gid = 0b11 = 3  → total geral (ambos agrupados)
        # gid = 0b10 = 2  → total por categoria (regiao agrupada)
        # gid = 0b01 = 1  → total por regiao (categoria agrupada)
        # gid = 0b00 = 0  → detalhe (nenhum agrupado)
    )
)

# Filtrar apenas os totais por região (gid = 1):
resultado_cube_id.filter(col("gid") == 1).show()

# COMMAND ----------

# MAGIC %md
# ## 4. rollup — Subtotais hierárquicos

# COMMAND ----------

# MAGIC %md
# ### rollup vs cube
#
# ```
# df.rollup("regiao", "categoria", "produto")
#
# Gera agrupamentos HIERÁRQUICOS (da esquerda para a direita):
# ┌────────┬────────────┬──────────┬──────────────────────────────────────┐
# │ regiao │ categoria  │ produto  │ Representa                           │
# ├────────┼────────────┼──────────┼──────────────────────────────────────┤
# │ SP     │ Eletronicos│ Notebook │ Detalhe completo                     │
# │ SP     │ Eletronicos│ null     │ Subtotal SP + Eletronicos            │
# │ SP     │ null       │ null     │ Subtotal SP (todas categorias)       │
# │ null   │ null       │ null     │ Total GERAL                          │
# └────────┴────────────┴──────────┴──────────────────────────────────────┘
#
# cube com 3 dims gera 2^3 = 8 combinações
# rollup com 3 dims gera apenas N+1 = 4 combinações
#
# Use rollup para hierarquias naturais: Ano → Mês → Dia, País → Estado → Cidade
# Use cube quando qualquer combinação de dimensões é válida
# ```

# COMMAND ----------

# Rollup hierárquico: regiao → categoria → produto
resultado_rollup = (
    df
    .rollup("regiao", "categoria", "produto")
    .agg(
        spark_sum("valor").alias("receita"),
        count("*").alias("pedidos"),
        grouping_id("regiao", "categoria", "produto").alias("nivel"),
    )
    .orderBy(
        col("regiao").asc_nulls_last(),
        col("categoria").asc_nulls_last(),
        col("produto").asc_nulls_last(),
    )
)

resultado_rollup.show(30, truncate=False)

# COMMAND ----------

# Labeling dos níveis do rollup
resultado_rollup_labeled = (
    df
    .rollup("regiao", "categoria", "produto")
    .agg(
        spark_sum("valor").alias("receita"),
        grouping_id("regiao", "categoria", "produto").alias("gid"),
    )
    .withColumn("nivel_desc", when(col("gid") == 7, lit("🌎 Total Geral"))
                              .when(col("gid") == 3, lit("📍 Total por Região"))
                              .when(col("gid") == 1, lit("📦 Total por Categoria"))
                              .otherwise(lit("   Detalhe")))
    .orderBy(
        col("regiao").asc_nulls_last(),
        col("categoria").asc_nulls_last(),
        col("produto").asc_nulls_last(),
    )
    .select("nivel_desc", "regiao", "categoria", "produto", "receita")
)

resultado_rollup_labeled.show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# ## 5. Padrões avançados de agregação

# COMMAND ----------

# MAGIC %md
# ### 5a. Agregação em múltiplos níveis com union

# COMMAND ----------

# Às vezes você precisa de métricas em granularidades diferentes no mesmo DataFrame
from pyspark.sql.functions import lit

nivel_detalhe = (
    df.groupBy("regiao", "categoria")
    .agg(spark_sum("valor").alias("receita"), count("*").alias("pedidos"))
    .withColumn("nivel", lit("detalhe"))
)

nivel_regiao = (
    df.groupBy("regiao")
    .agg(spark_sum("valor").alias("receita"), count("*").alias("pedidos"))
    .withColumn("categoria", lit("TODOS"))
    .withColumn("nivel", lit("regiao"))
)

nivel_total = (
    df.agg(spark_sum("valor").alias("receita"), count("*").alias("pedidos"))
    .withColumn("regiao", lit("TODAS"))
    .withColumn("categoria", lit("TODOS"))
    .withColumn("nivel", lit("total"))
)

resultado_multinivel = (
    nivel_detalhe
    .union(nivel_regiao.select(nivel_detalhe.columns))
    .union(nivel_total.select(nivel_detalhe.columns))
    .orderBy("nivel", "regiao", "categoria")
)

resultado_multinivel.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ### 5b. Top-N por grupo (groupBy + window vs join)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, dense_rank

# Top 2 produtos por receita dentro de cada região
window_regiao = Window.partitionBy("regiao").orderBy(col("receita").desc())

top_por_regiao = (
    df
    .groupBy("regiao", "produto")
    .agg(spark_sum("valor").alias("receita"))
    .withColumn("rank", dense_rank().over(window_regiao))
    .filter(col("rank") <= 2)
    .orderBy("regiao", "rank")
)

top_por_regiao.show()

# COMMAND ----------

# MAGIC %md
# ### 5c. Percentil e distribuição estatística por grupo

# COMMAND ----------

df.groupBy("regiao").agg(
    count("*").alias("n"),
    spark_round(avg("valor"), 2).alias("media"),
    spark_round(stddev("valor"), 2).alias("std"),
    percentile_approx("valor", 0.25).alias("p25"),
    percentile_approx("valor", 0.50).alias("p50_mediana"),
    percentile_approx("valor", 0.75).alias("p75"),
    percentile_approx("valor", 0.90).alias("p90"),
    percentile_approx("valor", 0.99).alias("p99"),
    corr("valor", "quantidade").alias("correlacao_valor_qtd"),
).show()

# COMMAND ----------

# MAGIC %md
# ### 5d. Agregação com filter pós-groupBy vs having em SQL

# COMMAND ----------

# PySpark: filter depois do agg (equivalente ao HAVING em SQL)
regioes_relevantes = (
    df
    .groupBy("regiao")
    .agg(
        spark_sum("valor").alias("receita_total"),
        count("*").alias("num_pedidos"),
    )
    .filter(col("receita_total") > 10000)  # equivale ao HAVING em SQL
    .filter(col("num_pedidos") >= 3)
)
regioes_relevantes.show()

# COMMAND ----------

# Equivalente em SQL:
df.createOrReplaceTempView("vendas")
spark.sql("""
    SELECT regiao,
           SUM(valor)  AS receita_total,
           COUNT(*)    AS num_pedidos
    FROM vendas
    GROUP BY regiao
    HAVING SUM(valor) > 10000
       AND COUNT(*) >= 3
    ORDER BY receita_total DESC
""").show()

# COMMAND ----------

# MAGIC %md
# ## 6. Comportamento com NULL nas agregações

# COMMAND ----------

# MAGIC %md
# **Regras importantes:**
# - `count(*)` conta todas as linhas, inclusive nulls
# - `count(coluna)` ignora nulls naquela coluna
# - `sum`, `avg`, `min`, `max` ignoram nulls automaticamente
# - Em `groupBy`, todas as linhas com null na chave são agrupadas juntas (null = null)
# - Em `pivot`, valores null na coluna pivotada geram uma coluna `null`

# COMMAND ----------

# Observar comportamento com a linha que tem regiao = null no dataset
df.groupBy("regiao").agg(
    count("*").alias("total_linhas"),
    count("regiao").alias("linhas_nao_nulas"),   # não conta as que têm regiao null
    spark_sum("valor").alias("receita"),
).orderBy(col("regiao").asc_nulls_last()).show()

# A linha com regiao = null aparece como grupo separado
# count("*") = 1, count("regiao") = 0 para o grupo null

# COMMAND ----------

# MAGIC %md
# ## ⚠️ Resumo de armadilhas e pontos de prova
#
# | Tema | O que saber |
# |---|---|
# | `count(*)` vs `count(col)` | `count(*)` conta nulls; `count(col)` ignora nulls |
# | `groupBy` + null | Linhas com null na chave são agrupadas juntas como um grupo "null" |
# | `pivot` sem lista de valores | Dispara 2 jobs — em produção sempre especifique os valores |
# | `cube` com N dims | Gera 2^N agrupamentos — pode ser caro para N grande |
# | `rollup` vs `cube` | rollup: hierárquico N+1 combinações. cube: todas as 2^N combinações |
# | `grouping()` | Retorna 1 se coluna foi agregada (subtotal), 0 se tem valor real |
# | `grouping_id()` | Bitmask de todos os `grouping()` — filtrar nível específico de cube/rollup |
# | HashAggregate parcial | O Spark agrega parcialmente em cada partição ANTES do shuffle — eficiente |
# | `collect_list` vs `collect_set` | list preserva ordem e duplicatas; set remove duplicatas |
# | HAVING em PySpark | É um `.filter()` após o `.agg()` — não existe palavra-chave HAVING |

# COMMAND ----------
