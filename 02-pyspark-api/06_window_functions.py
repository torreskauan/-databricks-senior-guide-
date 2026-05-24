# Databricks notebook source

# MAGIC %md
# # 06 — Window Functions: row_number, rank, lag, lead, frames
#
# **Analogia:**
# Imagine que você está em uma corrida de Fórmula 1 com múltiplas etapas.
# As funções de janela são como um **painel que exibe informações contextuais de cada piloto
# sem eliminar nenhuma linha da tabela**.
#
# - `row_number`: qual é a posição do piloto na classificação geral desta etapa?
# - `rank`: qual é o rank, mas se dois pilotos empatam, ambos ficam no mesmo lugar
#   (e o próximo pula o número)?
# - `dense_rank`: igual ao rank, mas o próximo número não pula
# - `lag`: qual foi o tempo do piloto na etapa ANTERIOR?
# - `lead`: qual será o tempo na etapa SEGUINTE?
# - Frames (`ROWS BETWEEN`): qual foi o melhor tempo nas últimas 3 etapas?
#
# **Conceito técnico:**
# Window functions calculam um valor para cada linha baseando-se em um conjunto de linhas
# relacionadas (a "janela"). Diferente de `groupBy`, as linhas originais são preservadas —
# a função adiciona uma coluna nova sem reduzir o número de linhas.
#
# Toda window function opera sobre um `WindowSpec` composto de:
# - **PARTITION BY:** divide os dados em grupos independentes (como um groupBy, mas sem reduzir)
# - **ORDER BY:** define a ordenação dentro de cada partição
# - **FRAME:** define quais linhas ao redor da linha atual participam do cálculo
#
# **Quando usar este conhecimento:**
# - Rankings, top-N por grupo, deduplicação por chave composta
# - Cálculo de variações (MoM, YoY, delta em relação à linha anterior)
# - Médias móveis, running totals, cumulative sums
# - SCD Type 2, first/last value por grupo
# - Entrevistas sênior: window function é questão garantida

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, row_number, rank, dense_rank, percent_rank, ntile,
    lag, lead, first, last,
    sum as spark_sum, avg, min as spark_min, max as spark_max, count,
    round as spark_round, lit, when
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Dataset base: vendas mensais por vendedor e região
schema = StructType([
    StructField("vendedor",   StringType(), True),
    StructField("regiao",     StringType(), True),
    StructField("mes",        StringType(), True),
    StructField("receita",    DoubleType(), True),
    StructField("pedidos",    LongType(),   True),
])

dados = [
    ("Ana",    "SP", "2024-01", 12000.0, 8),
    ("Ana",    "SP", "2024-02", 15000.0, 10),
    ("Ana",    "SP", "2024-03", 11000.0, 7),
    ("Ana",    "SP", "2024-04", 18000.0, 12),
    ("Bruno",  "RJ", "2024-01",  8000.0, 5),
    ("Bruno",  "RJ", "2024-02",  8000.0, 5),  # empate intencional
    ("Bruno",  "RJ", "2024-03", 14000.0, 9),
    ("Bruno",  "RJ", "2024-04",  9000.0, 6),
    ("Carla",  "SP", "2024-01", 20000.0, 14),
    ("Carla",  "SP", "2024-02", 17000.0, 11),
    ("Carla",  "SP", "2024-03", 22000.0, 15),
    ("Carla",  "SP", "2024-04", 16000.0, 10),
    ("Diana",  "MG", "2024-01",  9500.0, 6),
    ("Diana",  "MG", "2024-02", 11000.0, 7),
    ("Diana",  "MG", "2024-03", 10500.0, 7),
    ("Diana",  "MG", "2024-04", 13000.0, 9),
    ("Eduardo","MG", "2024-01",  7000.0, 4),
    ("Eduardo","MG", "2024-02",  9000.0, 6),
    ("Eduardo","MG", "2024-03",  7500.0, 5),
    ("Eduardo","MG", "2024-04", 12000.0, 8),
]

df = spark.createDataFrame(dados, schema=schema)
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ## 1. WindowSpec — A Especificação da Janela

# COMMAND ----------

# MAGIC %md
# ### Anatomia de um WindowSpec
#
# ```
# Window
#   .partitionBy("coluna_de_agrupamento")   ← divide em grupos independentes
#   .orderBy("coluna_de_ordenacao")         ← ordem dentro de cada grupo
#   .rowsBetween(inicio, fim)               ← frame: quais linhas participam
#               ou
#   .rangeBetween(inicio, fim)              ← frame por valor, não por posição
#
# Constantes de frame:
#   Window.unboundedPreceding  → desde o início da partição
#   Window.unboundedFollowing  → até o fim da partição
#   Window.currentRow          → a linha atual
#   -N                         → N linhas antes da atual
#   +N                         → N linhas depois da atual
# ```

# COMMAND ----------

# Definindo WindowSpecs reutilizáveis

# Janela por vendedor, ordenada por mês (para funções de sequência temporal)
w_vendedor = Window.partitionBy("vendedor").orderBy("mes")

# Janela por região, ordenada por receita desc (para rankings)
w_regiao_rank = Window.partitionBy("regiao").orderBy(col("receita").desc())

# Janela global ordenada por receita (ranking geral — sem PARTITION BY)
w_global_rank = Window.orderBy(col("receita").desc())

# Janela para acumulados: desde o início até a linha atual
w_vendedor_acum = (
    Window.partitionBy("vendedor")
    .orderBy("mes")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

# Janela para médias móveis: 2 linhas antes + linha atual (janela de 3)
w_vendedor_movel = (
    Window.partitionBy("vendedor")
    .orderBy("mes")
    .rowsBetween(-2, Window.currentRow)
)

print("WindowSpecs definidos.")

# COMMAND ----------

# MAGIC %md
# ## 2. Funções de Ranking

# COMMAND ----------

# MAGIC %md
# ### row_number, rank, dense_rank, percent_rank, ntile
#
# ```
# Receitas de Bruno (RJ) para ilustrar diferenças:
# 2024-01: 8000  │ row_number=1 │ rank=1 │ dense_rank=1
# 2024-02: 8000  │ row_number=2 │ rank=1 │ dense_rank=1  ← EMPATE
# 2024-03: 14000 │ row_number=3 │ rank=3 │ dense_rank=2  ← rank pula para 3, dense_rank não pula
# 2024-04: 9000  │ row_number=4 │ rank=4 │ dense_rank=3
#
# row_number: sempre sequencial, nunca repete — desempate arbitrário (por ORDER BY)
# rank:       empates recebem o mesmo número; o próximo PULA posições
# dense_rank: empates recebem o mesmo número; o próximo NÃO pula
# ```

# COMMAND ----------

df_ranking = df.withColumn("row_num",     row_number().over(w_regiao_rank)) \
               .withColumn("rank",        rank().over(w_regiao_rank)) \
               .withColumn("dense_rank",  dense_rank().over(w_regiao_rank)) \
               .withColumn("pct_rank",    spark_round(percent_rank().over(w_regiao_rank), 2)) \
               .withColumn("quartil",     ntile(4).over(w_regiao_rank))

df_ranking.select("regiao", "vendedor", "mes", "receita",
                  "row_num", "rank", "dense_rank", "pct_rank", "quartil") \
          .orderBy("regiao", "rank").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ### Diferenças práticas:
#
# | Função | Empate | Próximo número | Uso típico |
# |---|---|---|---|
# | `row_number` | Desempata arbitrariamente por ORDER BY | Sempre sequencial | Deduplicação: pegar 1 linha por grupo |
# | `rank` | Mesmo número | Pula (1,1,3,4) | Rankings esportivos, classificações |
# | `dense_rank` | Mesmo número | Não pula (1,1,2,3) | Rankings sem lacunas |
# | `percent_rank` | Percentual de 0.0 a 1.0 | — | Distribuição percentual |
# | `ntile(n)` | Divide em n buckets | — | Quartis, decis, percentis aproximados |

# COMMAND ----------

# MAGIC %md
# ### Padrão clássico: Top-N por grupo com row_number

# COMMAND ----------

# Top 2 vendedores por receita em cada região e mês
w_regiao_mes = Window.partitionBy("regiao", "mes").orderBy(col("receita").desc())

top2_por_regiao_mes = (
    df
    .withColumn("rn", row_number().over(w_regiao_mes))
    .filter(col("rn") <= 2)
    .drop("rn")
    .orderBy("regiao", "mes", col("receita").desc())
)

print("Top 2 vendedores por região/mês:")
top2_por_regiao_mes.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ### Padrão: deduplicação com row_number (pegar a linha mais recente por chave)

# COMMAND ----------

# Simular uma tabela com duplicatas — pegar apenas o registro mais recente por vendedor
dados_dup = dados + [
    ("Ana", "SP", "2024-04", 18500.0, 13),  # versão mais nova do mesmo mês
    ("Bruno", "RJ", "2024-04", 9200.0, 7),  # versão mais nova
]
df_dup = spark.createDataFrame(dados_dup, schema=schema)

w_dedup = Window.partitionBy("vendedor", "mes").orderBy(col("receita").desc())

df_deduplicado = (
    df_dup
    .withColumn("rn", row_number().over(w_dedup))
    .filter(col("rn") == 1)   # pega apenas a linha com maior receita (mais recente/atual)
    .drop("rn")
)

print(f"Antes da deduplicação: {df_dup.count()} linhas")
print(f"Depois da deduplicação: {df_deduplicado.count()} linhas")

# COMMAND ----------

# MAGIC %md
# ## 3. lag e lead — Acessando Linhas Vizinhas

# COMMAND ----------

# MAGIC %md
# ```
# lag(col, n, default)  → valor de n linhas ANTES da atual (passado)
# lead(col, n, default) → valor de n linhas DEPOIS da atual (futuro)
#
# Linha atual: Ana, 2024-02, receita=15000
# lag(receita, 1) → 12000 (Ana em 2024-01)
# lead(receita, 1) → 11000 (Ana em 2024-03)
#
# Primeira linha da partição: lag → null (ou default se especificado)
# Última linha da partição: lead → null (ou default se especificado)
# ```

# COMMAND ----------

df_temporal = (
    df
    .withColumn("receita_mes_anterior",  lag("receita", 1).over(w_vendedor))
    .withColumn("receita_prox_mes",      lead("receita", 1).over(w_vendedor))
    .withColumn("receita_2_meses_atras", lag("receita", 2, 0.0).over(w_vendedor))
    # variação absoluta mês a mês
    .withColumn("variacao_mom",
        col("receita") - lag("receita", 1).over(w_vendedor))
    # variação percentual mês a mês
    .withColumn("pct_variacao_mom",
        spark_round(
            (col("receita") - lag("receita", 1).over(w_vendedor))
            / lag("receita", 1).over(w_vendedor) * 100,
        1))
)

df_temporal.select("vendedor", "mes", "receita",
                   "receita_mes_anterior", "variacao_mom", "pct_variacao_mom") \
           .orderBy("vendedor", "mes").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ### Padrão: identificar sequências e gaps com lag/lead

# COMMAND ----------

# Identificar se houve crescimento ou queda em relação ao mês anterior
df_tendencia = (
    df
    .withColumn("receita_anterior", lag("receita", 1).over(w_vendedor))
    .withColumn("tendencia",
        when(col("receita_anterior").isNull(), lit("primeiro_mes"))
        .when(col("receita") > col("receita_anterior"), lit("📈 crescimento"))
        .when(col("receita") < col("receita_anterior"), lit("📉 queda"))
        .otherwise(lit("➡️  estável"))
    )
    .select("vendedor", "mes", "receita", "receita_anterior", "tendencia")
    .orderBy("vendedor", "mes")
)

df_tendencia.show(truncate=False)

# COMMAND ----------

# Padrão SCD Type 2: detectar mudanças usando lag para saber o valor anterior
# (lag na coluna que pode mudar → se diferente do atual → houve mudança)
dados_scd = [
    ("Ana",   "2024-01", "Silver"),
    ("Ana",   "2024-02", "Silver"),
    ("Ana",   "2024-03", "Gold"),    # mudou de tier
    ("Ana",   "2024-04", "Gold"),
    ("Bruno", "2024-01", "Bronze"),
    ("Bruno", "2024-02", "Silver"),  # mudou de tier
    ("Bruno", "2024-03", "Silver"),
]
df_scd = spark.createDataFrame(dados_scd, ["vendedor", "mes", "tier"])

w_scd = Window.partitionBy("vendedor").orderBy("mes")

df_mudancas = (
    df_scd
    .withColumn("tier_anterior", lag("tier", 1).over(w_scd))
    .withColumn("houve_mudanca",
        (col("tier") != col("tier_anterior")) | col("tier_anterior").isNull()
    )
    .filter(col("houve_mudanca"))
    .select("vendedor", "mes", "tier_anterior", "tier")
)

print("Registros onde o tier mudou (base para SCD Type 2):")
df_mudancas.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ## 4. Frames — ROWS BETWEEN e RANGE BETWEEN

# COMMAND ----------

# MAGIC %md
# ### ROWS BETWEEN vs RANGE BETWEEN
#
# ```
# ROWS BETWEEN: conta linhas físicas (por posição)
# ┌────────────────────────────────────────────────────────────────┐
# │  ROWS BETWEEN -2 AND CURRENT ROW (janela de 3 linhas)         │
# │                                                                │
# │  mes      receita  │ Linhas na janela    │ Resultado           │
# │  2024-01  12000    │ [12000]             │ avg = 12000         │
# │  2024-02  15000    │ [12000, 15000]      │ avg = 13500         │
# │  2024-03  11000    │ [12000,15000,11000] │ avg = 12667         │
# │  2024-04  18000    │ [15000,11000,18000] │ avg = 14667         │
# └────────────────────────────────────────────────────────────────┘
#
# RANGE BETWEEN: usa o VALOR da coluna ORDER BY (não a posição física)
# → Linhas com o mesmo valor no ORDER BY são tratadas como "na mesma posição"
# → Se ORDER BY for por data, RANGE BETWEEN trata datas iguais juntas
# → Use ROWS para resultados determinísticos quando há empates
# ```

# COMMAND ----------

# Running total (acumulado desde o início)
# Média móvel de 3 meses
# Min/Max rolling
df_frames = (
    df
    .withColumn("running_total",
        spark_sum("receita").over(w_vendedor_acum))
    .withColumn("media_movel_3m",
        spark_round(avg("receita").over(w_vendedor_movel), 0))
    .withColumn("max_rolling_3m",
        spark_max("receita").over(w_vendedor_movel))
    .withColumn("min_rolling_3m",
        spark_min("receita").over(w_vendedor_movel))
    .select("vendedor", "mes", "receita",
            "running_total", "media_movel_3m", "max_rolling_3m")
    .orderBy("vendedor", "mes")
)

df_frames.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ### Frames mais comuns com suas definições

# COMMAND ----------

# Exemplos dos frames mais usados na prática

w_base = Window.partitionBy("vendedor").orderBy("mes")

frames_demo = (
    df
    # Acumulado desde o início da partição até a linha atual
    .withColumn("acumulado",
        spark_sum("receita").over(
            w_base.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        ))
    # Total da partição inteira (mesmo valor em todas as linhas do grupo)
    .withColumn("total_vendedor",
        spark_sum("receita").over(
            w_base.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        ))
    # Participação percentual de cada mês no total do vendedor
    .withColumn("pct_do_total",
        spark_round(col("receita") /
            spark_sum("receita").over(
                w_base.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ) * 100, 1))
    # Média dos 2 vizinhos (1 antes + atual + 1 depois)
    .withColumn("media_centrada_3",
        spark_round(avg("receita").over(
            w_base.rowsBetween(-1, 1)
        ), 0))
    .select("vendedor", "mes", "receita", "acumulado",
            "total_vendedor", "pct_do_total", "media_centrada_3")
    .orderBy("vendedor", "mes")
)

frames_demo.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ## 5. first, last — Primeiro e Último Valor da Janela

# COMMAND ----------

# MAGIC %md
# ```
# first(col, ignorenulls=False): primeiro valor da janela definida pelo frame
# last(col, ignorenulls=False):  último valor da janela definida pelo frame
#
# ⚠️ Sem ORDER BY: first/last não são determinísticos (qualquer linha pode ser a "primeira")
# Com ORDER BY + frame unbounded: first = primeiro da partição, last = último
# ```

# COMMAND ----------

w_full = (Window.partitionBy("vendedor")
                .orderBy("mes")
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))

df_first_last = (
    df
    .withColumn("primeiro_mes_receita", first("receita").over(w_full))
    .withColumn("ultimo_mes_receita",   last("receita").over(w_full))
    .withColumn("delta_inicio_fim",
        col("ultimo_mes_receita") - col("primeiro_mes_receita"))
    .select("vendedor", "mes", "receita",
            "primeiro_mes_receita", "ultimo_mes_receita", "delta_inicio_fim")
    .orderBy("vendedor", "mes")
)

df_first_last.show(truncate=False)

# COMMAND ----------

# Padrão: preencher nulos com o último valor não-nulo (forward fill)
dados_com_null = [
    ("Ana", "2024-01", 12000.0),
    ("Ana", "2024-02", None),
    ("Ana", "2024-03", None),
    ("Ana", "2024-04", 18000.0),
]
df_null = spark.createDataFrame(dados_com_null, ["vendedor", "mes", "receita"])

w_ffill = (Window.partitionBy("vendedor")
                 .orderBy("mes")
                 .rowsBetween(Window.unboundedPreceding, Window.currentRow))

df_ffill = df_null.withColumn(
    "receita_preenchida",
    last("receita", ignorenulls=True).over(w_ffill)  # last não-nulo antes da linha atual
)

print("Forward fill com last(ignorenulls=True):")
df_ffill.show()

# COMMAND ----------

# MAGIC %md
# ## 6. Window sem PARTITION BY — Janela Global

# COMMAND ----------

# Sem partitionBy: a janela é o DataFrame inteiro
# Útil para rankings globais ou participação no total geral

w_global = Window.orderBy(col("receita").desc())

df_global = (
    df
    .withColumn("rank_global", rank().over(w_global))
    .withColumn("total_geral",
        spark_sum("receita").over(
            Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        ))
    .withColumn("pct_total_geral",
        spark_round(col("receita") /
            spark_sum("receita").over(
                Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ) * 100, 1))
    .select("vendedor", "mes", "receita", "rank_global", "pct_total_geral")
    .orderBy("rank_global")
)

print("Ranking global e participação no total geral:")
df_global.show(10)

# COMMAND ----------

# MAGIC %md
# ### ⚠️ Window sem PARTITION BY é perigoso em escala
#
# Sem `partitionBy`, TODOS os dados vão para uma única partição antes do cálculo.
# Em um DataFrame de bilhões de linhas → OOM garantido.
#
# **Regra:** sempre use `partitionBy` quando o volume for grande.
# Se você realmente precisa do total global → calcule separadamente com `agg` e faça join.

# COMMAND ----------

# Alternativa segura para total global em escala
total_global = df.agg(spark_sum("receita").alias("total_geral")).collect()[0][0]

df_pct_seguro = df.withColumn("pct_total_geral",
    spark_round(col("receita") / lit(total_global) * 100, 1))

df_pct_seguro.select("vendedor", "mes", "receita", "pct_total_geral") \
             .orderBy(col("receita").desc()).show(5)

# COMMAND ----------

# MAGIC %md
# ## 7. Window em SQL

# COMMAND ----------

df.createOrReplaceTempView("vendas")

spark.sql("""
    SELECT
        vendedor,
        mes,
        receita,

        -- Ranking por região
        ROW_NUMBER() OVER (PARTITION BY regiao ORDER BY receita DESC)  AS row_num,
        RANK()       OVER (PARTITION BY regiao ORDER BY receita DESC)  AS rnk,
        DENSE_RANK() OVER (PARTITION BY regiao ORDER BY receita DESC)  AS dense_rnk,

        -- Comparação temporal
        LAG(receita,  1, 0) OVER (PARTITION BY vendedor ORDER BY mes) AS receita_anterior,
        LEAD(receita, 1, 0) OVER (PARTITION BY vendedor ORDER BY mes) AS receita_proxima,

        -- Acumulado e rolling
        SUM(receita) OVER (
            PARTITION BY vendedor
            ORDER BY mes
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS acumulado,

        AVG(receita) OVER (
            PARTITION BY vendedor
            ORDER BY mes
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS media_movel_3m,

        -- Total da partição
        SUM(receita) OVER (PARTITION BY vendedor) AS total_vendedor

    FROM vendas
    ORDER BY vendedor, mes
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ## 8. Performance de Window Functions

# COMMAND ----------

# MAGIC %md
# ### O que acontece internamente
#
# ```
# Window function no Physical Plan:
#
# *(2) Window [row_number() windowspecdefinition(regiao, receita DESC, ...)]
#  +- *(1) Sort [regiao ASC, receita DESC]
#      +- Exchange hashpartitioning(regiao, N)
#
# Custo:
# 1. Exchange: shuffle por PARTITION BY (cada partição vai para o mesmo Executor)
# 2. Sort: ordenação dentro de cada partição (por ORDER BY)
# 3. Window: cálculo da função sobre as linhas ordenadas
#
# Multiple window functions na MESMA partitionBy + orderBy:
# → Spark otimiza em um único Sort + único Exchange → eficiente
#
# Window functions com DIFERENTES partitionBy:
# → Cada partitionBy diferente = Exchange separado = shuffle extra
# → Agrupe window functions com o mesmo WindowSpec sempre que possível
# ```

# COMMAND ----------

# Bom: múltiplas funções no mesmo WindowSpec → 1 shuffle, 1 sort
df_otimizado = (
    df
    .withColumn("rn",         row_number().over(w_vendedor))
    .withColumn("rnk",        rank().over(w_vendedor))
    .withColumn("acumulado",  spark_sum("receita").over(w_vendedor_acum))
    .withColumn("lag_rec",    lag("receita", 1).over(w_vendedor))
)
# Todos usam o mesmo partitionBy("vendedor").orderBy("mes") → 1 Exchange

print("=== Plano com WindowSpecs compatíveis (eficiente) ===")
df_otimizado.explain(mode="simple")

# COMMAND ----------

# Ruim: WindowSpecs diferentes → shuffles extras
w_por_regiao = Window.partitionBy("regiao").orderBy(col("receita").desc())

df_ineficiente = (
    df
    .withColumn("rank_por_vendedor", rank().over(w_vendedor))  # partitionBy vendedor
    .withColumn("rank_por_regiao",   rank().over(w_por_regiao))  # partitionBy regiao → novo shuffle
)

print("=== Plano com WindowSpecs diferentes (2 shuffles) ===")
df_ineficiente.explain(mode="simple")

# COMMAND ----------

# MAGIC %md
# ## ⚠️ Resumo de armadilhas e pontos de prova
#
# | Tema | O que saber |
# |---|---|
# | `row_number` vs `rank` vs `dense_rank` | row_number: sempre único. rank: pula posição no empate. dense_rank: não pula |
# | Window sem `partitionBy` | Toda a tabela vai para 1 partição → OOM em escala |
# | `lag`/`lead` na primeira/última linha | Retorna null por padrão — use o 3º argumento para default |
# | `ROWS BETWEEN` vs `RANGE BETWEEN` | ROWS: por posição física. RANGE: por valor do ORDER BY — empates juntos |
# | Frame default sem ORDER BY | `ROWS BETWEEN unboundedPreceding AND unboundedFollowing` (toda a partição) |
# | Frame default com ORDER BY | `RANGE BETWEEN unboundedPreceding AND currentRow` — acumulado implícito |
# | `first`/`last` sem ORDER BY | Não determinístico — qualquer linha pode ser retornada |
# | `last(ignorenulls=True)` | Forward fill — valor não-nulo mais recente antes da linha atual |
# | WindowSpecs iguais → 1 shuffle | Agrupar funções com mesmo `partitionBy + orderBy` = eficiente |
# | WindowSpecs diferentes → shuffles extras | Cada `partitionBy` diferente = Exchange extra no plano |
# | Top-N por grupo | `row_number().over(w) <= N` — padrão mais eficiente |
# | Deduplicação | `row_number().over(w) == 1` + drop coluna auxiliar |

# COMMAND ----------
