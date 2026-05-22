# Databricks notebook source

# MAGIC %md
# # 02 — DAG, Jobs, Stages, Tasks e Narrow vs Wide
#
# **Analogia:**
# Imagine que você vai fazer um jantar elaborado. O **DAG** é a receita completa com todas as etapas
# e dependências anotadas. Um **Job** é o jantar inteiro sendo executado. As **Stages** são as
# "fases" da preparação — você não pode montar o prato antes de cozinhar os ingredientes.
# As **Tasks** são os cozinheiros individuais trabalhando em paralelo — cada um cuida de uma
# porção específica. **Narrow transformations** são quando um cozinheiro trabalha só com os
# ingredientes da sua bancada. **Wide transformations** são quando todos precisam trocar
# ingredientes entre si antes de continuar — isso força uma sincronização.
#
# **Conceito técnico:**
# O Spark usa um **DAG (Directed Acyclic Graph)** para representar o plano lógico de execução.
# Quando uma **action** é chamada, o DAG Scheduler divide o grafo em **Stages**, separadas por
# **shuffle boundaries** (wide transformations). Cada Stage é dividida em **Tasks** — uma por
# partição. Narrow transformations não movem dados entre partições; wide transformations exigem
# um shuffle de dados entre os Executors.
#
# **Quando usar este conhecimento:**
# - Ao ler o Spark UI para diagnosticar gargalos
# - Ao escrever código PySpark/SQL mais eficiente (reduzir shuffles)
# - Para entender por que uma query é lenta e onde intervir
# - Entrevistas e prova de certificação Databricks

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, broadcast

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# ## 1. DAG — Directed Acyclic Graph

# COMMAND ----------

# MAGIC %md
# O Spark é **lazy**: nenhuma transformação é executada quando você a define.
# O Spark apenas registra o que precisa ser feito, construindo um grafo de dependências.
#
# **Directed** → as dependências têm direção (A → B → C), sem ciclos
# **Acyclic**  → um nó nunca depende de si mesmo ou de um descendente
# **Graph**    → múltiplos caminhos podem convergir (ex: joins)
#
# O grafo só é executado quando uma **action** é chamada:
# - `collect()`, `show()`, `count()`, `write()`, `save()`, `first()`, `take(n)`
#
# **Transformações** (lazy — apenas constroem o DAG):
# - `select()`, `filter()`, `withColumn()`, `groupBy()`, `join()`, `orderBy()`

# COMMAND ----------

# Exemplo: construindo um DAG sem executar nada ainda
df_raw = spark.range(1_000_000)              # transformação → lazy
df_filtrado = df_raw.filter(col("id") % 2 == 0)   # transformação → lazy
df_dobrado = df_filtrado.withColumn("dobro", col("id") * 2)  # transformação → lazy

# Neste ponto, NADA foi executado — apenas o plano foi criado.
# Para visualizar o plano lógico:
df_dobrado.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# ```
# DAG construído até aqui:
#
#   spark.range(1_000_000)         ← fonte de dados
#           │
#           ▼
#   filter(id % 2 == 0)            ← narrow (opera na mesma partição)
#           │
#           ▼
#   withColumn("dobro", id * 2)    ← narrow (opera na mesma partição)
#           │
#           ▼
#   [sem action → nada executado]
# ```

# COMMAND ----------

# MAGIC %md
# ## 2. Jobs — A Unidade de Execução

# COMMAND ----------

# Um Job é criado toda vez que uma ACTION é chamada.
# Um único script pode gerar múltiplos Jobs.

# Este count() dispara um JOB:
total = df_dobrado.count()   # ← action → dispara Job 1
print(f"Total de linhas: {total}")

# Este show() dispara outro JOB:
df_dobrado.show(5)           # ← action → dispara Job 2

# COMMAND ----------

# MAGIC %md
# ### Identificando Jobs no Spark UI
#
# No Spark UI (aba **Jobs**) você verá:
# - `Job ID` — numerado sequencialmente (0, 1, 2...)
# - `Description` — geralmente o nome da action que disparou o job
# - `Submitted` — timestamp
# - `Duration` — tempo total
# - `Stages` — quantas stages o job tem (ex: `2/2` = 2 completas de 2 total)
# - `Tasks` — total de tasks executadas

# COMMAND ----------

# MAGIC %md
# ## 3. Stages — Separadas por Shuffle

# COMMAND ----------

# MAGIC %md
# O DAG Scheduler quebra o grafo em Stages usando uma regra simples:
#
# **Nova Stage sempre que houver uma wide transformation (shuffle)**
#
# ```
# Stage 1 ──────────────────────┐
#   read → filter → withColumn  │ narrow → tudo na mesma partição
#                               │
#                         SHUFFLE BOUNDARY ← groupBy, join, orderBy, distinct...
#                               │
# Stage 2 ──────────────────────┘
#   agg → resultado final
# ```
#
# Cada Stage só pode começar quando a Stage anterior terminar completamente.
# É por isso que Stages criam "barreiras de sincronização".

# COMMAND ----------

# Exemplo com 2 Stages: leitura + groupBy (shuffle)
df_exemplo = spark.createDataFrame(
    [(1, "A", 100), (2, "B", 200), (1, "A", 150), (2, "B", 50), (3, "C", 300)],
    ["id", "categoria", "valor"]
)

resultado = (
    df_exemplo
    .filter(col("valor") > 50)           # narrow — Stage 1
    .withColumn("valor_2x", col("valor") * 2)  # narrow — Stage 1
    .groupBy("categoria")                # ← SHUFFLE BOUNDARY → Nova Stage
    .agg(spark_sum("valor").alias("total_valor"))  # narrow — Stage 2
)

resultado.show()
# Spark UI → Job disparado → 2 Stages visíveis

# COMMAND ----------

# MAGIC %md
# ### Stage Skipping — Reutilização de cache
#
# Se um DataFrame foi cacheado, o Spark pula a(s) Stage(s) que o produziram.
# Você verá stages com status `Skipped` no Spark UI.

df_cacheado = df_exemplo.filter(col("valor") > 50).cache()
df_cacheado.count()  # materializa o cache — Stage 1 executa

# Na próxima action, a Stage que produzia df_cacheado será pulada:
df_cacheado.groupBy("categoria").count().show()  # Stage 1 = Skipped

df_cacheado.unpersist()  # libera o cache após uso

# COMMAND ----------

# MAGIC %md
# ## 4. Tasks — A Unidade Mínima de Trabalho

# COMMAND ----------

# MAGIC %md
# Cada Stage é dividida em Tasks.
# **Número de Tasks de uma Stage = número de partições do DataFrame naquela Stage**
#
# ```
# Stage 1 — 8 partições → 8 Tasks rodando em paralelo (uma por partição)
#
# Partição 0 → Task 0 (Executor 1, slot 1)
# Partição 1 → Task 1 (Executor 1, slot 2)
# Partição 2 → Task 2 (Executor 2, slot 1)
# Partição 3 → Task 3 (Executor 2, slot 2)
# Partição 4 → Task 4 (Executor 3, slot 1)
# Partição 5 → Task 5 (Executor 3, slot 2)
# Partição 6 → Task 6 (Executor 4, slot 1)
# Partição 7 → Task 7 (Executor 4, slot 2)
# ```

# COMMAND ----------

# Verificar o número de partições (= número de tasks da primeira Stage)
df_lido = spark.range(1_000_000)
print(f"Partições (tasks na Stage de leitura): {df_lido.rdd.getNumPartitions()}")

# Após um shuffle (groupBy, join), o número de partições muda:
# spark.sql.shuffle.partitions define quantas partições o shuffle vai criar
# Default: 200 (pode ser muito alto para clusters pequenos)
print(f"spark.sql.shuffle.partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# Com AQE ativado, esse valor é ajustado automaticamente:
print(f"AQE ativado: {spark.conf.get('spark.sql.adaptive.enabled')}")

# COMMAND ----------

# MAGIC %md
# ### Task Metrics no Spark UI
#
# Na aba **Stages → Tasks**, você consegue ver por task:
#
# | Métrica | O que significa |
# |---|---|
# | `Duration` | Tempo total da task |
# | `GC Time` | Tempo gasto em Garbage Collection (alto = problema de memória) |
# | `Input Size` | Bytes lidos da fonte (disco/S3/ADLS) |
# | `Shuffle Read` | Bytes recebidos de outras partições via shuffle |
# | `Shuffle Write` | Bytes enviados para outras partições via shuffle |
# | `Spill (Memory)` | Dados que foram de memória para disco — ruim |
# | `Spill (Disk)` | Tamanho dos dados espilhados em disco |
#
# **Sinal de alerta:** tasks com duração muito discrepante entre si → **data skew**

# COMMAND ----------

# MAGIC %md
# ## 5. Narrow vs Wide Transformations

# COMMAND ----------

# MAGIC %md
# ### Narrow Transformations
#
# Cada partição de saída depende de **apenas uma** partição de entrada.
# Nenhum dado precisa se mover entre nós.
# Podem ser executadas em **pipeline** — sem shuffle, sem barreira de sincronização.
#
# ```
# Partição 0 (Exec 1) ──→ Partição 0 saída (Exec 1)   ← dados ficam no mesmo lugar
# Partição 1 (Exec 2) ──→ Partição 1 saída (Exec 2)
# Partição 2 (Exec 3) ──→ Partição 2 saída (Exec 3)
# ```

# COMMAND ----------

# Exemplos de NARROW transformations:
df_base = spark.range(100).toDF("numero")

narrow_exemplos = (
    df_base
    .filter(col("numero") > 10)              # narrow: filtra linha a linha
    .select(col("numero"), (col("numero") * 2).alias("dobro"))  # narrow: mapeia linha a linha
    .withColumn("par", col("numero") % 2 == 0)  # narrow: calcula coluna nova por linha
    .limit(50)                               # narrow (mas força 1 partição — cuidado!)
)

# map, flatMap, mapPartitions também são narrow (nível RDD)
# union() é narrow se os schemas são compatíveis (sem shuffle)

print("Narrow transformations: nenhum shuffle no plano abaixo")
narrow_exemplos.explain(mode="simple")

# COMMAND ----------

# MAGIC %md
# ### Wide Transformations (Shuffle)
#
# Cada partição de saída depende de **múltiplas** partições de entrada.
# O Spark precisa redistribuir os dados entre todos os Executors — **shuffle**.
# Shuffles são caros: serialização, escrita em disco, transferência de rede, leitura.
#
# ```
# Partição 0 (Exec 1) ──┬──→ Partição 0 saída (Exec 1)   ← dados de qualquer lugar
# Partição 1 (Exec 2) ──┼──→ Partição 1 saída (Exec 2)
# Partição 2 (Exec 3) ──┴──→ Partição 2 saída (Exec 3)
#                ↑
#           SHUFFLE: todos os Executors trocam dados entre si
# ```

# COMMAND ----------

# Exemplos de WIDE transformations:
df_vendas = spark.createDataFrame(
    [(1, "SP", 500), (2, "RJ", 300), (1, "SP", 200), (3, "MG", 400), (2, "RJ", 100)],
    ["vendedor_id", "estado", "valor"]
)

df_clientes = spark.createDataFrame(
    [(1, "Ana"), (2, "Bruno"), (3, "Carla")],
    ["vendedor_id", "nome"]
)

# groupBy → WIDE (shuffle para agrupar por chave)
por_estado = df_vendas.groupBy("estado").agg(spark_sum("valor").alias("total"))

# join → WIDE (shuffle para alinhar chaves — exceto Broadcast Join)
com_nome = df_vendas.join(df_clientes, on="vendedor_id", how="inner")

# orderBy → WIDE (shuffle para ordenação global)
ordenado = df_vendas.orderBy("valor")

# distinct → WIDE (shuffle para deduplicar globalmente)
estados_unicos = df_vendas.select("estado").distinct()

# repartition(n) → WIDE (redistribui partições)
redistribuido = df_vendas.repartition(4)

# COMMAND ----------

# MAGIC %md
# ### Tabela de referência: Narrow vs Wide
#
# | Transformação | Tipo | Motivo |
# |---|---|---|
# | `filter()` / `where()` | Narrow | opera por linha, sem mover dados |
# | `select()` / `withColumn()` | Narrow | mapeia por linha |
# | `map()` / `flatMap()` | Narrow | por registro |
# | `union()` | Narrow | concatena sem mover dados entre partições |
# | `coalesce()` | Narrow | reduz partições sem shuffle completo |
# | `limit()` | Narrow* | *mas força 1 partição no final — cuidado |
# | `groupBy()` + agg | **Wide** | agrupa por chave → shuffle |
# | `join()` (sem broadcast) | **Wide** | alinha chaves entre partições → shuffle |
# | `orderBy()` / `sort()` | **Wide** | ordenação global → shuffle |
# | `distinct()` | **Wide** | deduplicação global → shuffle |
# | `repartition(n)` | **Wide** | redistribuição full → shuffle |
# | `groupByKey()` | **Wide** | RDD — shuffle por chave |
# | `reduceByKey()` | **Wide** | RDD — shuffle (mais eficiente que groupByKey) |

# COMMAND ----------

# MAGIC %md
# ## 6. Como reduzir Shuffles na prática

# COMMAND ----------

# MAGIC %md
# ### Técnica 1: Broadcast Join
# Evita o shuffle de tabelas pequenas

df_pequeno = spark.createDataFrame(
    [(1, "Ana"), (2, "Bruno"), (3, "Carla")],
    ["id", "nome"]
)

df_grande = spark.range(1_000_000).withColumnRenamed("id", "usuario_id")

# Sem broadcast → Shuffle Join (wide)
join_normal = df_grande.join(df_pequeno, df_grande.usuario_id == df_pequeno.id)

# Com broadcast → Broadcast Hash Join (narrow — envia a tabela pequena para todos os Executors)
join_broadcast = df_grande.join(broadcast(df_pequeno), df_grande.usuario_id == df_pequeno.id)

# O Spark faz broadcast automaticamente quando:
# tabela < spark.sql.autoBroadcastJoinThreshold (default: 10 MB)
print("Broadcast threshold:", spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

# COMMAND ----------

# MAGIC %md
# ### Técnica 2: Filtrar ANTES do join
# Reduz o volume de dados que vai para o shuffle

# ❌ Menos eficiente: join primeiro, filtra depois
# resultado = df_grande.join(df_pequeno, ...).filter(col("valor") > 100)

# ✅ Mais eficiente: filtra antes, menos dados no shuffle
# df_grande_filtrado = df_grande.filter(col("valor") > 100)
# resultado = df_grande_filtrado.join(df_pequeno, ...)

# COMMAND ----------

# MAGIC %md
# ### Técnica 3: Usar coalesce() em vez de repartition() para reduzir
#
# `repartition(n)` → shuffle completo (wide)
# `coalesce(n)` → move dados localmente quando possível (narrow) — apenas para REDUZIR partições

df_base_grande = spark.range(1_000_000)

# Para reduzir partições antes de salvar:
df_base_grande.coalesce(4).write.format("noop").mode("overwrite").save()  # narrow

# Para aumentar ou redistribuir uniformemente → use repartition (wide, mas necessário)
df_base_grande.repartition(16).write.format("noop").mode("overwrite").save()

# COMMAND ----------

# MAGIC %md
# ## 7. Fluxo Completo — Da Action ao resultado

# COMMAND ----------

# MAGIC %md
# ```
#  Seu código PySpark/SQL
#          │
#          ▼
#  ┌───────────────────┐
#  │   DAG Scheduler   │  ← constrói o grafo de dependências
#  │  Analisa o DAG e  │
#  │  divide em Stages │
#  └────────┬──────────┘
#           │
#     ┌─────┴─────────────────────────┐
#     │                               │
#     ▼                               ▼
#  Stage 1                         Stage 2
#  (narrow ops)                    (pós-shuffle)
#  Tasks: 1 por partição           Tasks: 1 por partição
#     │                               │
#     └─── SHUFFLE ───────────────────┘
#             │ (escrita em disco + rede)
#             ▼
#  ┌───────────────────┐
#  │   Task Scheduler  │  ← envia tasks para os Executors disponíveis
#  └────────┬──────────┘
#           │
#  ┌────────┴─────────────────────┐
#  │         Executors            │
#  │  Task 0 │ Task 1 │ Task 2 …  │  ← paralelo, uma task por slot
#  └──────────────────────────────┘
#           │
#           ▼
#       Resultado (Action: count, show, write...)
# ```

# COMMAND ----------

# MAGIC %md
# ## ⚠️ Resumo de armadilhas e pontos de prova
#
# | Tema | O que saber |
# |---|---|
# | Lazy evaluation | Transformações não executam até uma action ser chamada |
# | 1 action = 1 job | Cada `count()`, `show()`, `write()` dispara um Job separado |
# | Stage boundary | Qualquer wide transformation (groupBy, join, sort) cria nova Stage |
# | Tasks = partições | Número de tasks de uma Stage = partições do DF naquele ponto |
# | `shuffle.partitions` | Default 200 — pode ser excessivo; AQE ajusta automaticamente |
# | `coalesce` vs `repartition` | coalesce para reduzir (narrow); repartition para redistribuir (wide) |
# | Broadcast Join | Evita shuffle em joins com tabela pequena — use `broadcast()` ou ajuste threshold |
# | Filtrar antes do join | Reduz volume do shuffle — sempre que possível, filtre cedo |
# | Stage skipped | Aparece no Spark UI quando o resultado está cacheado — comportamento esperado |
# | orderBy vs sortWithinPartitions | orderBy = shuffle global (wide); sortWithinPartitions = narrow |

# COMMAND ----------
