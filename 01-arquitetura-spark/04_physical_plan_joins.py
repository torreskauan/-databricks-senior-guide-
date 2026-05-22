# Databricks notebook source

# MAGIC %md
# # 04 — Physical Plan: Joins — BHJ, SMJ, SHJ — Custo e Escolha
#
# **Analogia:**
# Imagine que você precisa combinar dois arquivos de fichas de clientes.
#
# **BHJ (Broadcast Hash Join):** Um arquivo é tão pequeno que cabe em uma folha de papel.
# Você faz fotocópias e distribui para cada atendente — cada um consulta na própria cópia
# sem precisar se comunicar com os outros. Zero coordenação, máxima velocidade.
#
# **SMJ (Sort Merge Join):** Ambos os arquivos são enormes. Você os ordena alfabeticamente,
# distribui as letras entre os atendentes (A-G, H-M, N-Z) e cada um junta as fichas do seu
# intervalo em paralelo. Requer organização prévia, mas escala bem.
#
# **SHJ (Shuffle Hash Join):** Um arquivo é médio e o outro é grande. Você embaralha os dois
# pela mesma chave, e cada atendente monta uma tabela hash do arquivo menor para consultar
# rapidamente enquanto percorre o maior. Mais rápido que SMJ quando cabe na memória.
#
# **Conceito técnico:**
# O Spark tem 5 estratégias de join físico. As 3 principais são:
# - **BHJ**: broadcast do lado menor + hash lookup. Sem shuffle. Mais rápido para small-large.
# - **SMJ**: shuffle de ambos os lados + sort + merge linear. Robusto, escalável, padrão para large-large.
# - **SHJ**: shuffle de ambos os lados + hash table do menor. Sem sort, mas requer memória.
#
# O Catalyst escolhe automaticamente baseado em tamanho estimado e configurações.
# Você pode forçar a escolha com **hints** ou **configs**.
#
# **Quando usar este conhecimento:**
# - Ao diagnosticar joins lentos no Spark UI
# - Ao forçar a estratégia certa via hints
# - Para evitar OOM em joins mal dimensionados
# - Entrevistas sênior e prova Databricks Professional

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, rand, expr
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# ## Visão geral: as 5 estratégias de join físico
#
# ```
# ┌──────────────────────────────────────────────────────────────────────────────┐
# │               DECISION TREE — Como o Catalyst escolhe o join                 │
# │                                                                              │
# │  lado menor < autoBroadcastJoinThreshold (10 MB default)?                   │
# │            │                                                                 │
# │    SIM ────┼──→ BROADCAST HASH JOIN (BHJ)  ← sem shuffle, mais rápido       │
# │            │                                                                 │
# │    NÃO ────┼──→ preferSortMergeJoin = false?                                 │
# │            │         │                                                       │
# │            │   SIM ──┼──→ lado menor cabe em memória (hash table)?           │
# │            │         │         │                                             │
# │            │         │   SIM ──┼──→ SHUFFLE HASH JOIN (SHJ)                  │
# │            │         │         │                                             │
# │            │         │   NÃO ──┼──→ SORT MERGE JOIN (SMJ)                    │
# │            │         │                                                       │
# │            │   NÃO ──┼──→ SORT MERGE JOIN (SMJ)  ← padrão large-large       │
# │            │                                                                 │
# │  Casos especiais:                                                            │
# │  · Sem equi-join → BROADCAST NESTED LOOP JOIN (BNLJ)                        │
# │  · Força bruta   → CARTESIAN JOIN (CROSS JOIN)                              │
# └──────────────────────────────────────────────────────────────────────────────┘
# ```

# COMMAND ----------

# MAGIC %md
# ## 1. Broadcast Hash Join (BHJ)

# COMMAND ----------

# MAGIC %md
# ### Como funciona
#
# ```
# DRIVER coleta o lado pequeno
#         │
#         ▼
# BroadcastExchange ─────────────────────────────────┐
#         │                                           │
#         ▼                                           ▼
#   Executor 1               Executor 2          Executor 3
#   [hash table da           [hash table da      [hash table da
#    tabela pequena]          tabela pequena]     tabela pequena]
#         │                       │                   │
#   lê partição 1           lê partição 2       lê partição 3
#   do lado grande          do lado grande      do lado grande
#         │                       │                   │
#   lookup no hash          lookup no hash      lookup no hash
#   (O(1) por linha)        (O(1) por linha)    (O(1) por linha)
#
# ZERO SHUFFLE do lado grande — a rede só move a tabela pequena (1 vez)
# ```
#
# **Custo:** O(N) onde N = tamanho do lado grande
# **Complexidade de rede:** apenas o tamanho da tabela pequena × número de Executors
# **Limitação:** a tabela pequena INTEIRA precisa caber na memória de cada Executor

# COMMAND ----------

# Setup: tabela grande e tabela pequena
df_grande = (
    spark.range(5_000_000)
    .withColumn("cliente_id", (col("id") % 1000).cast("long"))
    .withColumn("valor", (rand() * 1000).cast("double"))
    .withColumnRenamed("id", "transacao_id")
)

df_pequeno = spark.createDataFrame(
    [(i, f"Cliente_{i}", ["SP", "RJ", "MG", "RS"][i % 4]) for i in range(1000)],
    ["cliente_id", "nome", "estado"]
)

print(f"Tabela grande: ~{df_grande.rdd.getNumPartitions()} partições")
print(f"Tabela pequena: {df_pequeno.count()} linhas")

# COMMAND ----------

# BHJ automático: Spark decide sozinho quando o lado menor < threshold
print("Broadcast threshold atual:", spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
# Default: 10MB (10485760 bytes)
# Databricks costuma usar valores maiores em alguns runtimes

# Join sem hint — se df_pequeno for < 10MB, Spark fará BHJ automaticamente
join_auto = df_grande.join(df_pequeno, on="cliente_id", how="inner")
print("\n=== Physical Plan — BHJ automático ===")
join_auto.explain(mode="formatted")
# Procure por: BroadcastHashJoin e BroadcastExchange

# COMMAND ----------

# BHJ forçado com hint — útil quando o threshold não cobre o tamanho real
# mas você sabe que a tabela cabe na memória dos Executors

join_bhj_forcado = df_grande.join(broadcast(df_pequeno), on="cliente_id", how="inner")

# Alternativa via hint SQL:
# SELECT /*+ BROADCAST(clientes) */ * FROM transacoes JOIN clientes USING (cliente_id)

print("\n=== Physical Plan — BHJ forçado com broadcast() ===")
join_bhj_forcado.explain(mode="formatted")
# Você SEMPRE verá BroadcastHashJoin e BroadcastExchange aqui

# COMMAND ----------

# MAGIC %md
# ### Configurações importantes do BHJ
#
# | Config | Default | Descrição |
# |---|---|---|
# | `spark.sql.autoBroadcastJoinThreshold` | `10485760` (10MB) | Tamanho máximo para broadcast automático. `-1` desativa. |
# | `spark.sql.broadcastTimeout` | `300` (5 min) | Timeout para o broadcast chegar nos Executors |
# | `spark.broadcast.blockSize` | `4096` KB | Tamanho dos blocos de broadcast |
#
# **Quando aumentar o threshold:**
# Se você tem tabelas de dimensão de 50-200MB que são sempre joinadas e cabem confortavelmente
# na memória dos seus Executors, aumentar o threshold pode eliminar shuffles desnecessários.

# COMMAND ----------

# Aumentar o threshold (cuidado: afeta todos os joins da sessão)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))  # 50MB
print("Novo threshold:", spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

# Restaurar
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))

# COMMAND ----------

# MAGIC %md
# ### ⚠️ Armadilhas do BHJ
#
# 1. **OOM no Executor**: a hash table da tabela broadcast precisa caber na memória de CADA Executor.
#    Se você broadcastar uma tabela de 2GB e tiver 4 Executors → cada um precisa de 2GB livres.
#
# 2. **OOM no Driver**: o Driver coleta a tabela pequena antes de broadcastar.
#    Se a tabela for maior que `spark.driver.maxResultSize` → falha.
#
# 3. **Broadcast de tabela errada**: nunca force broadcast no lado GRANDE.
#    `broadcast(df_grande).join(df_pequeno)` → OOM garantido.
#
# 4. **Timeout**: em clusters lentos ou tabelas grandes, o broadcast pode expirar.
#    Aumente `spark.sql.broadcastTimeout` se necessário.

# COMMAND ----------

# MAGIC %md
# ## 2. Sort Merge Join (SMJ)

# COMMAND ----------

# MAGIC %md
# ### Como funciona
#
# ```
# FASE 1 — Shuffle (2 Exchanges):
#   Lado A: cada linha vai para a partição hash(join_key) % N
#   Lado B: cada linha vai para a partição hash(join_key) % N
#   → Mesma chave SEMPRE vai para o mesmo Executor (garantia do shuffle)
#
# FASE 2 — Sort:
#   Cada Executor ordena suas partições de A e B pela join_key
#
# FASE 3 — Merge:
#   Dois ponteiros percorrem A_sorted e B_sorted simultaneamente
#   → Scan linear O(N+M) — muito eficiente em CPU
#   → Funciona porque ambos estão ordenados pela mesma chave
#
# ┌──────────────┐      ┌──────────────┐
# │  Partição A  │      │  Partição B  │
# │  (ordenada)  │      │  (ordenada)  │
# │  key=1, ...  │──┐   │  key=1, ...  │
# │  key=2, ...  │  ├──→│  key=2, ...  │  ← merge ponteiro a ponteiro
# │  key=3, ...  │──┘   │  key=3, ...  │
# └──────────────┘      └──────────────┘
# ```
#
# **Custo:** O(N log N + M log M) para o sort + O(N+M) para o merge
# **Vantagem:** escalável para qualquer tamanho, robusto com spill para disco
# **Desvantagem:** dois shuffles + dois sorts = overhead elevado

# COMMAND ----------

# Forçar SMJ (desativar broadcast para ver o comportamento)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # desativa broadcast automático

df_medio_a = (
    spark.range(500_000)
    .withColumn("chave", (col("id") % 10_000).cast("long"))
    .withColumn("valor_a", (rand() * 500).cast("double"))
)

df_medio_b = (
    spark.range(300_000)
    .withColumn("chave", (col("id") % 10_000).cast("long"))
    .withColumn("valor_b", (rand() * 300).cast("double"))
)

join_smj = df_medio_a.join(df_medio_b, on="chave", how="inner")

print("=== Physical Plan — Sort Merge Join ===")
join_smj.explain(mode="formatted")
# Procure por: SortMergeJoin, Sort, Exchange (2 deles — um para cada lado)

# COMMAND ----------

# Restaurar threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))

# COMMAND ----------

# MAGIC %md
# ### Quando o SMJ é a melhor escolha
#
# - Ambos os lados são grandes (> threshold de broadcast)
# - Os dados já estão pré-particionados pela join key (elimina um shuffle!)
# - Joins com chaves complexas ou múltiplas chaves
# - Quando você precisa de garantia de escalabilidade (SMJ sempre funciona, mesmo com OOM → spill)
#
# ### Eliminando o shuffle do SMJ com bucketing
#
# Se ambas as tabelas são escritas com `bucketBy` na mesma chave e mesmo número de buckets,
# o Spark **elimina o Exchange** do SMJ — só faz o sort e merge.

# COMMAND ----------

# Exemplo: salvar tabelas com bucketing para eliminar shuffle no join futuro
# (requer tabelas gerenciadas no Hive/Unity Catalog)

# df_medio_a.write \
#     .bucketBy(16, "chave") \
#     .sortBy("chave") \
#     .saveAsTable("catalog.schema.tabela_a_bucketed")
#
# df_medio_b.write \
#     .bucketBy(16, "chave") \
#     .sortBy("chave") \
#     .saveAsTable("catalog.schema.tabela_b_bucketed")
#
# Join sem shuffle:
# spark.table("tabela_a_bucketed").join(spark.table("tabela_b_bucketed"), on="chave")
# Physical Plan: SortMergeJoin sem Exchange → enorme ganho de performance

print("Bucketing: salva o custo do shuffle em joins recorrentes entre as mesmas tabelas")

# COMMAND ----------

# MAGIC %md
# ## 3. Shuffle Hash Join (SHJ)

# COMMAND ----------

# MAGIC %md
# ### Como funciona
#
# ```
# FASE 1 — Shuffle (2 Exchanges):
#   Igual ao SMJ: ambos os lados são redistribuídos pela join_key
#
# FASE 2 — Build hash table do lado menor:
#   Para cada partição, o Executor constrói uma hash table do lado menor (build side)
#   Não precisa de sort — O(1) por lookup
#
# FASE 3 — Probe com o lado maior:
#   O Executor percorre o lado maior (probe side) linha por linha
#   e consulta a hash table para encontrar matches
#
# ┌──────────────────┐
# │  Hash Table      │  ← construída do lado menor (cabe na memória do Executor)
# │  key=1 → rows    │
# │  key=2 → rows    │
# └──────────────────┘
#         ↑ probe
# lado maior percorrido linha por linha
# ```
#
# **Custo:** O(N) para build + O(M) para probe — sem sort
# **Vantagem:** mais rápido que SMJ quando a hash table cabe na memória
# **Desvantagem:** se a hash table não couber → OOM (sem spill para disco no SHJ)

# COMMAND ----------

# Forçar SHJ via hint:
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # desativa broadcast

df_build = (
    spark.range(50_000)
    .withColumn("chave", (col("id") % 5_000).cast("long"))
    .withColumn("info", expr("concat('dado_', id)"))
)

df_probe = (
    spark.range(2_000_000)
    .withColumn("chave", (col("id") % 5_000).cast("long"))
    .withColumn("valor", (rand() * 1000).cast("double"))
)

# Hint SHUFFLE_HASH força SHJ
join_shj = df_probe.hint("SHUFFLE_HASH").join(df_build, on="chave", how="inner")

print("=== Physical Plan — Shuffle Hash Join ===")
join_shj.explain(mode="formatted")
# Procure por: ShuffledHashJoin, Exchange (2 deles, sem Sort)

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))

# COMMAND ----------

# MAGIC %md
# ## 4. Comparativo completo BHJ × SMJ × SHJ

# COMMAND ----------

# MAGIC %md
# ```
# ┌─────────────────────┬──────────────────┬──────────────────┬──────────────────┐
# │ Critério            │ BHJ              │ SMJ              │ SHJ              │
# ├─────────────────────┼──────────────────┼──────────────────┼──────────────────┤
# │ Shuffle             │ Nenhum           │ 2 (ambos lados)  │ 2 (ambos lados)  │
# │ Sort                │ Nenhum           │ Sim (ambos)      │ Não              │
# │ Hash table          │ Sim (broadcast)  │ Não              │ Sim (1 lado)     │
# │ Complexidade        │ O(N)             │ O(NlogN+MlogM)   │ O(N+M)           │
# │ Escalabilidade      │ Limitada (memória│ Excelente        │ Boa              │
# │                     │ dos Executors)   │ (spill p/ disco) │ (sem spill)      │
# │ Risco OOM           │ Driver + Executor│ Baixo (spill)    │ Executor (build) │
# │ Equi-join obrigatório│ Sim             │ Sim              │ Sim              │
# │ Quando usar         │ Small-Large      │ Large-Large      │ Medium-Large     │
# │                     │ (dim × fato)     │ (padrão seguro)  │ (sem sort)       │
# │ Hint PySpark        │ broadcast(df)    │ MERGE            │ SHUFFLE_HASH     │
# │ Hint SQL            │ BROADCAST(alias) │ MERGE(alias)     │ SHUFFLE_HASH     │
# └─────────────────────┴──────────────────┴──────────────────┴──────────────────┘
# ```

# COMMAND ----------

# MAGIC %md
# ## 5. Hints — Forçando a estratégia de join

# COMMAND ----------

# MAGIC %md
# ### PySpark Hints

# COMMAND ----------

df_a = spark.range(1_000_000).withColumn("chave", (col("id") % 10_000).cast("long"))
df_b = spark.range(10_000).withColumn("chave", col("id").cast("long"))

# BHJ — broadcast do lado menor
hint_bhj = df_a.join(broadcast(df_b), on="chave")
# OU: df_a.join(df_b.hint("BROADCAST"), on="chave")

# SMJ — forçar sort merge mesmo que broadcast fosse possível
hint_smj = df_a.join(df_b.hint("MERGE"), on="chave")

# SHJ — shuffle hash join
hint_shj = df_a.join(df_b.hint("SHUFFLE_HASH"), on="chave")

# BNLJ — broadcast nested loop (joins sem equi-join, ex: range join)
# df_a.join(df_b.hint("BROADCAST"), on=condição_complexa_sem_igualdade)

for nome, df_join in [("BHJ", hint_bhj), ("SMJ", hint_smj), ("SHJ", hint_shj)]:
    print(f"\n{'='*10} {nome} {'='*10}")
    df_join.explain(mode="simple")

# COMMAND ----------

# MAGIC %md
# ### Hints em Spark SQL

# COMMAND ----------

spark.sql("""
    SELECT /*+ BROADCAST(b) */
        a.id,
        b.chave
    FROM range(1000000) a
    JOIN range(10000) b ON a.id % 10000 = b.id
""").explain(mode="simple")

# Outros hints SQL:
# /*+ MERGE(alias) */         → Sort Merge Join
# /*+ SHUFFLE_HASH(alias) */  → Shuffle Hash Join
# /*+ SHUFFLE_REPLICATE_NL(alias) */ → força Nested Loop com shuffle

# COMMAND ----------

# MAGIC %md
# ## 6. Como o AQE muda a estratégia em runtime

# COMMAND ----------

# MAGIC %md
# O AQE pode **converter SMJ → BHJ em runtime** se, após o shuffle de um dos lados,
# perceber que ele é menor do que estimado — sem você precisar fazer nada.
#
# ```
# Plano inicial (estimativa pré-execução):
#   SMJ — Spark estima ambos os lados como grandes
#
# Runtime (após shuffle do lado esquerdo):
#   AQE mede: "oh, o lado esquerdo tem apenas 3MB após o filtro"
#   → Converte automaticamente para BHJ do lado esquerdo
#
# Resultado no Spark UI:
#   Você verá "AQE" e o plano final diferente do plano inicial
# ```

# COMMAND ----------

# Verificar se AQE está convertendo joins:
print("AQE ativo:", spark.conf.get("spark.sql.adaptive.enabled"))

# Threshold para conversão SMJ → BHJ em runtime:
# spark.sql.adaptive.autoBroadcastJoinThreshold
# Default: herda spark.sql.autoBroadcastJoinThreshold
try:
    val = spark.conf.get("spark.sql.adaptive.autoBroadcastJoinThreshold")
except Exception:
    val = "(herda autoBroadcastJoinThreshold)"
print("AQE broadcast threshold:", val)

# COMMAND ----------

# MAGIC %md
# ## 7. Diagnosticando joins lentos no Spark UI

# COMMAND ----------

# MAGIC %md
# ### Checklist no Spark UI para joins
#
# **Aba Jobs:**
# - Verifique se o job tem mais stages do que esperado → shuffles desnecessários
#
# **Aba Stages:**
# - Procure stages com alto `Shuffle Read/Write` → indicativo do custo do shuffle
# - Procure `Spill` → join usando mais memória do que disponível → risco de lentidão
# - Procure tasks com durações muito desiguais → skew → alguma chave de join concentrada
#
# **Aba SQL → Query Details:**
# - Clique na query para ver o plano com métricas reais
# - BroadcastExchange: verifica o tamanho real que foi broadcastado
# - SortMergeJoin: verifica quantas linhas entraram e saíram
# - Exchange: verifica bytes escritos e lidos no shuffle
#
# ### Sinais de que você escolheu a estratégia errada
#
# | Sintoma | Causa provável | Solução |
# |---|---|---|
# | BroadcastTimeout | Tabela broadcast maior do que cabe na rede/tempo | Reduza threshold ou use SMJ |
# | OOM no Executor durante build | SHJ com hash table muito grande | Use SMJ (tem spill) |
# | OOM no Driver | Broadcast forçado de tabela grande | Não force broadcast no lado grande |
# | Skew extremo em SortMergeJoin | Chave com muitos valores iguais | Ative AQE skew join ou use salting |
# | SMJ lento mas tabela pequena | Threshold baixo → não fez BHJ | Aumente threshold ou use hint |

# COMMAND ----------

# MAGIC %md
# ## 8. Guia de decisão prático

# COMMAND ----------

# MAGIC %md
# ```
# Qual join strategy usar?
#
# ┌─ Lado menor < 10MB (ou ajuste do threshold)?
# │    └─ SIM → BHJ automático — não faça nada
# │
# ├─ Lado menor entre 10MB e ~500MB e cabe na memória do Executor?
# │    └─ SIM → broadcast(df_menor) ou aumente o threshold
# │               Valide: executor.memory - overhead - cache > tamanho da tabela × 3
# │
# ├─ Ambos os lados grandes (GBs)?
# │    ├─ Join recorrente na mesma chave → bucketing (elimina shuffle do SMJ)
# │    ├─ Skew na chave → AQE skew join ativo + verifique se precisa de salting
# │    └─ Caso geral → SMJ (padrão, robusto, com spill)
# │
# ├─ Um lado médio (100MB-2GB) sem sort necessário?
# │    └─ SHJ com hint → sem sort, mais rápido que SMJ se cabe na memória
# │
# └─ Join por range ou condição complexa (não equi-join)?
#      └─ BNLJ (broadcast nested loop) — lento, use com cautela e sempre com broadcast
# ```

# COMMAND ----------

# MAGIC %md
# ## ⚠️ Resumo de armadilhas e pontos de prova
#
# | Tema | O que saber |
# |---|---|
# | BHJ sem shuffle | É o único join sem shuffle — sempre preferível para small-large |
# | Broadcast vai para o Driver primeiro | Driver coleta o lado pequeno → risco de Driver OOM |
# | SMJ tem spill | Único join que derrama para disco sem falhar — mais seguro para dados grandes |
# | SHJ sem sort, sem spill | Mais rápido que SMJ, mas OOM se hash table não couber |
# | AQE converte SMJ → BHJ | Em runtime, após shuffle, se perceber que um lado é pequeno |
# | Bucketing elimina Exchange | Tabelas com mesmo `bucketBy(N, key)` → SMJ sem shuffle |
# | Hint vs Config | Hint afeta apenas aquele join. Config afeta toda a sessão |
# | `broadcast()` no lado errado | `broadcast(df_grande)` → OOM garantido no Driver e Executors |
# | Skew em join | Chave com muitos nulls ou valor dominante → 1 task com 90% dos dados |
# | BNLJ é O(N×M) | Evite não-equi-joins em tabelas grandes — quadrático |

# COMMAND ----------
