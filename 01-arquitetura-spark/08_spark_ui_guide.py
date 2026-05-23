# Databricks notebook source

# MAGIC %md
# # 08 — Spark UI: Leitura de Todas as Abas
#
# **Analogia:**
# O Spark UI é o painel de instrumentos de um avião: durante o voo você não adivinha se
# está tudo bem — você olha para os instrumentos. Altitude (memória usada), velocidade
# (throughput de tasks), temperatura dos motores (GC time), consumo de combustível
# (shuffle bytes) — cada medidor conta uma história. Um piloto experiente sabe para qual
# instrumento olhar quando algo parece errado, e sabe o que um valor fora do normal significa.
#
# **Conceito técnico:**
# O Spark UI é uma interface web que expõe métricas de execução em tempo real e histórico.
# No Databricks, é acessado via "Spark UI" no cluster ou na task de um job.
# Cada aba cobre uma dimensão diferente da execução: Jobs, Stages, Tasks, Storage,
# Executors, SQL, Streaming e Environment.
#
# **Quando usar este conhecimento:**
# - Diagnóstico de jobs lentos: onde está o gargalo?
# - Identificar skew, spill, OOM, shuffle excessivo
# - Validar que otimizações (broadcast, cache, AQE) funcionaram
# - Ler planos de execução com métricas reais
# - Entrevistas sênior: "como você diagnosticaria esse job lento?"

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, spark_sum, count, broadcast
from pyspark.sql.types import LongType, DoubleType, StringType

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# ## Como acessar o Spark UI no Databricks
#
# ```
# 1. Em um cluster ativo:
#    Compute → [nome do cluster] → aba "Spark UI"
#    → Abre a UI do driver desse cluster
#
# 2. Durante execução de um Job:
#    Jobs → [nome do job] → [run específico] → Task → "Spark UI"
#    → Abre a UI do contexto daquele run
#
# 3. Via código (retorna a URL do Spark UI):
# ```

print("Spark UI URL:", spark.sparkContext.uiWebUrl)

# COMMAND ----------

# MAGIC %md
# ## Geração de workload para análise na UI
#
# Rode as células abaixo e use o Spark UI em paralelo para observar cada aba.

# COMMAND ----------

# Dataset base para todas as demonstrações
df_transacoes = (
    spark.range(5_000_000)
    .withColumn("cliente_id", (col("id") % 1000).cast(LongType()))
    .withColumn("produto_id", (col("id") % 200).cast(LongType()))
    .withColumn("valor", (rand() * 1000).cast(DoubleType()))
    .withColumn("regiao", (col("id") % 5).cast(LongType()))
)

df_clientes = spark.createDataFrame(
    [(i, f"Cliente_{i}", ["SP","RJ","MG","RS","BA"][i % 5]) for i in range(1000)],
    ["cliente_id", "nome", "estado"]
)

df_produtos = spark.createDataFrame(
    [(i, f"Produto_{i}", float(i * 10)) for i in range(200)],
    ["produto_id", "descricao", "preco_base"]
)

# COMMAND ----------

# MAGIC %md
# ## Aba 1 — JOBS

# COMMAND ----------

# MAGIC %md
# ### O que você vê na aba Jobs
#
# ```
# ┌─────────────────────────────────────────────────────────────────────────────┐
# │  Jobs                                                                       │
# │                                                                             │
# │  Job Id │ Description          │ Submitted │ Duration │ Stages  │ Tasks    │
# │  ────────┼──────────────────────┼───────────┼──────────┼─────────┼──────── │
# │  0      │ count at <...>:1     │ 10:23:01  │ 2s       │ 2/2     │ 12/12   │
# │  1      │ show at <...>:1      │ 10:23:03  │ 0.3s     │ 1/1     │ 4/4     │
# │  2      │ collect at <...>:1   │ 10:23:05  │ 5s       │ 3/3     │ 48/48   │
# └─────────────────────────────────────────────────────────────────────────────┘
# ```
#
# **Colunas:**
# - **Description:** qual action disparou o job (count, show, collect, save...)
#   No Databricks, mostra também o nome do notebook e a linha
# - **Duration:** tempo total do job (inclui tempo de scheduling)
# - **Stages:** `completas/total`. Ex: `2/2` = 2 stages, todas OK. `1/3` = 1 de 3 completas
# - **Tasks:** total de tasks executadas com sucesso
#
# **DAG Visualization:** clique em qualquer job para ver o grafo de stages com as
# dependências visuais. Cor verde = completo, azul = rodando, vermelho = falhou.

# COMMAND ----------

# Dispara 3 jobs distintos — observe na aba Jobs
count_result = df_transacoes.count()                    # Job 1
print(f"Total de transações: {count_result:,}")

df_transacoes.select("regiao", "valor").show(5)         # Job 2

soma_por_regiao = (
    df_transacoes
    .groupBy("regiao")
    .agg(spark_sum("valor").alias("total"))
    .collect()
)
print("Soma por região:", soma_por_regiao)               # Job 3

# COMMAND ----------

# MAGIC %md
# ### O que procurar na aba Jobs
#
# | Sinal | Diagnóstico |
# |---|---|
# | Job com duração muito maior que os outros | Stage com skew ou spill dentro dele |
# | Muitos jobs pequenos e rápidos | Pipeline bem fragmentado — normal |
# | Job "rodando" há muito tempo | Alguma task travada (stragglers) |
# | Job com `Stages: 1/3` por longo tempo | Stage intermediária bloqueada — verifique Stages |
# | Stages `Skipped` | Cache ativo — Spark pulou recomputação — comportamento esperado |

# COMMAND ----------

# MAGIC %md
# ## Aba 2 — STAGES

# COMMAND ----------

# MAGIC %md
# ### O que você vê na aba Stages
#
# ```
# ┌──────────────────────────────────────────────────────────────────────────────┐
# │  Stages                                                                      │
# │                                                                              │
# │  Stage │ Description     │ Tasks │ Input    │ Output  │ Shuffle R │ Shuffle W│
# │  ──────┼─────────────────┼───────┼──────────┼─────────┼───────────┼──────── │
# │  0     │ count (scan)    │ 16    │ 512 MB   │ -       │ -         │ 128 MB  │
# │  1     │ count (agg)     │ 200   │ -        │ -       │ 128 MB    │ -       │
# │  2     │ show (scan)     │ 4     │ SKIPPED  │ -       │ -         │ -       │
# └──────────────────────────────────────────────────────────────────────────────┘
# ```
#
# **Colunas chave:**
# - **Input:** bytes lidos da fonte (disco/S3/ADLS). Alto input com pouco output = boa filtragem
# - **Output:** bytes escritos como resultado final da stage
# - **Shuffle Read:** bytes recebidos de outras partições via shuffle
# - **Shuffle Write:** bytes enviados para as próximas partições via shuffle
# - **Spill (Memory):** dados que não couberam na memória (em memória, antes de serializar)
# - **Spill (Disk):** tamanho dos dados spilled no disco (após compressão)

# COMMAND ----------

# Dispara um job com múltiplas stages — observe cada stage na UI
resultado_stages = (
    df_transacoes                              # Stage 0: scan + filter
    .filter(col("valor") > 100)
    .join(broadcast(df_clientes), "cliente_id")  # broadcast join — não cria stage extra
    .groupBy("estado")                         # Stage 1: shuffle por estado
    .agg(
        spark_sum("valor").alias("total_valor"),
        count("*").alias("num_transacoes")
    )
    .orderBy("total_valor", ascending=False)   # Stage 2: sort global (outro shuffle)
)

resultado_stages.show()

# COMMAND ----------

# MAGIC %md
# ### Detalhamento de uma Stage — clique em qualquer Stage
#
# ```
# Summary Metrics for N Tasks:
# ┌──────────────┬────────┬────────┬────────┬────────┬────────┐
# │ Metric       │ Min    │ 25th % │ Median │ 75th % │ Max    │
# ├──────────────┼────────┼────────┼────────┼────────┼────────┤
# │ Duration     │ 1.2s   │ 1.5s   │ 1.6s   │ 1.8s   │ 12.3s  │← outlier = skew
# │ GC Time      │ 0ms    │ 50ms   │ 80ms   │ 120ms  │ 4.5s   │← 4.5s = GC problem
# │ Input Size   │ 32 MB  │ 33 MB  │ 33 MB  │ 34 MB  │ 35 MB  │← uniforme = sem skew de leitura
# │ Shuffle Read │ 10 MB  │ 11 MB  │ 11 MB  │ 12 MB  │ 89 MB  │← 89 MB = skew na chave
# │ Spill(Mem)   │ 0 B    │ 0 B    │ 0 B    │ 0 B    │ 512 MB │← 1 task espillou muito
# └──────────────┴────────┴────────┴────────┴────────┴────────┘
#
# Interpretação:
# · Duration Max >> Median → 1 task straggler → provavelmente a de Shuffle Read alto
# · GC Time 4.5s em uma task de 12s = 37% do tempo em GC → problema de memória
# · Shuffle Read desuniforme (89 MB vs 11 MB mediano) → DATA SKEW na chave de join/groupBy
# · Spill em 1 task → a task com skew não tem memória suficiente para seus dados
# ```

# COMMAND ----------

# MAGIC %md
# ### DAG Visualization dentro da Stage
#
# Cada stage tem um mini-DAG mostrando os operadores físicos com métricas reais acumuladas.
# Você verá setas com "número de linhas" e "bytes" entre os nós — valores REAIS de execução,
# não estimativas do plano.
#
# Compare estes valores reais com o que o `EXPLAIN` estimou:
# se as estimativas forem muito diferentes dos valores reais → estatísticas desatualizadas
# → execute `ANALYZE TABLE ... COMPUTE STATISTICS` para atualizar.

# COMMAND ----------

# MAGIC %md
# ## Aba 3 — TASKS (dentro de uma Stage)

# COMMAND ----------

# MAGIC %md
# ### O que você vê na tabela de Tasks
#
# ```
# Tasks for Stage N:
# ┌──────┬────────┬──────────┬──────────┬──────────┬──────────┬───────────────┐
# │ Index│Status  │ Duration │ GC Time  │ Input    │ Shfl Read│ Shfl Write    │
# ├──────┼────────┼──────────┼──────────┼──────────┼──────────┼───────────────┤
# │ 0    │SUCCESS │ 1.2s     │ 45ms     │ 128 MB   │ -        │ 32 MB         │
# │ 1    │SUCCESS │ 1.3s     │ 52ms     │ 130 MB   │ -        │ 31 MB         │
# │ 2    │SUCCESS │ 12.1s    │ 4.2s     │ 128 MB   │ -        │ 120 MB        │← SKEW
# │ 3    │SUCCESS │ 1.1s     │ 40ms     │ 127 MB   │ -        │ 30 MB         │
# └──────┴────────┴──────────┴──────────┴──────────┴──────────┴───────────────┘
#
# Task 2 é um STRAGGLER:
# · Duração 10× maior que as outras
# · GC Time alto (4.2s = 35% da duração) → memória pressionada
# · Shuffle Write muito maior (120 MB vs ~31 MB) → dados concentrados nesta partição
# → Diagnóstico: DATA SKEW na chave de shuffle
# ```

# COMMAND ----------

# MAGIC %md
# ### Status de tasks e o que significam
#
# | Status | Significado |
# |---|---|
# | `SUCCESS` | Task concluída com sucesso |
# | `FAILED` | Task falhou — será re-tentada (até spark.task.maxFailures vezes) |
# | `RUNNING` | Em execução no momento |
# | `KILLED` | Cancelada (ex: especulative execution matou uma cópia lenta) |
# | `SKIPPED` | Stage foi pulada (resultado em cache) |
# | `PENDING` | Aguardando slot disponível no Executor |
#
# **Speculative Execution:** se uma task está demorando muito mais que a mediana,
# o Spark pode lançar uma cópia especulativa em outro Executor. Quem terminar primeiro
# "ganha" e a outra é `KILLED`. Ativado via `spark.speculation = true` (off por default).

# COMMAND ----------

# MAGIC %md
# ## Aba 4 — STORAGE

# COMMAND ----------

# MAGIC %md
# ### O que você vê na aba Storage
#
# ```
# RDDs / DataFrames em cache:
# ┌──────────┬───────────────────┬───────────┬─────────┬──────────┬──────────┐
# │ RDD ID   │ Name              │ Storage   │ Cached  │ Size in  │ Size on  │
# │          │                   │ Level     │ Partns  │ Memory   │ Disk     │
# ├──────────┼───────────────────┼───────────┼─────────┼──────────┼──────────┤
# │ 12       │ df_transacoes     │ Disk Mem  │ 16/16   │ 1.2 GB   │ 0 B      │
# │          │ Deserialized 1x   │ Deser     │         │          │          │
# │ 15       │ df_clientes       │ Disk Mem  │ 1/1     │ 45 KB    │ 0 B      │
# └──────────┴───────────────────┴───────────┴─────────┴──────────┴──────────┘
# ```
#
# **Colunas:**
# - **Storage Level:** onde está armazenado (MEMORY_ONLY, MEMORY_AND_DISK, DISK_ONLY, OFF_HEAP)
# - **Cached Partitions:** quantas partições do total estão em cache (ex: 16/16 = completo)
# - **Size in Memory:** bytes usados no heap (se parcialmente no disco, mostra só a parte em memória)
# - **Size on Disk:** bytes no disco local (partições que não couberam na memória)
#
# Se "Cached Partitions" < total → o cache está **parcial** (algumas partições foram evicted
# por pressão de memória — o Spark vai recomputar essas partições quando necessário)

# COMMAND ----------

# Demonstração: cache e visualização na aba Storage
df_transacoes.cache()
df_transacoes.count()  # materializa

df_clientes_cache = df_clientes.cache()
df_clientes_cache.count()

# Acesse a aba Storage no Spark UI agora — você verá os 2 DataFrames listados
print("Acesse: Spark UI → Storage")
print("Você verá df_transacoes e df_clientes com seus tamanhos em memória")
print()
print("Storage em cache:")
for info in spark.sparkContext._jsc.sc().getRDDStorageInfo():
    print(f"  ID:{info.id()} | "
          f"Memória: {info.memSize()/1024/1024:.1f} MB | "
          f"Disco: {info.diskSize()/1024/1024:.1f} MB | "
          f"Partições em cache: {info.numCachedPartitions()}/{info.numPartitions()}")

# COMMAND ----------

df_transacoes.unpersist()
df_clientes_cache.unpersist()

# COMMAND ----------

# MAGIC %md
# ## Aba 5 — EXECUTORS

# COMMAND ----------

# MAGIC %md
# ### O que você vê na aba Executors
#
# ```
# Summary:
# Active Executors: 4  │  Dead Executors: 0  │  Total Tasks: 248  │  Active Tasks: 12
#
# ┌──────────┬────────┬─────────┬──────────┬──────────┬──────────┬──────────┬──────────┐
# │ Exec ID  │ Addr   │ Status  │ RDD Blk  │ Mem Used │ Disk Used│ Cores    │ Tasks    │
# ├──────────┼────────┼─────────┼──────────┼──────────┼──────────┼──────────┼──────────┤
# │ driver   │ ...    │ Active  │ 0        │ 0.0 B    │ 0.0 B    │ 0        │ 0/0/0/0  │
# │ 1        │ ...    │ Active  │ 4        │ 1.2 GB   │ 0 B      │ 4        │ 0/0/62/0 │
# │ 2        │ ...    │ Active  │ 4        │ 1.1 GB   │ 0 B      │ 4        │ 0/0/62/0 │
# │ 3        │ ...    │ Active  │ 4        │ 1.3 GB   │ 50 MB    │ 4        │ 0/0/60/0 │← spill
# │ 4        │ ...    │ Active  │ 4        │ 1.2 GB   │ 0 B      │ 4        │ 0/0/64/0 │
# └──────────┴────────┴─────────┴──────────┴──────────┴──────────┴──────────┴──────────┘
#
# Coluna Tasks: formato Active/Failed/Complete/Killed
# Exec 3 com "Disk Used: 50 MB" → spill acontecendo naquele Executor
# ```
#
# **O que analisar:**
# - **Mem Used desbalanceado:** algum Executor com memória muito maior → pode ter dados skewed
# - **Disk Used > 0:** Executor com spill
# - **Failed tasks:** clique no número para ver qual task falhou e por quê
# - **Dead Executors:** Executor morreu (OOM, preemptação spot instance, falha de rede)
# - **Logs:** botão "Logs" em cada Executor → acesso ao stdout/stderr do processo JVM

# COMMAND ----------

# Ver Executors ativos programaticamente
sc = spark.sparkContext
executors = sc._jsc.sc().statusTracker().getExecutorInfos()
print(f"Executors ativos (incluindo driver): {len(executors)}")
for exec_info in executors:
    print(f"  Host: {exec_info.host()}")

# COMMAND ----------

# MAGIC %md
# ### Métricas agregadas por Executor
#
# Role a tela para baixo na aba Executors para ver métricas acumuladas:
# - **Task Time (GC Time):** quanto tempo total foi gasto em GC por Executor
# - **Input:** total de dados lidos da fonte
# - **Shuffle Read / Write:** totais de shuffle por Executor
# - **Logs:** link para stdout e stderr do processo JVM daquele Executor
#   → quando uma task falha com erro estranho, os logs do Executor têm o stack trace completo

# COMMAND ----------

# MAGIC %md
# ## Aba 6 — SQL / DATAFRAME

# COMMAND ----------

# MAGIC %md
# ### O que você vê na aba SQL
#
# ```
# Completed Queries:
# ┌──────┬──────────────────────────────────────────┬──────────┬──────────┐
# │ ID   │ Description                              │ Duration │ Jobs     │
# ├──────┼──────────────────────────────────────────┼──────────┼──────────┤
# │ 0    │ count at NoteBook:42                     │ 2.1s     │ 0        │
# │ 1    │ collect at NoteBook:55                   │ 8.3s     │ 1, 2     │
# │ 2    │ == Physical Plan == ...                  │ 0.5s     │ 3        │
# └──────┴──────────────────────────────────────────┴──────────┴──────────┘
# ```
#
# **Esta é a aba mais poderosa para diagnóstico de SQL/DataFrame.**
# Clique em qualquer query para ver o **Physical Plan com métricas reais** por operador.

# COMMAND ----------

# Executar uma query complexa para analisar na aba SQL
query_complexa = (
    df_transacoes
    .join(broadcast(df_clientes), "cliente_id")
    .join(broadcast(df_produtos), "produto_id")
    .filter(col("valor") > 50)
    .groupBy("estado", "descricao")
    .agg(
        spark_sum("valor").alias("total_vendas"),
        count("*").alias("num_pedidos")
    )
    .orderBy("total_vendas", ascending=False)
)

query_complexa.show(10)

# COMMAND ----------

# MAGIC %md
# ### Lendo o Physical Plan com métricas na aba SQL
#
# ```
# Clique na query → você verá um grafo visual do plano:
#
# ┌─────────────────────────────────────────────────────────────────────┐
# │  Sort                                                               │
# │  number of output rows: 25                                          │
# │  sort time total: 45ms                                              │
# └──────────────────────────┬──────────────────────────────────────────┘
#                            │
# ┌──────────────────────────▼──────────────────────────────────────────┐
# │  HashAggregate (final)                                              │
# │  number of output rows: 25                                          │
# │  avg hash map probe: 1.2                                            │
# └──────────────────────────┬──────────────────────────────────────────┘
#                            │
# ┌──────────────────────────▼──────────────────────────────────────────┐
# │  Exchange (shuffle by estado, descricao)                            │
# │  shuffle bytes written: 1.2 MB                 ← volume real       │
# │  shuffle records written: 5,000,000            ← linhas reais      │
# └──────────────────────────┬──────────────────────────────────────────┘
#                            │
# ┌──────────────────────────▼──────────────────────────────────────────┐
# │  HashAggregate (partial)                                            │
# │  number of output rows: 5,000,000 → 25,000     ← agregação parcial │
# └──────────────────────────┬──────────────────────────────────────────┘
#                            │
# ┌──────────────────────────▼──────────────────────────────────────────┐
# │  Filter (valor > 50)                                                │
# │  number of output rows: 4,750,000  ← 95% passou no filtro          │
# └──────────────────────────┬──────────────────────────────────────────┘
#                            │
# ┌──────────────────────────▼──────────────────────────────────────────┐
# │  BroadcastHashJoin (cliente_id)                                     │
# │  number of output rows: 5,000,000                                   │
# │  avg hash table probe: 1.0  ← eficiente, sem colisão               │
# └──────────────────────────┬──────────────────────────────────────────┘
#                          ┌─┘──────────────┐
# ┌─────────────────────────▼─┐  ┌──────────▼──────────────────────────┐
# │  Scan (transacoes)        │  │  BroadcastExchange (clientes)       │
# │  files read: 16           │  │  data size: 45 KB ← confirmação     │
# │  rows output: 5,000,000   │  │  do tamanho real do broadcast       │
# └───────────────────────────┘  └─────────────────────────────────────┘
# ```

# COMMAND ----------

# MAGIC %md
# ### O que procurar no plano SQL com métricas
#
# | O que ver | Diagnóstico |
# |---|---|
# | `BroadcastExchange data size` muito grande | Broadcast de tabela grande → risco de OOM |
# | `number of output rows` explodindo entre nós | Join cartesiano ou condição de join errada |
# | `avg hash map probe` alto (> 2-3) | Muitas colisões no hash → chave com skew |
# | `Exchange shuffle bytes written` alto | Shuffle caro — considere broadcast ou bucketing |
# | Estimativas muito diferentes dos valores reais | Estatísticas desatualizadas → `ANALYZE TABLE` |
# | Muitos nós `Exchange` encadeados | Múltiplos shuffles — refatore a query |
# | `spill size` em algum nó | Memória insuficiente para aquele operador |

# COMMAND ----------

# MAGIC %md
# ## Aba 7 — ENVIRONMENT

# COMMAND ----------

# MAGIC %md
# ### O que você vê na aba Environment
#
# A aba Environment lista todas as configurações ativas do Spark nessa sessão.
# Dividida em seções:
#
# - **Spark Properties:** todas as configs `spark.*` — o que realmente está ativo
# - **Hadoop Properties:** configs do sistema de arquivos (HDFS, S3, ADLS)
# - **System Properties:** JVM system properties (versão Java, classpath, etc.)
# - **Runtime Information:** versão do Spark, versão Java, versão Scala, user
# - **Classpath Entries:** JARs no classpath

# COMMAND ----------

# Verificar programaticamente as configurações críticas
configs_importantes = [
    # Memória
    ("spark.executor.memory",              "Heap do Executor"),
    ("spark.executor.memoryOverhead",      "Overhead fora do heap"),
    ("spark.driver.memory",                "Heap do Driver"),
    ("spark.driver.maxResultSize",         "Limite de collect() no Driver"),
    ("spark.memory.fraction",              "Fração do heap para Unified Pool"),
    ("spark.memory.storageFraction",       "Fração protegida para Storage"),
    # Performance
    ("spark.sql.adaptive.enabled",         "AQE ativo"),
    ("spark.sql.codegen.wholeStage",       "Whole-Stage CodeGen"),
    ("spark.sql.autoBroadcastJoinThreshold","Threshold para broadcast automático"),
    ("spark.sql.shuffle.partitions",       "Partições padrão após shuffle"),
    # Delta
    ("spark.databricks.delta.optimizeWrite.enabled", "Delta: optimize write"),
    ("spark.databricks.delta.autoCompact.enabled",   "Delta: auto compaction"),
]

print(f"\n{'Configuração':<50} {'Valor':<20} {'Descrição'}")
print("=" * 100)
for config, descricao in configs_importantes:
    try:
        valor = spark.conf.get(config)
    except Exception:
        valor = "(não definido)"
    print(f"{config:<50} {valor:<20} {descricao}")

# COMMAND ----------

# MAGIC %md
# ## Aba 8 — STREAMING (quando aplicável)

# COMMAND ----------

# MAGIC %md
# ### O que você vê na aba Streaming
#
# Aparece apenas quando há um Structured Streaming query ativa.
#
# ```
# Active Streaming Queries:
# ┌─────────────────────────────────────────────────────────────────────┐
# │ Query: minha_query                                                  │
# │ Status: ACTIVE                                                      │
# │ Sources: [KafkaV2[subscribe=topico_vendas]]                        │
# │ Sinks: [DeltaSink[/mnt/delta/vendas_processadas]]                  │
# │                                                                     │
# │ Batch Statistics (últimos 20 micro-batches):                       │
# │ ┌────────┬──────────┬──────────┬──────────┬──────────┬──────────┐  │
# │ │ Batch  │ Input    │ Process  │ Trigger  │ Input    │ Batch    │  │
# │ │ ID     │ Rows/s   │ Rows/s   │ Time     │ Rows     │ Duration │  │
# │ ├────────┼──────────┼──────────┼──────────┼──────────┼──────────┤  │
# │ │ 142    │ 12,450   │ 14,200   │ 30s      │ 373,500  │ 28.5s    │  │
# │ │ 143    │ 13,100   │ 14,200   │ 30s      │ 393,000  │ 29.1s    │  │
# │ │ 144    │ 15,800   │ 14,200   │ 30s      │ 474,000  │ 33.4s    │← ATRASO
# └─────────────────────────────────────────────────────────────────────┘
#
# Batch 144: Input rows > Processing rows/s × Trigger time → acumulando atraso
# Solução: aumentar recursos do cluster ou otimizar a query
# ```
#
# **Métricas chave do Streaming:**
# - **Input rows/s:** taxa de chegada dos dados (Kafka, AutoLoader, etc.)
# - **Processing rows/s:** taxa de processamento (deve ser ≥ input rows/s)
# - **Batch Duration > Trigger Time:** o cluster não processa a tempo → lag acumulando
# - **Trigger Time:** tempo entre disparos do micro-batch

# COMMAND ----------

# MAGIC %md
# ## Guia de diagnóstico sistemático

# COMMAND ----------

# MAGIC %md
# ### Fluxo de investigação: "Por que esse job está lento?"
#
# ```
# 1. JOBS → Identifique qual job está demorando
#            └─ Clique no job → veja o DAG de stages
#                              └─ Qual stage está demorando?
#
# 2. STAGES → Abra a stage lenta
#              ├─ Summary Metrics: Duration Max >> Median?
#              │   └─ SIM → DATA SKEW → veja qual coluna tem distribuição desigual
#              ├─ Spill (Memory) > 0?
#              │   └─ SIM → MEMÓRIA INSUFICIENTE → mais partições ou mais memória
#              ├─ GC Time alto (>10% da task duration)?
#              │   └─ SIM → PRESSÃO DE MEMÓRIA → UDFs, coleções, memória insuficiente
#              └─ Shuffle Read desbalanceado entre tasks?
#                  └─ SIM → DATA SKEW na chave de shuffle → AQE skew join ou salting
#
# 3. SQL → Abra a query no plano visual
#           ├─ BroadcastExchange muito grande → risco de OOM
#           ├─ Muitos Exchange (shuffles) → refatore para broadcast ou bucketing
#           ├─ Estimativas muito diferentes dos valores reais → ANALYZE TABLE
#           └─ Nó com exploding rows → join cartesiano ou condição errada
#
# 4. EXECUTORS → Alguém com memória muito diferente dos outros?
#                 ├─ SIM → dado concentrado em um Executor → skew
#                 └─ Dead Executors? → OOM ou falha de nó → veja os logs
#
# 5. ENVIRONMENT → As configurações estão corretas para este workload?
#                   ├─ AQE ativo?
#                   ├─ Broadcast threshold adequado?
#                   └─ shuffle.partitions razoável?
# ```

# COMMAND ----------

# MAGIC %md
# ### Tabela de sintomas e diagnósticos rápidos
#
# | Sintoma no Spark UI | Aba | Diagnóstico | Solução |
# |---|---|---|---|
# | Stage Duration Max 10× > Median | Stages | Data skew | AQE skew join, salting |
# | Spill (Memory) > 0 em várias tasks | Stages | Memória insuficiente | Mais partições, mais memória |
# | GC Time > 10% da task | Stages / Tasks | Pressure no heap | Menos UDFs Python, mais overhead |
# | Shuffle Read muito desigual | Tasks | Skew na chave | Mesma solução de skew acima |
# | BroadcastExchange > 200 MB | SQL | Broadcast perigoso | Reduza threshold ou use SMJ |
# | Muitos Exchange no plano | SQL | Muitos shuffles | Broadcast, bucketing, reescreva a query |
# | Estimativa ≠ valor real (10×+) | SQL | Stats velhas | `ANALYZE TABLE COMPUTE STATISTICS` |
# | Executor com Disk Used > 0 | Executors | Spill naquele Executor | Distribuição desigual |
# | Dead Executor | Executors | OOM ou spot preemptado | Verifique logs, aumente memória |
# | Batch Duration > Trigger Time | Streaming | Lag acumulando | Mais recursos ou otimize query |
# | Cached Partitions < total | Storage | Cache evicted | Mais memória ou revise o que cachear |

# COMMAND ----------

# MAGIC %md
# ## ⚠️ Resumo de armadilhas e pontos de prova
#
# | Tema | O que saber |
# |---|---|
# | Physical Plan lido de baixo | O nó na base executa primeiro — leia de baixo para cima |
# | Métricas reais na aba SQL | São os únicos valores reais — o EXPLAIN são estimativas |
# | `Skipped` na Stage | Significa que o resultado veio do cache — comportamento correto |
# | Straggler task | 1 task com 10× mais dados que as outras → job inteiro espera ela |
# | Speculative execution | `spark.speculation=true` pode ajudar stragglers, mas não resolve skew |
# | Dead Executor ≠ job failure | Tasks são resubmitidas — job falha só se todas as tentativas falham |
# | GC Time alto | Sempre suspeite de Python UDFs ou objetos grandes em User Memory |
# | Environment aba | Confirme que AQE está ativo, broadcast threshold correto, configs de memória OK |
# | Streaming lag | Input rows/s > Processing rows/s × trigger interval = lag acumulando |
# | `avg hash map probe` | > 2 indica colisões no hash = skew na chave de join/agg |

# COMMAND ----------
