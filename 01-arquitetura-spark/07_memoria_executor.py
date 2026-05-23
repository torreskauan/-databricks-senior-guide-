# Databricks notebook source

# MAGIC %md
# # 07 — Memória do Executor: Unified Memory Model, Execution vs Storage Pool, Spill
#
# **Analogia:**
# Imagine o Executor como um restaurante com uma cozinha de tamanho fixo.
# A cozinha tem áreas diferentes: uma bancada de **trabalho ativo** (execution memory —
# onde se picam ingredientes, fritam, montam pratos) e uma **geladeira** (storage memory —
# onde ficam os ingredientes pré-processados, o cache de mise en place).
#
# No Spark antigo, essas áreas tinham tamanho fixo e não podiam emprestar espaço uma para
# a outra — se a bancada lotasse, o chef parava; se a geladeira estivesse vazia, espaço
# era desperdiçado.
#
# No **Unified Memory Model** (Spark 1.6+), a divisão é dinâmica: se a bancada precisar
# de mais espaço, ela pode tomar parte da geladeira (desde que ela não esteja muito cheia).
# E vice-versa. Quando a cozinha toda lota — **spill**: o chef começa a usar uma mesa
# auxiliar no corredor (disco). Tudo ainda funciona, mas mais devagar.
#
# **Conceito técnico:**
# O Unified Memory Model gerencia a memória do Executor em regiões com fronteiras dinâmicas.
# A região principal é dividida em **Execution Memory** (operações: shuffle, sort, join,
# aggregation) e **Storage Memory** (cache, broadcast). Ambas pertencem ao mesmo pool
# unificado e podem "emprestar" memória uma da outra conforme a demanda. Quando o pool
# se esgota, operações de execução fazem **spill** para disco — degradando performance
# mas evitando OOM. Storage memory pode ser **evicted** (despejada) para liberar espaço.
#
# **Quando usar este conhecimento:**
# - Ao dimensionar clusters: quanto de memória por Executor?
# - Ao diagnosticar spill, OOM e GC no Spark UI
# - Ao tunar `spark.memory.fraction` e `spark.memory.storageFraction`
# - Ao decidir quando usar cache e quando não vale a pena
# - Entrevistas sênior e prova Databricks Professional

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, spark_partition_id, count
from pyspark.sql.types import DoubleType, LongType

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# ## 1. Mapa completo da memória do Executor
#
# ```
# ┌──────────────────────────────────────────────────────────────────────────────┐
# │                spark.executor.memory  (ex: 8g — heap JVM)                   │
# │                                                                              │
# │  ┌──────────────────────────────────────────────────────────────────────┐   │
# │  │  Reserved Memory  (~300 MB fixo — uso interno do Spark)              │   │
# │  │  · Objetos internos do Spark, metadados, classes                    │   │
# │  │  · Não configurável. Se executor.memory < 1.5× reserved → erro     │   │
# │  └──────────────────────────────────────────────────────────────────────┘   │
# │                                                                              │
# │  Usable Memory = executor.memory − 300 MB                                   │
# │                                                                              │
# │  ┌──────────────────────────────────────────────────────────────────────┐   │
# │  │  spark.memory.fraction  (default: 0.6) × Usable                     │   │
# │  │                                                                      │   │
# │  │  UNIFIED MEMORY POOL  ← fronteira dinâmica entre Execution/Storage  │   │
# │  │                                                                      │   │
# │  │  ┌────────────────────────┐  ┌─────────────────────────────────┐   │   │
# │  │  │  EXECUTION MEMORY      │  │  STORAGE MEMORY                  │   │   │
# │  │  │                        │  │                                  │   │   │
# │  │  │  · Shuffle read/write  │  │  · df.cache() / df.persist()    │   │   │
# │  │  │  · Sort buffers        │  │  · Broadcast variables           │   │   │
# │  │  │  · Join hash tables    │  │  · Resultados de unroll de RDD  │   │   │
# │  │  │  · Aggregation maps    │  │                                  │   │   │
# │  │  │                        │  │  storageFraction (default: 0.5)  │   │   │
# │  │  │  Pode tomar Storage se │  │  = parte protegida do pool       │   │   │
# │  │  │  Storage não estiver   │  │  Execution pode tomar o resto    │   │   │
# │  │  │  usando                │←→│  Storage pode ser evicted se     │   │   │
# │  │  │                        │  │  Execution precisar              │   │   │
# │  │  └────────────────────────┘  └─────────────────────────────────┘   │   │
# │  └──────────────────────────────────────────────────────────────────────┘   │
# │                                                                              │
# │  ┌──────────────────────────────────────────────────────────────────────┐   │
# │  │  User Memory  = (1 − spark.memory.fraction) × Usable                │   │
# │  │  · Estruturas de dados do seu código Python/Scala                   │   │
# │  │  · Metadados de UDFs, coleções internas                             │   │
# │  │  · O Spark NÃO gerencia — OOM se estourar                          │   │
# │  └──────────────────────────────────────────────────────────────────────┘   │
# │                                                                              │
# └──────────────────────────────────────────────────────────────────────────────┘
#
#  + spark.executor.memoryOverhead  (FORA do heap JVM — não configurado acima)
#    · Processos Python (PySpark workers)
#    · Buffers NIO do Netty (comunicação de rede)
#    · Metadados de containers (YARN/Kubernetes)
#    · Default: max(384m, 0.1 × executor.memory)
#    · Para PySpark pesado: aumente para 20-30% de executor.memory
# ```

# COMMAND ----------

# MAGIC %md
# ## 2. Calculando os pools na prática

# COMMAND ----------

def calcular_memoria_pools(executor_memory_gb: float,
                            memory_fraction: float = 0.6,
                            storage_fraction: float = 0.5) -> None:
    """
    Calcula e exibe a distribuição de memória do Executor
    conforme o Unified Memory Model do Spark.
    """
    RESERVED_MB = 300
    executor_mb = executor_memory_gb * 1024

    usable_mb = executor_mb - RESERVED_MB
    unified_pool_mb = usable_mb * memory_fraction
    user_memory_mb = usable_mb * (1 - memory_fraction)

    # storageFraction define a parte PROTEGIDA do pool unificado
    # (Execution não pode forçar eviction abaixo deste limite)
    storage_protected_mb = unified_pool_mb * storage_fraction
    # O restante do pool pode ser usado por qualquer um (dinâmico)
    execution_initial_mb = unified_pool_mb * (1 - storage_fraction)

    overhead_mb = max(384, executor_mb * 0.1)
    total_process_mb = executor_mb + overhead_mb

    print(f"{'='*60}")
    print(f"  DISTRIBUIÇÃO DE MEMÓRIA — Executor {executor_memory_gb}g")
    print(f"{'='*60}")
    print(f"  executor.memory (heap JVM)       : {executor_mb:>8.0f} MB ({executor_memory_gb}g)")
    print(f"  ├─ Reserved Memory (fixo)        : {RESERVED_MB:>8.0f} MB")
    print(f"  ├─ Usable Memory                 : {usable_mb:>8.0f} MB")
    print(f"  │   ├─ Unified Pool ({memory_fraction*100:.0f}%)         : {unified_pool_mb:>8.0f} MB")
    print(f"  │   │   ├─ Storage protegida ({storage_fraction*100:.0f}%): {storage_protected_mb:>8.0f} MB")
    print(f"  │   │   └─ Execution inicial ({(1-storage_fraction)*100:.0f}%): {execution_initial_mb:>8.0f} MB")
    print(f"  │   │       [ambos podem usar o pool inteiro dinamicamente]")
    print(f"  │   └─ User Memory ({(1-memory_fraction)*100:.0f}%)         : {user_memory_mb:>8.0f} MB")
    print(f"  └─ memoryOverhead (fora do heap) : {overhead_mb:>8.0f} MB")
    print(f"  {'─'*50}")
    print(f"  TOTAL processo por Executor      : {total_process_mb:>8.0f} MB")
    print(f"{'='*60}\n")

# Exemplos de configurações comuns
calcular_memoria_pools(executor_memory_gb=4)
calcular_memoria_pools(executor_memory_gb=8)
calcular_memoria_pools(executor_memory_gb=16, memory_fraction=0.7)

# COMMAND ----------

# Verificar as configurações atuais do cluster
print("Configurações de memória do cluster atual:")
print(f"  spark.executor.memory           : {spark.conf.get('spark.executor.memory', '(não definido)')}")
print(f"  spark.executor.memoryOverhead   : {spark.conf.get('spark.executor.memoryOverhead', '(usa o default 10%)')}")
print(f"  spark.memory.fraction           : {spark.conf.get('spark.memory.fraction')}")
print(f"  spark.memory.storageFraction    : {spark.conf.get('spark.memory.storageFraction')}")
print(f"  spark.memory.offHeap.enabled    : {spark.conf.get('spark.memory.offHeap.enabled')}")
print(f"  spark.memory.offHeap.size       : {spark.conf.get('spark.memory.offHeap.size', '0')}")

# COMMAND ----------

# MAGIC %md
# ## 3. Execution Memory — O que usa e como gerencia

# COMMAND ----------

# MAGIC %md
# ### Operações que consomem Execution Memory
#
# ```
# SHUFFLE WRITE (estágio do Map):
#   · Serializa registros e os organiza por partição de destino
#   · Usa ExternalSorter com um buffer in-memory (spark.shuffle.file.buffer: 32k default)
#   · Quando o buffer enche → spill para disco como arquivo de shuffle temporário
#
# SHUFFLE READ (estágio do Reduce):
#   · Busca os blocos de shuffle dos outros Executors via rede
#   · Armazena em buffers para decodificação (spark.reducer.maxSizeInFlight: 48m)
#   · Alimenta a operação seguinte (aggregation, sort, join)
#
# SORT:
#   · TimSort in-memory para partições que cabem
#   · External sort com merge de arquivos quando não cabe → spill
#
# HASH AGGREGATION (HashAggregate):
#   · Mantém um HashMap em memória: chave → valor acumulado
#   · Quando o HashMap excede a memória disponível → spill → external aggregation
#
# JOIN — Sort Merge Join:
#   · Sort de ambos os lados → usa execution memory
#   · Se não cabe → spill → external sort
#
# JOIN — Shuffle Hash Join:
#   · Constrói hash table do lado menor → usa execution memory
#   · SHJ NÃO faz spill → se não couber → OOM → Spark muda para SMJ (com AQE)
# ```

# COMMAND ----------

# Configurações da Execution Memory
exec_configs = {
    "spark.shuffle.file.buffer":            "32k   — Buffer de escrita do shuffle write",
    "spark.reducer.maxSizeInFlight":        "48m   — Máx dados em voo no shuffle read",
    "spark.shuffle.sort.bypassMergeThreshold": "200 — Partições abaixo disso → bypass sort no shuffle",
    "spark.sql.shuffle.partitions":         "200   — Partições após um shuffle (ajustado pelo AQE)",
    "spark.shuffle.spill.compress":         "true  — Comprime dados spilled no shuffle",
    "spark.shuffle.compress":               "true  — Comprime blocos de shuffle",
}

print(f"\n{'Configuração':<45} {'Valor Atual':<15} {'Descrição'}")
print("=" * 100)
for config, descricao in exec_configs.items():
    try:
        valor = spark.conf.get(config)
    except Exception:
        valor = "(default)"
    print(f"{config:<45} {valor:<15} {descricao}")

# COMMAND ----------

# MAGIC %md
# ## 4. Storage Memory — Cache, Persist e Broadcast

# COMMAND ----------

# MAGIC %md
# ### StorageLevels — onde os dados ficam

# COMMAND ----------

from pyspark import StorageLevel

# Os StorageLevels definem onde e como os dados são armazenados
niveis = {
    "MEMORY_ONLY":          "Heap JVM. Sem serialização. Mais rápido para acesso, mais GC.",
    "MEMORY_ONLY_SER":      "Heap JVM serializado (Kryo). Menos GC, acesso levemente mais lento.",
    "MEMORY_AND_DISK":      "Heap primeiro, spill para disco se não couber. Mais comum.",
    "MEMORY_AND_DISK_SER":  "Heap serializado + disco. Menos memória, mais CPU.",
    "DISK_ONLY":            "Apenas disco. Lento mas não ocupa memória.",
    "OFF_HEAP":             "Memória off-heap (requer offHeap.enabled=true). Menos GC.",
}

print("StorageLevels disponíveis:\n")
for nivel, descricao in niveis.items():
    print(f"  StorageLevel.{nivel:<25} → {descricao}")

# COMMAND ----------

# Demonstração de cache e persist
df_grande = (
    spark.range(10_000_000)
    .withColumn("valor", (col("id") * rand()).cast(DoubleType()))
    .withColumn("categoria", (col("id") % 10).cast(LongType()))
)

# cache() = MEMORY_AND_DISK com serialização (no Spark 3+, desserializado no Databricks)
df_grande.cache()
df_grande.count()  # materializa o cache

# persist() com nível explícito
df_outra = spark.range(5_000_000).withColumn("x", rand())
df_outra.persist(StorageLevel.MEMORY_AND_DISK)
df_outra.count()

# COMMAND ----------

# Verificar o que está no Storage no momento
print("Storage Memory — DataFrames em cache:")
for rdd_info in spark.sparkContext._jsc.sc().getRDDStorageInfo():
    print(f"  RDD ID: {rdd_info.id()}")
    print(f"  Nome:   {rdd_info.name()}")
    print(f"  Nível:  {rdd_info.storageLevel()}")
    print(f"  Memória usada: {rdd_info.memSize() / 1024 / 1024:.1f} MB")
    print(f"  Disco usado:   {rdd_info.diskSize() / 1024 / 1024:.1f} MB")
    print()

# COMMAND ----------

# Liberar cache explicitamente — SEMPRE faça isso quando não precisar mais
df_grande.unpersist()
df_outra.unpersist()
print("Cache liberado.")

# COMMAND ----------

# MAGIC %md
# ### Regras para uso de cache
#
# | Situação | Cache? | Motivo |
# |---|---|---|
# | DataFrame usado 1× | ❌ Não | Overhead de serialização sem benefício |
# | DataFrame usado 2+ vezes em ações diferentes | ✅ Sim | Evita recomputação do DAG |
# | DataFrame lido de fonte externa (S3/ADLS) usado 3+ vezes | ✅ Sim | Evita I/O repetido |
# | DataFrame de shuffle intermediário | ⚠️ Avaliar | AQE pode ser suficiente |
# | DataFrame que cabe confortavelmente na memória | ✅ Sim | MEMORY_ONLY para máxima velocidade |
# | DataFrame maior que memória disponível | ⚠️ MEMORY_AND_DISK | Evita recomputação parcial |
# | Loop de ML / iterativo | ✅ Sim forte | Reuso intenso do mesmo DataFrame |

# COMMAND ----------

# MAGIC %md
# ## 5. Spill — Quando a memória não é suficiente

# COMMAND ----------

# MAGIC %md
# ### Como o spill funciona
#
# ```
# EXECUTION MEMORY durante uma operação de sort/shuffle/agg:
#
# ┌─────────────────────────────────────────────┐
# │  Execution Memory (ex: 2 GB disponíveis)    │
# │                                             │
# │  [dados em memória: 1.8 GB]                 │
# │  [novos dados chegando: +500 MB]            │
# │                                             │
# │  Total necessário: 2.3 GB > 2 GB → SPILL   │
# └─────────────────────────────────────────────┘
#         │
#         ▼  Spill para disco local do Executor
# ┌─────────────────────────────────────────────┐
# │  Disco local (spark.local.dir)              │
# │  spill_file_1.dat (serializado + comprimido)│
# └─────────────────────────────────────────────┘
#         │
#         ▼  Ao finalizar a operação:
#   Merge dos arquivos de spill + dados restantes em memória
#   (External Sort / External Aggregation)
#
# Custo do spill:
#   · Serialização + compressão → CPU
#   · Escrita em disco → I/O (mesmo SSD: 500 MB/s vs memória: 50 GB/s)
#   · Leitura + deserialização na fase de merge → mais CPU e I/O
#   → Spill pode tornar uma stage 10-100x mais lenta
# ```

# COMMAND ----------

# MAGIC %md
# ### Identificando spill no Spark UI
#
# **Aba Stages → coluna "Spill (Memory)":**
# - Spill (Memory): quanto de dados foram derramados (tamanho em memória antes de serializar)
# - Spill (Disk): tamanho no disco após serialização/compressão (geralmente menor)
#
# **Aba Stages → clicar em uma Stage → Tasks:**
# - Cada task mostra seu spill individual
# - Tasks com spill alto + duração alta = gargalo
#
# **Diagnóstico:** se algumas tasks têm spill muito maior que outras → **data skew**
# Se todas as tasks têm spill → memória insuficiente para o volume de dados

# COMMAND ----------

# Forçar spill para demonstração (reduzir memória disponível artificialmente)
# ATENÇÃO: apenas para fins educacionais — não use em produção

# Configuração que força spill mais cedo (reduz o threshold):
spark.conf.set("spark.sql.shuffle.partitions", "4")  # poucas partições = mais dados por task

df_spill_demo = (
    spark.range(20_000_000)
    .withColumn("chave", (col("id") % 5).cast(LongType()))
    .withColumn("valor", rand())
)

# Esta operação com poucas partições e muitos dados provavelmente causará spill
resultado_spill = (
    df_spill_demo
    .groupBy("chave")
    .agg({"valor": "sum", "id": "count"})
)
resultado_spill.show()
# Verifique no Spark UI: Stages → a stage do groupBy deve mostrar Spill > 0

# Restaurar
spark.conf.set("spark.sql.shuffle.partitions", "200")

# COMMAND ----------

# MAGIC %md
# ## 6. Estratégias para reduzir/eliminar Spill

# COMMAND ----------

# MAGIC %md
# ### Estratégia 1: Aumentar o número de partições (reduz dados por task)

# COMMAND ----------

# Mais partições = menos dados por task = menos pressão de memória por task
# AQE faz isso automaticamente para shuffles — verifique se está ativo
print("AQE ativo:", spark.conf.get("spark.sql.adaptive.enabled"))
print("AQE coalesce partições:", spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled"))

# Para forçar mais partições manualmente:
# spark.conf.set("spark.sql.shuffle.partitions", "400")  # 2× o default

# COMMAND ----------

# MAGIC %md
# ### Estratégia 2: Aumentar executor.memory ou reduzir executor.cores

# COMMAND ----------

# Menos cores por Executor = mais memória disponível por core/task
# Exemplo:
#   8g executor, 4 cores → 2g por task (pool unificado total / cores)
#   8g executor, 2 cores → 4g por task
#
# Regra prática para clusters Databricks:
# · Workers com 32g RAM → executor.memory=24g, 4 cores → 6g por task
# · Workers com 16g RAM → executor.memory=12g, 2 cores → 6g por task

def calcular_memoria_por_task(executor_memory_gb, num_cores,
                               memory_fraction=0.6, storage_fraction=0.5):
    reserved = 0.3  # 300 MB em GB
    usable = executor_memory_gb - reserved
    unified = usable * memory_fraction
    execution_pool = unified  # no pior caso Execution usa o pool inteiro
    por_task = execution_pool / num_cores
    print(f"  {executor_memory_gb}g / {num_cores} cores → "
          f"{por_task:.1f}g por task (execution pool compartilhado)")

print("Memória de execution disponível por task (estimativa):")
for mem, cores in [(4, 2), (4, 4), (8, 2), (8, 4), (16, 4), (32, 8)]:
    calcular_memoria_por_task(mem, cores)

# COMMAND ----------

# MAGIC %md
# ### Estratégia 3: Reparticionamento antes de operações pesadas

# COMMAND ----------

# Se uma operação específica causa spill, reparticionar antes pode distribuir melhor os dados
df_pesado = spark.range(50_000_000).withColumn("grp", (col("id") % 100).cast(LongType()))

# Sem reparticionamento manual (depende do AQE)
df_pesado.groupBy("grp").count().explain(mode="simple")

# Com reparticionamento explícito antes do groupBy
df_pesado.repartition(200, "grp").groupBy("grp").count().explain(mode="simple")
# Repartition por "grp" garante que todos os registros da mesma chave
# vão para a mesma partição — evita segundo shuffle no groupBy

# COMMAND ----------

# MAGIC %md
# ### Estratégia 4: Ajustar spark.memory.fraction

# COMMAND ----------

# Aumentar memory.fraction dá mais memória para o pool unificado
# MAS reduz User Memory — pode causar OOM se seu código cria muitas estruturas Python

# Default: 0.6
# Para workloads com muito shuffle e pouca lógica Python:
# spark.conf.set("spark.memory.fraction", "0.75")

# Para workloads com muito Python (pandas, UDFs, coleções grandes):
# Mantenha em 0.6 ou até reduza e aumente executor.memoryOverhead

# COMMAND ----------

# MAGIC %md
# ## 7. OOM vs Spill — Qual é qual?

# COMMAND ----------

# MAGIC %md
# ```
# SPILL (comportamento esperado e recuperável):
# ┌──────────────────────────────────────────────────────────────────┐
# │  · Execution Memory está cheia                                   │
# │  · Spark serializa e despeja para disco local                   │
# │  · A operação CONTINUA, mas mais devagar                        │
# │  · Você vê: "Spill (Memory)" no Spark UI                        │
# │  · Operações que fazem spill: sort, shuffle, hash aggregation   │
# │  · SortMergeJoin: faz spill                                     │
# │  · HashAggregate: faz spill                                     │
# │  · ShuffledHashJoin: NÃO faz spill → pode causar OOM            │
# └──────────────────────────────────────────────────────────────────┘
#
# OOM — OutOfMemoryError (falha):
# ┌──────────────────────────────────────────────────────────────────┐
# │  · Memória esgotada e não é possível fazer spill                 │
# │  · Ou: região que não permite spill (User Memory, off-heap)     │
# │                                                                  │
# │  Causas comuns:                                                  │
# │  · collect() de DataFrame grande → Driver OOM                   │
# │  · broadcast() de tabela grande → Executor OOM                  │
# │  · ShuffledHashJoin com hash table enorme → Executor OOM        │
# │  · UDF Python acumulando dados → Worker Python OOM              │
# │  · User Memory lotada (coleções Python gigantes no código)      │
# │                                                                  │
# │  Diagnóstico no log:                                            │
# │  java.lang.OutOfMemoryError: Java heap space   → heap JVM      │
# │  java.lang.OutOfMemoryError: GC overhead limit → GC travado    │
# │  Python worker exited unexpectedly              → Python OOM   │
# └──────────────────────────────────────────────────────────────────┘
# ```

# COMMAND ----------

# MAGIC %md
# ## 8. Checklist de diagnóstico de problemas de memória

# COMMAND ----------

# MAGIC %md
# ```
# Sintoma → Diagnóstico → Solução
#
# ┌─ Alta "GC Time" nas tasks (>10% da duração)
# │   └─ Muitos objetos Java no heap
# │       └─ Causas: Python UDFs, objetos User Memory, cache muito grande
# │       └─ Soluções: use funções nativas, aumente memoryOverhead (PySpark),
# │                    reduza cache, aumente executor.memory
# │
# ├─ "Spill (Memory)" alto em uma Stage
# │   ├─ Todas as tasks espillam → memória insuficiente para o volume
# │   │   └─ Soluções: aumente shuffle.partitions, aumente executor.memory,
# │   │                reduza executor.cores (mais memória por task)
# │   └─ Poucas tasks espillam muito (outras ok) → data skew
# │       └─ Soluções: AQE skew join, salting na chave, repartition
# │
# ├─ OOM: "Java heap space"
# │   ├─ No Driver → collect/toPandas de dado grande
# │   │   └─ Nunca colete dados grandes; escreva em storage
# │   └─ No Executor → operação sem spill (SHJ, broadcast grande)
# │       └─ Desative broadcast forçado; use SMJ; aumente executor.memory
# │
# ├─ OOM: "Python worker exited"
# │   └─ Processo Python do Executor ficou sem memória
# │       └─ Aumente spark.executor.memoryOverhead (2-4g para PySpark pesado)
# │
# └─ "Lost Executor" + task failures
#     └─ Executor morreu (GC extremo ou OOM não capturado)
#         └─ Verifique logs do Executor; aumente memória ou reduza carga
# ```

# COMMAND ----------

# MAGIC %md
# ## 9. Configurações de referência por tipo de workload

# COMMAND ----------

print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║              REFERÊNCIA DE CONFIGURAÇÕES POR WORKLOAD                       ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  WORKLOAD: ETL pesado com muito shuffle (joins, groupBy grandes)             ║
║  spark.memory.fraction          = 0.70  (mais espaço para execution)        ║
║  spark.memory.storageFraction   = 0.30  (menos cache, mais execution)       ║
║  spark.sql.shuffle.partitions   = AQE   (deixe o AQE decidir)               ║
║  spark.executor.memoryOverhead  = 512m  (Scala/Java — menos Python)         ║
║                                                                              ║
║  WORKLOAD: PySpark com muitas UDFs e pandas                                  ║
║  spark.memory.fraction          = 0.60  (default — preserve user memory)    ║
║  spark.executor.memoryOverhead  = 2g    (processos Python precisam de espaço)║
║  spark.sql.execution.arrow.pyspark.enabled = true (Arrow para Pandas UDFs) ║
║                                                                              ║
║  WORKLOAD: ML iterativo / múltiplos usos do mesmo DataFrame                  ║
║  spark.memory.storageFraction   = 0.50  (default — cache equilibrado)       ║
║  StorageLevel                   = MEMORY_AND_DISK  (segurança vs velocidade)║
║                                                                              ║
║  WORKLOAD: Streaming (micro-batch)                                           ║
║  spark.memory.fraction          = 0.60  (default)                           ║
║  spark.executor.memoryOverhead  = 1g    (state store precisa de memória)    ║
║  Evite cache de DataFrames no streaming — use Delta como checkpoint          ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")

# COMMAND ----------

# MAGIC %md
# ## ⚠️ Resumo de armadilhas e pontos de prova
#
# | Tema | O que saber |
# |---|---|
# | Reserved memory | ~300 MB sempre consumidos. executor.memory deve ser > ~450 MB |
# | Unified pool é dinâmico | Execution e Storage se emprestam — não são compartimentos fixos |
# | storageFraction = piso, não teto | Define quanto Storage está protegido de Execution, não o máximo |
# | Spill = degradação, não falha | A task termina, mas 10-100× mais devagar |
# | SHJ não tem spill | ShuffledHashJoin → OOM se a hash table não couber |
# | memoryOverhead ≠ offHeap | Overhead = sempre existe (Python, NIO). offHeap = pool opcional. |
# | GC alto = objetos demais no heap | Python UDFs, coleções User Memory, cache grande |
# | executor.cores afeta memória por task | Menos cores = mais memória por task = menos spill |
# | cache() não é gratuito | Ocupa Storage Memory. Se não reusar 2+ vezes, não vale |
# | unpersist() explicitamente | Spark remove cache por LRU, mas liberar explicitamente é mais seguro |

# COMMAND ----------
