# Databricks notebook source

# MAGIC %md
# # 05 — Tungsten Engine: Whole-Stage CodeGen, Off-Heap e UnsafeRow
#
# **Analogia:**
# O Spark original funcionava como um chef que segue uma receita passo a passo em voz alta —
# lê um passo, executa, anota o resultado em um papel, passa para o próximo chef, que lê,
# executa, anota... Cada "papel" (objeto Java Row) precisa ser criado, preenchido, passado
# adiante e depois jogado fora pelo coletor de lixo (GC).
#
# O **Tungsten** mudou isso: o chef agora **memoriza a receita inteira e executa em fluxo
# contínuo** sem parar para anotar nada. Os ingredientes ficam organizados em prateleiras
# fora da cozinha principal (off-heap), onde o coletor de lixo não interfere.
# Em vez de caixas individuais (objetos), tudo é empilhado em blocos compactos de memória
# binária (UnsafeRow) — sem overhead de ponteiro Java, sem boxing/unboxing.
#
# **Conceito técnico:**
# O **Tungsten** é o motor de execução física do Spark, introduzido no Spark 1.4 e aprimorado
# continuamente. Opera em 3 dimensões:
# 1. **Whole-Stage CodeGen (WSCG):** compila múltiplos operadores em uma única função JVM,
#    eliminando overhead de chamadas virtuais e alocações intermediárias.
# 2. **Off-Heap Memory:** gerencia memória fora do heap da JVM usando `sun.misc.Unsafe`,
#    reduzindo GC pause e permitindo layouts de memória mais eficientes.
# 3. **UnsafeRow:** formato binário compacto de linha — sem objetos Java, sem boxing,
#    acesso por offset direto de memória.
#
# **Quando usar este conhecimento:**
# - Ao entender por que UDFs Python são mais lentas que funções nativas
# - Ao interpretar o Physical Plan (prefixos `*`, `InputAdapter`, `SerializeFromObject`)
# - Ao diagnosticar GC pressure e spill
# - Entrevistas sênior e prova Databricks Professional

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, pandas_udf, sum as spark_sum, length, upper
from pyspark.sql.types import StringType, LongType, DoubleType
import pandas as pd

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# ## 1. Whole-Stage Code Generation (WSCG)

# COMMAND ----------

# MAGIC %md
# ### O problema do modelo interpretado (pré-Tungsten)
#
# ```
# Spark original — modelo interpretado (Volcano / Iterator model):
#
#  Aggregation.next()
#     → chama Filter.next()
#         → chama Scan.next()
#             → retorna Row Java (objeto heap)
#         ← Filter verifica predicado → retorna Row ou chama next() de novo
#     ← Aggregation acumula → retorna Row
#
# Problemas:
# ┌─────────────────────────────────────────────────────────┐
# │ 1. Virtual dispatch: cada .next() é uma chamada de      │
# │    interface → JVM não consegue fazer inlining          │
# │                                                         │
# │ 2. Alocação de objetos: cada Row intermediária é um     │
# │    objeto Java no heap → pressão no GC                  │
# │                                                         │
# │ 3. Cache misses: objetos Java têm ponteiros espalhados  │
# │    → dados não ficam contíguos em memória               │
# └─────────────────────────────────────────────────────────┘
# ```

# COMMAND ----------

# MAGIC %md
# ### A solução: Whole-Stage CodeGen
#
# ```
# Com WSCG — modelo compilado (data-centric):
#
# O Catalyst gera código Java equivalente a este pseudocódigo:
#
#   // Tudo compilado em uma única função — sem chamadas de interface
#   void processPartition(Iterator<InternalRow> input) {
#       HashMap<Long, Long> agg = new HashMap<>();
#       while (input.hasNext()) {
#           UnsafeRow row = (UnsafeRow) input.next();
#           long key = row.getLong(0);           // acesso direto por offset
#           if (key > 100L) {                    // filtro inlined
#               long val = row.getLong(1);
#               agg.merge(key, val, Long::sum);  // agregação inlined
#           }
#       }
#       // emite resultados...
#   }
#
# Vantagens:
# ✓ Sem virtual dispatch — JIT consegue fazer inlining total
# ✓ Sem alocações intermediárias — menos GC
# ✓ Loop tight — CPU pipeline nunca para
# ✓ SIMD-friendly — dados contíguos em memória
# ```

# COMMAND ----------

# Identificando WSCG no Physical Plan
# Operadores dentro do mesmo "stage" de codegen têm o prefixo *(N)

df = spark.range(10_000_000).withColumn("valor", col("id") * 2)

resultado = (
    df
    .filter(col("valor") > 1000)      # ← vai ser inlined no mesmo loop
    .groupBy((col("id") % 100).alias("bucket"))
    .agg(spark_sum("valor").alias("total"))
)

print("=== Physical Plan — procure pelos prefixos *(1), *(2) ===")
resultado.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# ### Lendo os prefixos `*(N)` no Physical Plan
#
# ```
# == Physical Plan ==
#
# AdaptiveSparkPlan
#  +- HashAggregate [final]         ← *(2) — segundo stage de codegen
#     +- Exchange (shuffle)         ← SEM prefixo → fora do codegen (boundary)
#        +- HashAggregate [partial] ← *(1) — primeiro stage de codegen
#           +- Filter               ← *(1) — mesmo stage que o HashAggregate parcial
#              +- Range             ← *(1) — mesmo stage
#
# Regra: tudo com o mesmo número entre * está compilado em 1 função JVM.
# Exchange (shuffle), Sort, e Python UDFs são BOUNDARIES — quebram o codegen.
# ```
#
# **`InputAdapter`** aparece quando há transição entre codegen e não-codegen.
# Ele "adapta" a interface — sinal de que o fluxo foi interrompido.

# COMMAND ----------

# Verificar e configurar WSCG
print("WSCG ativo:", spark.conf.get("spark.sql.codegen.wholeStage"))
print("Fallback p/ interpretado se codegen falhar:",
      spark.conf.get("spark.sql.codegen.fallback"))
print("Número máx de campos para WSCG:",
      spark.conf.get("spark.sql.codegen.maxFields"))
# Se o DataFrame tiver mais colunas que maxFields (default: 100), WSCG é desativado

# COMMAND ----------

# MAGIC %md
# ### O que quebra o Whole-Stage CodeGen
#
# | Quebrador | Motivo | Solução |
# |---|---|---|
# | Python UDF | Caixa-preta para o JVM — precisa serializar/deserializar | Use funções nativas ou Pandas UDF |
# | Exchange (shuffle) | Boundary natural entre stages | Minimize shuffles |
# | Sort sem codegen | Alguns sorts externos | Normal — boundary esperado |
# | Schema com > 100 campos | `codegen.maxFields` | Aumente o limite ou reduza colunas |
# | `df.cache()` | InMemoryTableScan é um boundary | Use cache estrategicamente |

# COMMAND ----------

# Demonstração: UDF Python quebra o codegen

# Função nativa — compilada dentro do WSCG
df_base = spark.range(1_000_000)
df_nativo = df_base.withColumn("upper_id", (col("id") * 2 + 1))
print("=== Com função nativa (dentro do WSCG) ===")
df_nativo.explain(mode="simple")

# UDF Python — quebra o WSCG
@udf(returnType=LongType())
def dobra_python(x):
    return x * 2 + 1

df_udf = df_base.withColumn("upper_id", dobra_python(col("id")))
print("\n=== Com Python UDF (WSCG quebrado) ===")
df_udf.explain(mode="simple")
# Você verá: BatchEvalPython ou ArrowEvalPython → boundary do codegen

# COMMAND ----------

# MAGIC %md
# ### Pandas UDF (Arrow) — o meio-termo

# COMMAND ----------

# Pandas UDF usa Apache Arrow para transferência vetorizada entre JVM e Python
# É muito mais eficiente que UDF puro, mas ainda quebra o WSCG

@pandas_udf(LongType())
def dobra_pandas(serie: pd.Series) -> pd.Series:
    return serie * 2 + 1

df_pandas_udf = df_base.withColumn("upper_id", dobra_pandas(col("id")))
print("=== Com Pandas UDF (Arrow — mais eficiente que UDF puro) ===")
df_pandas_udf.explain(mode="simple")
# Você verá: ArrowEvalPython — melhor que BatchEvalPython, mas ainda um boundary

# COMMAND ----------

# MAGIC %md
# ## 2. UnsafeRow — Formato Binário Compacto

# COMMAND ----------

# MAGIC %md
# ### O problema com objetos Java Row
#
# ```
# Row Java convencional em memória heap:
#
# ┌─────────────────────────────────────────────────────────┐
# │ Object Header (16 bytes)                                │
# │ Pointer para campo 1 → [outro objeto no heap]          │
# │ Pointer para campo 2 → [outro objeto no heap]          │
# │ Pointer para campo 3 → [outro objeto no heap]          │
# └─────────────────────────────────────────────────────────┘
#
# Problemas:
# · Cada campo = ponteiro de 8 bytes → overhead enorme
# · Dados espalhados no heap → cache miss constante
# · Boxing de primitivos: long → Long (objeto) → GC pressure
# · Serialização para shuffle: Java serialize → lento e verboso
# ```
#
# ### A solução: UnsafeRow
#
# ```
# UnsafeRow em memória (contígua, sem ponteiros Java):
#
# ┌──────────┬────────────────────┬─────────────────────────┐
# │ Null     │  Fixed-length      │  Variable-length        │
# │ Bitmap   │  fields            │  fields (strings, etc)  │
# │ (N bits) │  (8 bytes each)    │  (length + bytes)       │
# └──────────┴────────────────────┴─────────────────────────┘
#  Offset 0   Offset 8*numFields   Offset calculado
#
# Acesso: row.getLong(fieldIndex) → lê diretamente do offset calculado
#         sem ponteiro, sem boxing, sem deserialização
#
# Vantagens:
# ✓ Dados contíguos → CPU cache-friendly
# ✓ Sem boxing de primitivos (long permanece long)
# ✓ Serialização para shuffle = copiar bytes brutos (ultra-rápido)
# ✓ Comparação de chaves = comparar bytes (sem equals() Java)
# ✓ Off-heap compatível (é só um bloco de bytes)
# ```

# COMMAND ----------

# UnsafeRow é o InternalRow usado internamente pelo Spark
# Você não trabalha diretamente com ele, mas pode ver seus efeitos

# Quando você faz df.collect() → Spark converte UnsafeRow → Row Python (overhead)
# Quando você usa funções nativas → tudo permanece UnsafeRow → máxima eficiência
# Quando você usa Python UDF → UnsafeRow → deserializa → Python → serializa → UnsafeRow

# Tamanho de UnsafeRow vs objeto Java:
# Uma Row com 5 campos Long: objeto Java ~200 bytes | UnsafeRow ~48 bytes (8+40)
print("UnsafeRow: formato interno do Spark — invisível para o usuário, mas central para performance")
print("Operações que mantêm tudo em UnsafeRow são sempre mais rápidas")

# COMMAND ----------

# MAGIC %md
# ## 3. Off-Heap Memory

# COMMAND ----------

# MAGIC %md
# ### Por que off-heap?
#
# ```
# Memória JVM (on-heap):
# ┌───────────────────────────────────────────────────────┐
# │                   JVM Heap                            │
# │  ┌─────────────┐  ┌──────────────┐  ┌─────────────┐  │
# │  │  Eden Space │→ │ Survivor     │→ │  Old Gen    │  │
# │  └─────────────┘  └──────────────┘  └─────────────┘  │
# │                                                       │
# │  GC Stop-The-World: JVM para TUDO para limpar →       │
# │  Spark UI mostra: "GC Time: 45s" em uma task de 60s  │
# └───────────────────────────────────────────────────────┘
#
# Memória Off-Heap (gerenciada pelo Tungsten via sun.misc.Unsafe):
# ┌───────────────────────────────────────────────────────┐
# │              Memória Nativa do Processo               │
# │  · Fora do alcance do GC Java                        │
# │  · Alocada/liberada manualmente pelo Tungsten         │
# │  · Usada para: shuffle buffers, sort buffers,        │
# │    aggregation hash tables, UnsafeRows em operações  │
# │    de shuffle e sort                                  │
# └───────────────────────────────────────────────────────┘
# ```

# COMMAND ----------

# Configurações de off-heap:
configs_offheap = {
    "spark.memory.offHeap.enabled": "Ativa uso de off-heap explícito",
    "spark.memory.offHeap.size": "Tamanho da região off-heap por Executor",
    "spark.executor.memoryOverhead": "Overhead fora do heap (Python, NIO, off-heap nativo)",
    "spark.executor.memoryOverheadFactor": "Fração do executor.memory para overhead (default: 0.1)",
}

print(f"\n{'Configuração':<45} {'Valor':<15} {'Descrição'}")
print("=" * 100)
for config, descricao in configs_offheap.items():
    try:
        valor = spark.conf.get(config)
    except Exception:
        valor = "(não definido)"
    print(f"{config:<45} {valor:<15} {descricao}")

# COMMAND ----------

# MAGIC %md
# ### Tungsten usa off-heap para operações críticas mesmo com offHeap.enabled=false
#
# O Tungsten usa `sun.misc.Unsafe` para:
# - **Sort externo:** PointerArray para sort de ponteiros (off-heap implícito)
# - **Shuffle write:** serialização direta em buffers binários
# - **Hash tables de agregação:** `UnsafeFixedWidthAggregationMap`
# - **Join hash tables:** `LongToUnsafeRowMap` no BHJ
#
# Quando `spark.memory.offHeap.enabled = true`, o Unified Memory Pool
# pode alocar fora do heap, reduzindo pressão no GC para operações de shuffle e sort.

# COMMAND ----------

# Off-heap para PySpark: sempre há overhead
# O processo Python roda FORA da JVM → comunicação via sockets/Arrow
# Por isso spark.executor.memoryOverhead deve ser aumentado em workloads PySpark pesados

# Recomendação para PySpark:
# spark.executor.memoryOverhead = max(384m, 0.1 × executor.memory) → padrão
# Para workloads com muitas UDFs Python ou pandas:
# spark.executor.memoryOverhead = 2g ou mais

print("Para PySpark com muitas UDFs: aumente spark.executor.memoryOverhead")
print("Regra prática: memoryOverhead >= 20% de executor.memory em workloads Python pesados")

# COMMAND ----------

# MAGIC %md
# ## 4. Vectorized Reader — Leitura Colunar com Tungsten

# COMMAND ----------

# MAGIC %md
# ### Row format vs Columnar format
#
# ```
# Dado: 3 linhas com 4 colunas (id, nome, valor, data)
#
# Row format (Parquet lido como Row):
# [1, "Ana", 100.0, "2024-01"]  [2, "Bruno", 200.0, "2024-02"]  [3, "Carla", 300.0, "2024-03"]
# Para filtrar valor > 150: lê TUDO e descarta os campos desnecessários
#
# Columnar format (Parquet lido colunar — Vectorized Reader):
# id:    [1, 2, 3]
# nome:  ["Ana", "Bruno", "Carla"]
# valor: [100.0, 200.0, 300.0]   ← só esta coluna é lida para o filtro
# data:  ["2024-01", "2024-02", "2024-03"]
#
# Com column pruning + vectorized reader:
# → Lê apenas a coluna "valor" do disco (column pruning)
# → Processa o array [100.0, 200.0, 300.0] com instruções SIMD
# → Resultado: [false, true, true] — sem ler nome ou data
# ```

# COMMAND ----------

# Verificar Vectorized Reader
print("Parquet Vectorized Reader:", spark.conf.get("spark.sql.parquet.enableVectorizedReader"))
print("Delta Vectorized Reader:", spark.conf.get("spark.sql.parquet.enableVectorizedReader"))

# Batch size do vectorized reader — quantas linhas são processadas por vez
print("Batch size:", spark.conf.get("spark.sql.parquet.columnarReaderBatchSize"))
# Default: 4096 linhas por batch — tamanho ideal para L1/L2 cache do CPU

# COMMAND ----------

# MAGIC %md
# ## 5. Benchmark prático: impacto do Tungsten

# COMMAND ----------

import time

df_bench = spark.range(50_000_000).withColumn("valor", col("id").cast(DoubleType()))

# COMMAND ----------

# Benchmark 1: função nativa (totalmente dentro do Tungsten/WSCG)
start = time.time()
resultado_nativo = df_bench.filter(col("valor") > 10_000_000).agg(spark_sum("valor")).collect()
tempo_nativo = time.time() - start
print(f"Função nativa (WSCG): {tempo_nativo:.2f}s | Resultado: {resultado_nativo[0][0]:.0f}")

# COMMAND ----------

# Benchmark 2: Python UDF (quebra WSCG, serialização Python↔JVM por linha)
@udf(returnType=DoubleType())
def filtra_python(x):
    return x if x > 10_000_000 else None

start = time.time()
resultado_udf = (
    df_bench
    .withColumn("filtrado", filtra_python(col("valor")))
    .filter(col("filtrado").isNotNull())
    .agg(spark_sum("filtrado"))
    .collect()
)
tempo_udf = time.time() - start
print(f"Python UDF (sem WSCG): {tempo_udf:.2f}s | Resultado: {resultado_udf[0][0]:.0f}")
print(f"Overhead da UDF Python: {tempo_udf/tempo_nativo:.1f}x mais lento")

# COMMAND ----------

# Benchmark 3: Pandas UDF com Arrow (vetorizado, mas ainda fora da JVM)
@pandas_udf(DoubleType())
def filtra_pandas(serie: pd.Series) -> pd.Series:
    return serie.where(serie > 10_000_000)

start = time.time()
resultado_pandas = (
    df_bench
    .withColumn("filtrado", filtra_pandas(col("valor")))
    .filter(col("filtrado").isNotNull())
    .agg(spark_sum("filtrado"))
    .collect()
)
tempo_pandas = time.time() - start
print(f"Pandas UDF (Arrow): {tempo_pandas:.2f}s | Resultado: {resultado_pandas[0][0]:.0f}")
print(f"Speedup vs Python UDF: {tempo_udf/tempo_pandas:.1f}x mais rápido")
print(f"Ainda {tempo_pandas/tempo_nativo:.1f}x mais lento que nativo")

# COMMAND ----------

# MAGIC %md
# ## 6. Referência rápida de configurações do Tungsten

# COMMAND ----------

tungsten_configs = {
    "spark.sql.codegen.wholeStage":       ("true",  "WSCG — nunca desative em produção"),
    "spark.sql.codegen.fallback":         ("true",  "Fallback p/ interpretado se codegen falhar"),
    "spark.sql.codegen.maxFields":        ("100",   "Máx campos para ativar WSCG"),
    "spark.sql.codegen.hugeMethodLimit":  ("65535", "Limite JVM de tamanho de método (bytes)"),
    "spark.sql.parquet.enableVectorizedReader": ("true", "Leitura vetorizada de Parquet/Delta"),
    "spark.sql.parquet.columnarReaderBatchSize": ("4096", "Linhas por batch no vectorized reader"),
    "spark.memory.offHeap.enabled":       ("false", "Off-heap explícito para Unified Memory Pool"),
    "spark.memory.offHeap.size":          ("0",     "Tamanho off-heap por Executor (se ativado)"),
}

print(f"\n{'Configuração':<50} {'Default':<10} {'Descrição'}")
print("=" * 110)
for config, (default, descricao) in tungsten_configs.items():
    try:
        valor_atual = spark.conf.get(config)
    except Exception:
        valor_atual = default
    marcador = "✓" if valor_atual == default else "≠"
    print(f"{marcador} {config:<48} {valor_atual:<10} {descricao}")

# COMMAND ----------

# MAGIC %md
# ## ⚠️ Resumo de armadilhas e pontos de prova
#
# | Tema | O que saber |
# |---|---|
# | WSCG e prefixo `*(N)` | Tudo com mesmo número = 1 função JVM compilada. Mais = melhor. |
# | Python UDF quebra WSCG | É o maior inimigo de performance em PySpark. Prefira funções nativas. |
# | Pandas UDF com Arrow | 5-10x mais rápido que UDF puro, mas ainda fora do JVM |
# | UnsafeRow é invisível | Você não vê, mas é o que roda. Operações que ficam em UnsafeRow são rápidas. |
# | Off-heap ≠ memoryOverhead | `offHeap.enabled` = controle explícito do pool. `memoryOverhead` = sempre existe (Python, NIO). |
# | GC pressure | Alta GC time na task = objetos Java demais no heap → UDFs Python ou operações que criam muitos objetos |
# | Vectorized reader | Lê Parquet/Delta em batches colunares. UDFs podem desativar isso. |
# | `codegen.maxFields` | DataFrames com >100 colunas perdem WSCG — projete schemas enxutos |
# | Tungsten é automático | Você não precisa ativar nada. Só precisa não quebrá-lo (UDFs, schema largo). |

# COMMAND ----------
