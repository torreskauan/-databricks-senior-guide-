# Databricks notebook source

# MAGIC %md
# # 09 — Cache e Persist
#
# > **Arquivo:** `02-pyspark-api/09_cache_persist.py`
# > **Módulo:** 02 — PySpark API
# > **Dependência:** `08_leitura_escrita.py`
#
# ---
#
# ## Analogia
#
# Cache é como um bloco de rascunho na sua mesa.
# Se você vai consultar a mesma tabela de CEPs vinte vezes durante
# o dia, faz sentido deixá-la no rascunho — você copia uma vez
# e consulta vinte vezes sem ir ao arquivo.
#
# Mas se você vai consultar apenas uma vez e jogar fora,
# você desperdiçou espaço na mesa com algo que não precisava.
# E se você lotou a mesa de rascunhos que nunca usa,
# fica sem espaço para o trabalho real.
#
# Cache mal usado é pior que não usar cache:
# consome memória, aumenta GC pressure, e o Spark pode despejar
# dados de cache que você de fato precisava.
#
# ---
#
# ## A regra de ouro antes de qualquer código
#
# **Só faça cache se o DataFrame for lido 2 ou mais vezes no mesmo job.**
# Se for lido uma vez, cache só adiciona overhead.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg
from pyspark.storage import StorageLevel
import time

# COMMAND ----------

# MAGIC %md
# ## 1. cache() — o atalho conveniente

# COMMAND ----------

# MAGIC %md
# ### O que cache() faz internamente
#
# `.cache()` é um atalho para `.persist(StorageLevel.MEMORY_AND_DISK)`
# no PySpark (no Scala é `MEMORY_ONLY`).
#
# Importante: cache() é lazy — assim como transformações, ele não
# materializa o DataFrame imediatamente. A materialização acontece
# na primeira Action após o cache().

# COMMAND ----------

# ── Exemplo: sem cache vs com cache ──────────────────────────────────────

# Cenário: você precisa fazer 3 aggregations diferentes no mesmo DataFrame base
# que veio de uma leitura custosa (join de 3 tabelas grandes)

df_base = (
    spark.read.table("prod.silver.pedidos")
    .join(spark.read.table("prod.silver.clientes"), "id_cliente", "left")
    .join(spark.read.table("prod.silver.produtos"),  "id_produto", "left")
    .filter(col("status") == "PAGO")
)

# ── SEM cache ─────────────────────────────────────────────────────────────
# Cada Action abaixo relê as 3 tabelas e refaz o join — 3 vezes no total
t0 = time.time()
total_pedidos  = df_base.count()                                        # Action 1 → relê tudo
total_valor    = df_base.agg(spark_sum("valor")).collect()[0][0]        # Action 2 → relê tudo
media_por_uf   = df_base.groupBy("uf").agg(avg("valor")).collect()      # Action 3 → relê tudo
print(f"Sem cache: {time.time() - t0:.1f}s")

# ── COM cache ─────────────────────────────────────────────────────────────
df_base.cache()                                    # marca para cache (lazy)

t0 = time.time()
total_pedidos  = df_base.count()                   # Action 1 → materializa o cache
total_valor    = df_base.agg(spark_sum("valor")).collect()[0][0]  # Action 2 → lê do cache
media_por_uf   = df_base.groupBy("uf").agg(avg("valor")).collect()  # Action 3 → lê do cache
print(f"Com cache: {time.time() - t0:.1f}s")

# OBRIGATÓRIO: libere quando não precisar mais
df_base.unpersist()

# COMMAND ----------

# MAGIC %md
# ### ⚠️ cache() é lazy — a armadilha mais comum

# COMMAND ----------

# ── Demonstração: cache não materializa imediatamente ────────────────────

df = spark.read.table("prod.silver.pedidos")

df.cache()   # ← NÃO faz nada ainda. Apenas registra a intenção.

# O cache é materializado na PRIMEIRA Action após o cache()
df.count()   # ← AQUI o DataFrame é lido e armazenado em memória/disco

# A partir daqui, todas as Actions usam o cache
df.filter(col("status") == "PAGO").count()   # ← lê do cache ✅
df.groupBy("regiao").count().show()          # ← lê do cache ✅

df.unpersist()

# COMMAND ----------

# ── Materializar o cache explicitamente com count() ───────────────────────

# Padrão: cache + count() para garantir materialização imediata
df_importante = spark.read.table("prod.silver.pedidos").filter(col("ativo") == True)
df_importante.cache()
df_importante.count()   # materializa agora — não na próxima operação

# Útil quando você quer garantir que o cache está pronto
# antes de distribuir múltiplas tasks que dependem dele

# COMMAND ----------

# MAGIC %md
# ## 2. persist() — controle granular do StorageLevel

# COMMAND ----------

# MAGIC %md
# ### Os StorageLevels disponíveis

# COMMAND ----------

from pyspark.storage import StorageLevel

# ── Todos os StorageLevels relevantes ────────────────────────────────────

storage_levels = [
    (StorageLevel.MEMORY_ONLY,
     "MEMORY_ONLY",
     "Apenas RAM (JVM heap). Se não couber: NÃO armazena, recalcula quando precisar.",
     "DataFrame pequeno que SEMPRE cabe na memória. Mais rápido para leitura."),

    (StorageLevel.MEMORY_AND_DISK,
     "MEMORY_AND_DISK",
     "RAM primeiro. Overflow vai para disco local do executor.",
     "Padrão do .cache() em PySpark. Seguro para qualquer tamanho."),

    (StorageLevel.MEMORY_ONLY_SER,
     "MEMORY_ONLY_SER",
     "RAM serializado (Kryo/Java). Menor uso de memória que MEMORY_ONLY.",
     "Quando MEMORY_ONLY causa GC excessivo — dados mais compactos."),

    (StorageLevel.MEMORY_AND_DISK_SER,
     "MEMORY_AND_DISK_SER",
     "RAM serializado + disco se necessário.",
     "Compromisso entre tamanho e velocidade. Bom para datasets médios-grandes."),

    (StorageLevel.DISK_ONLY,
     "DISK_ONLY",
     "Apenas disco local. Sem uso de RAM.",
     "DataFrame muito grande que não precisa de baixa latência."),

    (StorageLevel.MEMORY_AND_DISK_2,
     "MEMORY_AND_DISK_2",
     "MEMORY_AND_DISK com replicação em 2 nós.",
     "Fault-tolerance: se um executor cair, o outro tem a cópia."),

    (StorageLevel.OFF_HEAP,
     "OFF_HEAP",
     "Memória fora da JVM heap (requires spark.memory.offHeap.enabled=true).",
     "Reduz GC pressure em workloads de alta frequência. Configuração extra necessária."),
]

print(f"\n{'Level':<25} {'Onde armazena':<45} {'Quando usar'}")
print("─" * 120)
for level, name, onde, quando in storage_levels:
    print(f"\n  {name:<23} {onde}")
    print(f"  {'→ Quando usar:':<23} {quando}")

# COMMAND ----------

# ── Usando persist() com StorageLevel específico ──────────────────────────

from pyspark.storage import StorageLevel

# Para datasets que cabem na memória — máxima velocidade
df_pequeno.persist(StorageLevel.MEMORY_ONLY)

# Para datasets médios — padrão seguro (equivale ao .cache())
df_medio.persist(StorageLevel.MEMORY_AND_DISK)

# Para datasets grandes com GC alto — serializado usa menos RAM
df_grande.persist(StorageLevel.MEMORY_AND_DISK_SER)

# Para datasets muito grandes — acesso ocasional
df_enorme.persist(StorageLevel.DISK_ONLY)

# Com replicação — fault-tolerance em jobs críticos
df_critico.persist(StorageLevel.MEMORY_AND_DISK_2)

# COMMAND ----------

# MAGIC %md
# ## 3. unpersist() — liberar recursos

# COMMAND ----------

# MAGIC %md
# ### Por que unpersist é tão importante quanto cache

# COMMAND ----------

# ── unpersist: libera memória imediatamente ───────────────────────────────

df_temporario = spark.read.table("prod.silver.pedidos").cache()
df_temporario.count()  # materializa

# ... usa o DataFrame várias vezes ...

# Libera a memória quando não precisar mais
df_temporario.unpersist()
# blocking=True: espera a remoção ser concluída antes de continuar
df_temporario.unpersist(blocking=True)

# COMMAND ----------

# ── O que acontece se você NÃO fizer unpersist ────────────────────────────
#
# O Spark usa um mecanismo de LRU (Least Recently Used) para gerenciar
# o cache quando a memória enche:
#
# 1. Novos dados precisam de espaço em cache
# 2. Spark despeja o bloco menos recentemente usado (LRU eviction)
# 3. Se o bloco despejado for necessário depois → recalculate do zero
#
# Sem unpersist, você pode:
# → Desperdiçar memória com dados que nunca mais serão usados
# → Forçar o Spark a despejar dados que você DE FATO precisa
# → Aumentar GC pressure e degradar performance geral

# COMMAND ----------

# ── Padrão seguro com try/finally ────────────────────────────────────────

df_base = spark.read.table("prod.silver.pedidos").cache()
df_base.count()  # materializa

try:
    # Usa o cache para múltiplas operações
    resultado_a = df_base.groupBy("regiao").agg(spark_sum("valor").alias("total")).collect()
    resultado_b = df_base.filter(col("status") == "PAGO").count()
    resultado_c = df_base.agg(avg("valor")).collect()[0][0]

    print(f"Total por região: {resultado_a}")
    print(f"Pedidos pagos: {resultado_b}")
    print(f"Valor médio: {resultado_c}")

finally:
    df_base.unpersist()  # SEMPRE executado, mesmo se houver erro
    print("Cache liberado")

# COMMAND ----------

# MAGIC %md
# ## 4. Inspecionando o cache — Spark UI e API

# COMMAND ----------

# ── Ver DataFrames em cache via API ───────────────────────────────────────

# Verificar se um DataFrame específico está em cache
print(df_base.is_cached)         # True / False
print(df_base.storageLevel)      # StorageLevel atual

# COMMAND ----------

# ── Ver todos os RDDs/DataFrames em cache via SparkContext ────────────────

# Lista todos os RDDs persistidos atualmente
persistidos = spark.sparkContext._jsc.getPersistentRDDs()
print(f"DataFrames em cache: {len(persistidos)}")
for rdd_id, rdd in persistidos.items():
    print(f"  RDD ID: {rdd_id} | Name: {rdd.name()}")

# COMMAND ----------

# ── Spark UI — aba Storage ────────────────────────────────────────────────
#
# No Spark UI (porta 4040 ou aba "Spark UI" no Databricks):
# Aba Storage → mostra:
#   - Nome do RDD/DataFrame
#   - StorageLevel
#   - Fração em memória vs disco
#   - Tamanho total
#   - Número de partições cacheadas
#
# Sinais de problema na aba Storage:
#   → "Fraction Cached" < 100%: nem todas as partições foram armazenadas
#   → "Size in Memory" muito alto: pode estar causando GC excessivo
#   → "Size on Disk" alto com MEMORY_ONLY: dados estão sendo recalculados

# COMMAND ----------

# MAGIC %md
# ## 5. Delta Cache vs Spark Cache — diferença crítica no Databricks

# COMMAND ----------

# MAGIC %md
# ### Dois tipos de cache no Databricks

# COMMAND ----------

# ── Delta Cache (Databricks IO Cache) ────────────────────────────────────
#
# O Databricks tem um sistema de cache próprio ALÉM do Spark cache:
# o Delta Cache (anteriormente chamado IO Cache).
#
# Delta Cache:
# → Armazena dados lidos de object storage (S3, ADLS) no SSD local do executor
# → Transparente: acontece automaticamente sem nenhum código
# → Persiste entre jobs diferentes (diferente do Spark cache)
# → Disponível em node types com SSD (ex: cache-optimized instances)
# → Ativado por padrão em clusters com suporte
#
# Spark Cache (.cache() / .persist()):
# → Armazena DataFrame processado na JVM heap ou disco
# → Controlado pelo código — você decide o que cachear
# → Válido apenas durante o job atual
# → Sujeito a GC da JVM

# Configurações do Delta Cache
spark.conf.set("spark.databricks.io.cache.enabled",       "true")
spark.conf.set("spark.databricks.io.cache.maxDiskUsage",  "200g")
spark.conf.set("spark.databricks.io.cache.maxMetaDataCache", "1g")

# Verificar se Delta Cache está ativo
print(spark.conf.get("spark.databricks.io.cache.enabled"))

# COMMAND ----------

# ── Quando usar cada cache ────────────────────────────────────────────────

comparativo = [
    ("Caso de uso",
     "Delta Cache (automático)",
     "Spark Cache (.cache())"),

    ("Leitura repetida da MESMA tabela em múltiplos jobs",
     "✅ Ideal — persiste entre jobs",
     "❌ Não ajuda — expira com o job"),

    ("Reutilizar DataFrame transformado no MESMO job",
     "❌ Não cacheia transformações",
     "✅ Ideal — cacheia o resultado"),

    ("Tabela base lida muitas vezes em 1 notebook",
     "✅ Acontece automaticamente",
     "✅ Pode ajudar adicionalmente"),

    ("Resultado de join custoso reutilizado 3x",
     "❌ Join não é cacheado",
     "✅ Cacheia o resultado do join"),

    ("Configuração necessária",
     "Nenhuma (automático)",
     ".cache() ou .persist()"),
]

print(f"\n{'Caso de uso':<48} {'Delta Cache':<35} {'Spark Cache'}")
print("─" * 120)
for row in comparativo[1:]:
    print(f"  {row[0]:<46} {row[1]:<33} {row[2]}")

# COMMAND ----------

# MAGIC %md
# ## 6. Quando usar e quando NÃO usar cache

# COMMAND ----------

# ── Regra dos 2 usos ─────────────────────────────────────────────────────
#
# Cache só tem benefício se o DataFrame for LIDO pelo menos 2 vezes.
# Na primeira leitura, o cache tem custo (escrever em memória/disco).
# Na segunda e subsequentes, o cache tem benefício (ler de memória).
# Break-even: 2 usos. Benefício real: 3+ usos.

# COMMAND ----------

# ── ✅ QUANDO usar cache ──────────────────────────────────────────────────

# CASO 1: DataFrame base usado em múltiplas branches de análise
df_vendas_2024 = (
    spark.read.table("prod.silver.pedidos")
    .filter(col("ano") == 2024)
    .filter(col("status") == "PAGO")
    .join(spark.read.table("prod.silver.clientes"), "id_cliente", "left")
)
df_vendas_2024.cache()
df_vendas_2024.count()

# Agora usado em 5 análises diferentes
analise_regiao  = df_vendas_2024.groupBy("regiao").agg(spark_sum("valor")).collect()
analise_produto = df_vendas_2024.groupBy("produto").agg(count("*")).collect()
analise_mes     = df_vendas_2024.groupBy("mes").agg(avg("valor")).collect()
top_clientes    = df_vendas_2024.orderBy(col("valor").desc()).limit(100).collect()
total_geral     = df_vendas_2024.agg(spark_sum("valor")).collect()
df_vendas_2024.unpersist()

# COMMAND ----------

# CASO 2: Treino iterativo de ML (cada iteração relê os dados)
df_features = (
    spark.read.table("prod.gold.features_treino")
    .select("feature1", "feature2", "feature3", "label")
)
df_features.cache()
df_features.count()
# O algoritmo de ML vai iterar sobre os dados 50 vezes
# Sem cache: relê do disco 50 vezes
# Com cache: lê do disco 1 vez, das próximas 49 lê da RAM
# (Tipicamente, MLlib já gerencia cache internamente)
df_features.unpersist()

# COMMAND ----------

# CASO 3: DataFrame de referência (lookup) usado em múltiplos joins
df_cep_regiao = spark.read.table("prod.dim.cep_regiao")  # 500k linhas
df_cep_regiao.cache()
df_cep_regiao.count()

df_pedidos.join(df_cep_regiao, "cep", "left")
df_clientes.join(df_cep_regiao, "cep", "left")
df_enderecos.join(df_cep_regiao, "cep", "left")
df_cep_regiao.unpersist()

# COMMAND ----------

# ── ❌ QUANDO NÃO usar cache ──────────────────────────────────────────────

# CASO 1: DataFrame lido apenas uma vez
df_resultado = (
    spark.read.table("prod.silver.pedidos")
    .filter(col("status") == "PAGO")
    .groupBy("regiao")
    .agg(spark_sum("valor").alias("total"))
)
# Sem cache — lido apenas uma vez para escrever
df_resultado.write.format("delta").mode("overwrite").saveAsTable("prod.gold.vendas_regiao")
# ← NÃO cachear aqui: lê uma vez, escreve uma vez

# COMMAND ----------

# CASO 2: Pipeline linear (cada etapa usada uma só vez)
df_bronze = spark.read.table("prod.bronze.pedidos")    # sem cache
df_silver  = df_bronze.filter(col("valor") > 0)         # sem cache
df_gold    = df_silver.groupBy("regiao").agg(spark_sum("valor"))  # sem cache
df_gold.write.format("delta").mode("overwrite").saveAsTable("prod.gold.total_regiao")
# Pipeline linear: cada DF é usado exatamente uma vez → sem cache

# COMMAND ----------

# CASO 3: DataFrame muito grande que não cabe na memória
# e o StorageLevel seria DISK_ONLY de qualquer forma
# → O custo de serializar/desserializar para disco pode ser maior
#   que simplesmente reler o arquivo Parquet/Delta original

# CASO 4: DataFrame com query muito barata de recalcular
# → Um simples filter ou select em tabela Delta com Delta Cache ativo
#   pode ser mais rápido que o Spark cache pela latência da JVM

# COMMAND ----------

# MAGIC %md
# ## 7. Padrões avançados

# COMMAND ----------

# ── Checkpoint — alternativa ao cache para DAGs muito profundos ───────────
#
# Problema: após muitas transformações encadeadas, o DAG fica muito profundo.
# O plano de execução fica lento de construir e pode causar stack overflow.
#
# Solução: checkpoint() materializa o DataFrame e CORTA o lineage.
# Diferente do cache: checkpoint salva em disco e esquece a origem.

# Configurar diretório de checkpoint
spark.sparkContext.setCheckpointDir("/mnt/checkpoints/")

df_profundo = (
    spark.read.table("prod.silver.pedidos")
    .join(...)
    .join(...)
    .join(...)    # muitos joins encadeados
    # ... 20 transformações ...
)

# checkpoint() materializa no disco e quebra o lineage
df_checkpointed = df_profundo.checkpoint()

# A partir daqui, df_checkpointed não tem histórico de transformações
# Spark não precisa mais carregar o plano completo para cada Action

# COMMAND ----------

# ── localCheckpoint() — versão mais rápida (sem fault-tolerance) ──────────

# localCheckpoint: salva no disco LOCAL do executor (mais rápido)
# checkpoint:      salva no HDFS/cloud storage (mais seguro, mais lento)

df_local_cp = df_profundo.localCheckpoint()

# Use localCheckpoint quando:
# → O job pode ser reprocessado se falhar
# → Performance é mais importante que fault-tolerance

# COMMAND ----------

# MAGIC %md
# ## 8. Diagnóstico de problemas de cache

# COMMAND ----------

# ── Sintomas de cache mal usado ───────────────────────────────────────────

problemas = [
    ("GC Time alto (>10% no Spark UI)",
     "Muito dado em memória → aumentar executor memory ou mudar para MEMORY_AND_DISK_SER"),

    ("Spill to disk frequente",
     "Cache está consumindo memória que o Execution Memory precisaria → unpersist dados não usados"),

    ("Fraction Cached < 100% na aba Storage",
     "DataFrame não coube todo em cache → StorageLevel com DISK ou reduzir o que é cacheado"),

    ("Stage inesperadamente lento mesmo com cache",
     "Cache foi evicted (LRU) → muito dado em cache → unpersist o que não precisa"),

    ("OutOfMemoryError no executor",
     "Cache + Execution Memory excederam o heap → reduzir dados cacheados ou aumentar memória"),
]

print("Sintomas e soluções de problemas com cache:")
for sintoma, solucao in problemas:
    print(f"\n  Sintoma: {sintoma}")
    print(f"  Solução: {solucao}")

# COMMAND ----------

# MAGIC %md
# ## 9. Referência rápida

# COMMAND ----------

# ── Tabela de decisão: qual StorageLevel usar ─────────────────────────────

decisao = [
    ("DataFrame pequeno, sempre cabe na RAM",         "MEMORY_ONLY",          "Mais rápido para leitura"),
    ("Dataset médio, comportamento padrão seguro",    "MEMORY_AND_DISK",      "Equivale ao .cache()"),
    ("GC alto, precisa compactar em memória",         "MEMORY_ONLY_SER",      "Menor footprint na JVM"),
    ("Grande, GC alto, com fallback para disco",      "MEMORY_AND_DISK_SER",  "Balanceado"),
    ("Muito grande, acesso ocasional",                "DISK_ONLY",            "Sem pressão na heap"),
    ("Job crítico, não pode perder cache por falha",  "MEMORY_AND_DISK_2",    "Com replicação"),
    ("Alta frequência, fora da JVM heap",             "OFF_HEAP",             "Requer configuração extra"),
]

print(f"\n{'Situação':<48} {'StorageLevel':<25} {'Motivo'}")
print("─" * 100)
for sit, level, motivo in decisao:
    print(f"  {sit:<46} {level:<23} {motivo}")

# COMMAND ----------

# ── Comandos de referência rápida ────────────────────────────────────────

print("""
CACHE E PERSIST — COMANDOS ESSENCIAIS
──────────────────────────────────────────────────────

df.cache()                              → persist com MEMORY_AND_DISK
df.persist()                            → idem (sem argumento)
df.persist(StorageLevel.MEMORY_ONLY)    → persist com level específico
df.unpersist()                          → libera cache (async)
df.unpersist(blocking=True)             → libera cache (sync)
df.is_cached                            → True/False
df.storageLevel                         → StorageLevel atual

spark.sparkContext.setCheckpointDir("/caminho/")
df.checkpoint()                         → materializa e corta lineage (cloud)
df.localCheckpoint()                    → materializa local (mais rápido)
""")

# COMMAND ----------

# MAGIC %md
# ## Resumo — o que fixar deste arquivo
#
# | Conceito | O que saber |
# |----------|-------------|
# | Regra dos 2 usos | Cache só tem benefício se o DataFrame for lido 2+ vezes no mesmo job |
# | cache() é lazy | Materializa na primeira Action — não imediatamente |
# | cache() vs persist() | `cache()` = `persist(MEMORY_AND_DISK)` em PySpark |
# | MEMORY_ONLY | Mais rápido, mas descarta se não couber — recalcula |
# | MEMORY_AND_DISK | Seguro: overflow vai para disco local |
# | unpersist() | Obrigatório — use try/finally para garantir liberação |
# | Delta Cache | Automático no Databricks — cacheia I/O de object storage no SSD |
# | Spark Cache | Manual — cacheia DataFrame transformado na JVM |
# | checkpoint() | Materializa e corta lineage — para DAGs muito profundos |
# | Sintoma de abuso | GC Time alto, spill frequente, OOM — reduzir o que é cacheado |
#
# ### Conexão com a certificação Associate
# - A prova testa: quando usar vs quando não usar cache, diferença entre
#   `cache()` e `persist()`, e o que acontece com `MEMORY_ONLY` quando
#   os dados não cabem na memória (descarta, não lança erro)
#
# ### Próximo arquivo
# `10_broadcast_accumulator.py` — Broadcast variables para lookup eficiente
# e Accumulators para contadores distribuídos.
