# Databricks notebook source

# MAGIC %md
# # 03 — Catalyst Optimizer: Analysis → Logical Opt → Physical Plan → Code Gen
#
# **Analogia:**
# Imagine que você entrega uma receita escrita à mão para um chef experiente.
# Primeiro ele **lê e interpreta** o que você quis dizer (Analysis).
# Depois **reorganiza os passos** para ser mais eficiente — "vou cortar os legumes antes de
# ferver a água" (Logical Optimization).
# Então decide **como exatamente executar** cada etapa com o equipamento disponível
# (Physical Planning).
# Por fim, em vez de seguir a receita passo a passo, ele **memoriza tudo e executa em
# fluxo contínuo** sem parar a cada instrução (Code Generation).
#
# **Conceito técnico:**
# O **Catalyst** é o otimizador de queries do Spark SQL/DataFrame API.
# Ele transforma seu código em um plano de execução otimizado em 4 fases:
# 1. **Analysis** → resolve nomes, tipos e referências usando o Catalog
# 2. **Logical Optimization** → aplica regras algébricas (predicate pushdown, column pruning, etc.)
# 3. **Physical Planning** → gera planos físicos alternativos e escolhe o melhor (CBO)
# 4. **Code Generation** → Tungsten compila o plano final em bytecode JVM otimizado
#
# **Quando usar este conhecimento:**
# - Ao ler `EXPLAIN` e `EXPLAIN FORMATTED` para diagnosticar queries lentas
# - Ao entender por que o Spark "reordena" suas transformações automaticamente
# - Para saber onde intervenções manuais (hints, configs) realmente funcionam
# - Entrevistas sênior e prova Databricks Professional

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, upper, length, when, broadcast
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# ## Visão geral das 4 fases do Catalyst
#
# ```
#  Código Python/SQL
#       │
#       ▼
# ┌─────────────────────────────────────────────────────────────────────┐
# │  FASE 1 — ANALYSIS                                                   │
# │  Unresolved Logical Plan → Resolved Logical Plan                     │
# │  · Resolve nomes de colunas e tabelas no Catalog                     │
# │  · Verifica tipos e aplica coerção (type coercion)                   │
# │  · Lança AnalysisException se algo não existir                       │
# └───────────────────────────┬─────────────────────────────────────────┘
#                             │
#                             ▼
# ┌─────────────────────────────────────────────────────────────────────┐
# │  FASE 2 — LOGICAL OPTIMIZATION                                       │
# │  Resolved Logical Plan → Optimized Logical Plan                      │
# │  · Predicate Pushdown (filtros descem para a fonte)                  │
# │  · Column Pruning (remove colunas desnecessárias)                    │
# │  · Constant Folding (1 + 1 → 2 em tempo de compilação)              │
# │  · Boolean Simplification, Null Propagation, Join Reordering...      │
# └───────────────────────────┬─────────────────────────────────────────┘
#                             │
#                             ▼
# ┌─────────────────────────────────────────────────────────────────────┐
# │  FASE 3 — PHYSICAL PLANNING                                          │
# │  Optimized Logical Plan → Physical Plan (selecionado)                │
# │  · Gera múltiplos Physical Plans candidatos                          │
# │  · Cost-Based Optimizer (CBO) escolhe o de menor custo estimado      │
# │  · Define estratégia de join: BHJ, SMJ, SHJ                          │
# │  · Define estratégia de agregação: HashAggregate, SortAggregate      │
# └───────────────────────────┬─────────────────────────────────────────┘
#                             │
#                             ▼
# ┌─────────────────────────────────────────────────────────────────────┐
# │  FASE 4 — CODE GENERATION (Tungsten)                                 │
# │  Physical Plan → Bytecode JVM                                        │
# │  · Whole-Stage CodeGen: colapsa múltiplos operadores em 1 loop       │
# │  · Vectorized Reader: lê Parquet/Delta em batches (Arrow/columnar)   │
# │  · Off-heap memory para evitar GC overhead                           │
# └─────────────────────────────────────────────────────────────────────┘
# ```

# COMMAND ----------

# MAGIC %md
# ## Fase 1 — Analysis

# COMMAND ----------

# MAGIC %md
# O Analyzer resolve o **Unresolved Logical Plan** usando o **Catalog** (Unity Catalog ou Hive).
#
# O que ele faz:
# - Resolve `UnresolvedRelation` → tabela real com schema
# - Resolve `UnresolvedAttribute` → coluna tipada real
# - Aplica **type coercion**: 1 (Int) + 1.5 (Double) → promovido para Double
# - Expande `SELECT *` → lista explícita de colunas
# - Substitui funções pelo objeto interno correto
#
# Se algo falhar aqui → `AnalysisException` é lançada ANTES de qualquer execução.

# COMMAND ----------

# Exemplos de erros capturados na fase de Analysis:

# 1. Coluna inexistente → AnalysisException na hora do .explain() ou action
df_teste = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "nome"])

try:
    df_teste.select(col("coluna_que_nao_existe")).explain()
except Exception as e:
    print(f"[AnalysisException] {type(e).__name__}: {str(e)[:120]}")

# COMMAND ----------

# 2. Type coercion automática — Catalyst promove tipos silenciosamente
df_tipos = spark.createDataFrame([(1, 2.5), (3, 4.0)], ["inteiro", "double"])

# inteiro (Int) + double (Double) → Catalyst promove Int para Double automaticamente
df_coercao = df_tipos.withColumn("soma", col("inteiro") + col("double"))
df_coercao.printSchema()
# soma: double (coerção automática Int → Double)

# COMMAND ----------

# 3. Ver o plano ANTES de qualquer otimização (plano não resolvido não é visível diretamente,
#    mas podemos ver o Analyzed Plan via explain extended)
df_analise = df_teste.filter(col("id") > 1).select("nome")
df_analise.explain(mode="extended")
# No output você verá:
# == Parsed Logical Plan ==      ← antes do Analysis
# == Analyzed Logical Plan ==    ← depois do Analysis (tipos resolvidos)
# == Optimized Logical Plan ==   ← depois da Logical Optimization
# == Physical Plan ==            ← plano final

# COMMAND ----------

# MAGIC %md
# ## Fase 2 — Logical Optimization

# COMMAND ----------

# MAGIC %md
# O Optimizer aplica **regras de transformação** ao plano lógico.
# Cada regra é aplicada repetidamente até que nenhuma mais se aplique (ponto fixo).
#
# São mais de 100 regras — as mais importantes para o dia a dia:

# COMMAND ----------

# MAGIC %md
# ### 2a. Predicate Pushdown
#
# Filtros são "empurrados" para baixo no plano — o mais próximo possível da fonte.
# Resultado: menos dados lidos desde o início.

# COMMAND ----------

# Exemplo: o filtro parece estar "após" o join, mas o Catalyst o move para antes
df_pedidos = spark.createDataFrame(
    [(1, 101, 500.0), (2, 102, 300.0), (3, 101, 200.0), (4, 103, 800.0)],
    ["pedido_id", "cliente_id", "valor"]
)
df_clientes = spark.createDataFrame(
    [(101, "Ana", "SP"), (102, "Bruno", "RJ"), (103, "Carla", "MG")],
    ["cliente_id", "nome", "estado"]
)

# Você escreve: join DEPOIS filtra por estado
resultado = (
    df_pedidos
    .join(df_clientes, on="cliente_id")
    .filter(col("estado") == "SP")      # parece que filtra após o join
    .filter(col("valor") > 100)         # parece que filtra após o join
)

print("=== Com Predicate Pushdown (ver Optimized Logical Plan) ===")
resultado.explain(mode="extended")
# No Optimized Logical Plan você verá os filtros ANTES do join — Catalyst moveu eles

# COMMAND ----------

# MAGIC %md
# ### 2b. Column Pruning (Project Pushdown)
#
# O Catalyst remove colunas que não são usadas no resultado final,
# empurrando o `SELECT` para baixo — menos dados carregados na memória e menos I/O.

# COMMAND ----------

df_wide = spark.createDataFrame(
    [(1, "Ana", "SP", 30, "email@a.com", "555-1234", "Rua A", "Brasil")],
    ["id", "nome", "estado", "idade", "email", "telefone", "endereco", "pais"]
)

# Você usa apenas id e nome no final — Catalyst descarta as outras 6 colunas desde a leitura
resultado_pruned = df_wide.select("id", "nome").filter(col("id") > 0)

print("=== Column Pruning: apenas id e nome são carregadas ===")
resultado_pruned.explain(mode="formatted")
# No Physical Plan você verá: ReadSchema: struct<id:bigint,nome:string>
# As outras colunas não aparecem — o leitor Parquet/Delta nem as lê do disco

# COMMAND ----------

# MAGIC %md
# ### 2c. Constant Folding e Boolean Simplification

# COMMAND ----------

# Constant Folding: expressões com literais são avaliadas em tempo de compilação
df_base = spark.range(10)

# Você escreve: col + (2 * 3 + 4)
# Catalyst avalia (2 * 3 + 4) = 10 em tempo de compilação e usa literalmente 10
df_fold = df_base.withColumn("calc", col("id") + lit(2 * 3 + 4))

# Boolean simplification: NOT (NOT x) → x, TRUE AND x → x, FALSE OR x → x
df_bool = df_base.filter(col("id") > 0)
# Internamente: filter(NOT(NOT(id > 0))) → simplificado para filter(id > 0)

# Null propagation: NULL + qualquer coisa → NULL (sem precisar checar em runtime)

print("=== Constant Folding no Physical Plan ===")
df_fold.explain(mode="simple")

# COMMAND ----------

# MAGIC %md
# ### 2d. Join Reordering (com CBO ativado)
#
# Quando você junta múltiplas tabelas, o Catalyst pode reordenar os joins
# para minimizar o volume de dados intermediários.
# Requer **estatísticas de tabela** coletadas via `ANALYZE TABLE`.

# COMMAND ----------

# Para ativar o CBO e join reordering:
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")

# Para coletar estatísticas que o CBO usa:
# ANALYZE TABLE catalog.schema.tabela COMPUTE STATISTICS FOR ALL COLUMNS
# No Databricks com Delta, estatísticas básicas são coletadas automaticamente

print("CBO ativado:", spark.conf.get("spark.sql.cbo.enabled"))
print("Join reorder:", spark.conf.get("spark.sql.cbo.joinReorder.enabled"))

# COMMAND ----------

# MAGIC %md
# ## Fase 3 — Physical Planning

# COMMAND ----------

# MAGIC %md
# O **Physical Planner** converte o Optimized Logical Plan em um ou mais **Physical Plans**.
# Para cada operação lógica, existem múltiplas implementações físicas possíveis.
# O **Cost-Based Optimizer (CBO)** seleciona o plano de menor custo estimado.
#
# Exemplos de escolhas físicas:
# - Join lógico → BHJ ou SMJ ou SHJ (ver `04_physical_plan_joins.py`)
# - Aggregation → HashAggregate (2 fases: parcial + final) ou SortAggregate
# - Sort → TimSort in-memory ou external sort (com spill)

# COMMAND ----------

# Ver o Physical Plan diretamente
df_agg = (
    df_pedidos
    .groupBy("cliente_id")
    .agg({"valor": "sum"})
)

print("=== Physical Plan — Aggregation em 2 fases ===")
df_agg.explain(mode="formatted")
# Você verá:
# HashAggregate (partial) — roda em cada Executor antes do shuffle
# Exchange (shuffle por cliente_id)
# HashAggregate (final) — consolida os resultados parciais

# COMMAND ----------

# MAGIC %md
# ### Lendo o Physical Plan: de baixo para cima
#
# **CRÍTICO:** O Physical Plan é lido de baixo para cima — o operador na base é executado primeiro.
#
# ```
# == Physical Plan ==
#
# AdaptiveSparkPlan (6)                   ← AQE gerenciando o plano
#  +- == Final Plan ==
#     HashAggregate (5)  [final]          ← 5. agrega resultados parciais
#        +- Exchange (4)                  ← 4. shuffle por cliente_id
#           +- HashAggregate (3) [parcial]← 3. agrega dentro de cada partição
#              +- Filter (2)              ← 2. aplica filtro (pushdown!)
#                 +- Scan (1)            ← 1. lê os dados (primeiro a executar)
#
# Leitura: 1 → 2 → 3 → 4 → 5
# ```

# COMMAND ----------

# MAGIC %md
# ### Nós importantes do Physical Plan
#
# | Nó | Significado |
# |---|---|
# | `Scan` / `FileScan` | Leitura de dados (arquivo, Delta, JDBC) |
# | `Filter` | Predicado pushdown aplicado na leitura |
# | `Project` | Seleção de colunas (column pruning) |
# | `Exchange` | **Shuffle** — redistribuição de dados pela rede |
# | `Sort` | Ordenação (geralmente após Exchange) |
# | `HashAggregate` | Agregação via hash table (parcial + final) |
# | `BroadcastExchange` | Envio de tabela pequena para todos os Executors |
# | `BroadcastHashJoin` | Join usando tabela broadcast (sem shuffle!) |
# | `SortMergeJoin` | Join via sort + merge (dois shuffles) |
# | `ShuffledHashJoin` | Join via hash table após shuffle |
# | `WholeStageCodegen` | Múltiplos operadores compilados em 1 função JVM |
# | `*(n)` | Asterisco e número = dentro de um WholeStageCodegen |
# | `AdaptiveSparkPlan` | AQE gerenciando o plano dinamicamente |

# COMMAND ----------

# MAGIC %md
# ## Fase 4 — Code Generation (Tungsten)

# COMMAND ----------

# MAGIC %md
# ### Whole-Stage Code Generation
#
# Em vez de cada operador chamar o próximo via interface (overhead de polimorfismo JVM),
# o Tungsten **compila múltiplos operadores em uma única função Java** — eliminando o overhead
# de chamadas de método, alocações intermediárias e desserializações desnecessárias.
#
# ```
# Sem WholeStageCodegen (interpretado):
#   Scan → [desserializa] → Filter → [serializa] → Project → [serializa] → Agg
#   Cada seta = chamada de método + alocação de objeto Row = overhead enorme
#
# Com WholeStageCodegen (compilado):
#   [Scan + Filter + Project + Agg]  ← tudo em 1 loop gerado em bytecode JVM
#   Sem alocações intermediárias, sem chamadas de interface — CPU cache-friendly
# ```
#
# Você identifica WholeStageCodegen no Physical Plan pelo prefixo `*(n)`:
# `*(1) Filter`, `*(1) HashAggregate` — o número indica o "stage" de codegen.

# COMMAND ----------

# Para verificar se WholeStageCodegen está ativo:
print("WholeStageCodegen:", spark.conf.get("spark.sql.codegen.wholeStage"))
# Default: true — nunca desative em produção

# Para ver o código Java gerado (debugging avançado):
spark.conf.set("spark.sql.codegen.comments", "true")  # adiciona comentários no bytecode

df_codegen = df_pedidos.filter(col("valor") > 200).select("pedido_id", "valor")
print("=== Physical Plan com WholeStageCodegen ===")
df_codegen.explain(mode="formatted")
# Procure por *(1) no plano — tudo dentro do mesmo *(1) é um único loop compilado

# COMMAND ----------

# MAGIC %md
# ### Vectorized (Columnar) Reader
#
# O Tungsten também usa leitura vetorizada para Parquet e Delta:
# em vez de ler linha por linha (Row format), lê **colunas inteiras em batches** (Arrow/columnar).
# Isso permite uso de instruções SIMD do CPU — ordens de magnitude mais rápido para scans.

# COMMAND ----------

# Verificar se o Vectorized Reader está ativo:
print("Vectorized Parquet Reader:", spark.conf.get("spark.sql.parquet.enableVectorizedReader"))
print("Vectorized ORC Reader:", spark.conf.get("spark.sql.orc.enableVectorizedReader"))

# Para UDFs Python: o Vectorized Reader é desativado no estágio da UDF
# (Python não consegue ler formato colunar diretamente sem Arrow)
# Por isso Pandas UDFs (com Arrow) são muito mais eficientes que UDFs Python puras

# COMMAND ----------

# MAGIC %md
# ## Como usar EXPLAIN na prática

# COMMAND ----------

# MAGIC %md
# ### Os 5 modos de EXPLAIN

# COMMAND ----------

df_exemplo = (
    df_pedidos
    .join(df_clientes, on="cliente_id")
    .filter(col("valor") > 200)
    .groupBy("estado")
    .agg({"valor": "sum"})
)

# COMMAND ----------

# Modo 1: simple — só o Physical Plan (mais usado no dia a dia)
print("=" * 60, "SIMPLE")
df_exemplo.explain(mode="simple")

# COMMAND ----------

# Modo 2: extended — todos os 4 planos (parsed, analyzed, optimized, physical)
print("=" * 60, "EXTENDED")
df_exemplo.explain(mode="extended")

# COMMAND ----------

# Modo 3: codegen — mostra o código Java gerado pelo Tungsten
print("=" * 60, "CODEGEN")
df_exemplo.explain(mode="codegen")

# COMMAND ----------

# Modo 4: cost — Physical Plan com estimativas de custo do CBO
print("=" * 60, "COST")
df_exemplo.explain(mode="cost")

# COMMAND ----------

# Modo 5: formatted — Physical Plan formatado com métricas por nó (melhor para diagnóstico)
print("=" * 60, "FORMATTED")
df_exemplo.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# ### Diagnóstico sistemático com EXPLAIN
#
# Checklist ao analisar um Physical Plan lento:
#
# 1. **Procure `Exchange` desnecessários** → shuffles extras = gargalo de rede
# 2. **Verifique os `Filter` e `Scan`** → filtros estão sendo feitos no Scan? (pushdown OK)
# 3. **Confirme o tipo de join** → BHJ (bom) vs SMJ (shuffle) vs SHJ
# 4. **Procure `*(n)` agrupamentos** → mais operadores juntos = melhor codegen
# 5. **Veja se AQE está ativo** → `AdaptiveSparkPlan` no topo do plano
# 6. **Cheque `Sort` desnecessários** → `orderBy` global antes de `write` é quase sempre desnecessário

# COMMAND ----------

# MAGIC %md
# ## AQE — Adaptive Query Execution (extensão do Catalyst)

# COMMAND ----------

# MAGIC %md
# O AQE é uma extensão do Physical Planning que **reotimiza o plano em runtime**,
# usando estatísticas reais coletadas durante a execução (não estimativas).
#
# 3 otimizações principais do AQE:
#
# 1. **Coalescing shuffle partitions** → reduz partições pós-shuffle automaticamente
#    (ex: 200 partições → 8 reais se os dados forem pequenos)
#
# 2. **Switching join strategies** → converte SMJ → BHJ em runtime se percebe que
#    um lado é menor do que parecia antes do shuffle
#
# 3. **Skew join optimization** → detecta partições com skew e as divide automaticamente

# COMMAND ----------

# Verificar configurações do AQE:
aqe_configs = {
    "spark.sql.adaptive.enabled": "AQE ativado",
    "spark.sql.adaptive.coalescePartitions.enabled": "Coalesce automático",
    "spark.sql.adaptive.coalescePartitions.minPartitionSize": "Tamanho mínimo por partição",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "Tamanho alvo após coalesce",
    "spark.sql.adaptive.skewJoin.enabled": "Otimização de skew",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "Fator de skew (2x = skew detectado)",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "Threshold de bytes para skew",
}

print(f"\n{'Configuração':<55} {'Valor'}")
print("=" * 80)
for config, descricao in aqe_configs.items():
    try:
        valor = spark.conf.get(config)
    except Exception:
        valor = "(não definido)"
    print(f"{config:<55} {valor}")

# COMMAND ----------

# MAGIC %md
# ## ⚠️ Resumo de armadilhas e pontos de prova
#
# | Tema | O que saber |
# |---|---|
# | `AnalysisException` | Disparada na Fase 1 — antes de qualquer execução |
# | Predicate Pushdown | Filtros são movidos para baixo automaticamente — não se preocupe com a ordem |
# | Column Pruning | `SELECT *` lê todas as colunas — prefira selecionar apenas o necessário |
# | EXPLAIN lido de baixo | O nó na base do plano é executado primeiro |
# | `*(n)` no plano | Indica WholeStageCodegen — mais operadores juntos = mais eficiente |
# | `Exchange` = shuffle | Cada `Exchange` no plano = transferência de rede — minimize |
# | CBO vs RBO | RBO (Rule-Based) sempre ativo. CBO (custo) precisa de `ANALYZE TABLE` |
# | AQE | Reotimiza em runtime — mantido ativado por padrão no Databricks |
# | UDFs quebram otimização | UDFs Python são caixas-pretas para o Catalyst — use funções built-in |
# | `codegen.wholeStage` | Nunca desative — é a base do desempenho do Spark SQL |

# COMMAND ----------
