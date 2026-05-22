# Databricks notebook source

# MAGIC %md
# # 06 — RDD, DataFrame e Dataset: Comparação de APIs e Interoperabilidade
#
# **Analogia:**
# Imagine três formas de trabalhar com uma planilha de dados:
#
# **RDD** é como trabalhar com um arquivo de texto bruto linha por linha — você tem controle
# absoluto sobre cada caractere, mas precisa fazer tudo manualmente: parsear, validar, calcular.
# O computador não sabe o que é cada coluna.
#
# **DataFrame** é como trabalhar com uma planilha Excel com cabeçalhos — você faz `filtrar por
# estado = SP` e o Excel sabe o que fazer. Ele ainda pode otimizar (calcular apenas as colunas
# que você precisa, etc.) porque entende a estrutura.
#
# **Dataset** é como uma planilha Excel com uma classe Java mapeada — cada linha é um objeto
# `Venda(id, cliente, valor)` tipado em tempo de compilação. Você tem a estrutura do DataFrame
# mais a segurança de tipo do compilador.
#
# **Conceito técnico:**
# - **RDD (Resilient Distributed Dataset):** API de baixo nível. Coleção distribuída de objetos
#   JVM opacos. Sem schema, sem otimizador (Catalyst), sem Tungsten para operações arbitrárias.
#   Máximo controle, mínima otimização automática.
# - **DataFrame:** API de alto nível. RDD de `Row` com schema definido. Otimizado pelo Catalyst
#   e Tungsten. Representação interna = UnsafeRow. A API principal do Spark moderno.
# - **Dataset[T]:** API fortemente tipada (Scala/Java). `Dataset[Row]` = DataFrame.
#   Em PySpark não existe Dataset separado — DataFrame IS Dataset[Row].
#
# **Quando usar este conhecimento:**
# - Para justificar por que você usa DataFrame/SQL em vez de RDD
# - Para entender quando RDD ainda é necessário (operações não suportadas em DataFrame)
# - Para diagnosticar overhead de conversões (toDF, rdd, collect)
# - Entrevistas sênior e prova Databricks

# COMMAND ----------

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, sum as spark_sum, count, udf
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, DoubleType, IntegerType
)
import time

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# COMMAND ----------

# MAGIC %md
# ## 1. RDD — Resilient Distributed Dataset

# COMMAND ----------

# MAGIC %md
# ### Características do RDD
#
# ```
# RDD[T]
# ┌─────────────────────────────────────────────────────────────┐
# │  · Coleção distribuída de objetos JVM de tipo T             │
# │  · T pode ser qualquer coisa: String, Tuple, seu objeto     │
# │  · O Spark não sabe o que está dentro — opaco              │
# │  · Sem Catalyst: otimizações manuais                        │
# │  · Sem Tungsten: objetos Java no heap → GC pressure         │
# │  · Com tolerância a falhas: lineage para recomputar         │
# │  · Lazy evaluation: transformações constroem o grafo        │
# └─────────────────────────────────────────────────────────────┘
#
# Operações RDD:
# · Transformações (lazy): map, flatMap, filter, reduceByKey,
#   groupByKey, join, union, distinct, sortBy, mapPartitions
# · Actions (executam): collect, count, take, reduce, saveAsTextFile,
#   foreach, first
# ```

# COMMAND ----------

# Criando e manipulando um RDD
dados_brutos = [
    "1,Ana,SP,1500.0",
    "2,Bruno,RJ,2300.0",
    "3,Carla,SP,800.0",
    "4,Diana,MG,3100.0",
    "5,Eduardo,SP,950.0",
]

rdd_raw = sc.parallelize(dados_brutos, numSlices=2)

# Transformações encadeadas (lazy — nada executa ainda)
rdd_parsed = rdd_raw.map(lambda linha: linha.split(","))
rdd_sp = rdd_parsed.filter(lambda campos: campos[2] == "SP")
rdd_tupla = rdd_sp.map(lambda campos: (campos[1], float(campos[3])))
rdd_soma = rdd_tupla.reduceByKey(lambda a, b: a + b)

# Action — executa o grafo completo
resultado_rdd = rdd_soma.collect()
print("Resultado via RDD:", resultado_rdd)

# COMMAND ----------

# Operações especiais que só existem em RDD
rdd_numeros = sc.parallelize(range(1, 101))

# mapPartitions: processa uma partição inteira de uma vez (mais eficiente que map para I/O)
def processar_particao(iterator):
    # Conexão com banco seria aberta UMA vez por partição, não uma por linha
    return (x * 2 for x in iterator if x % 2 == 0)

rdd_processado = rdd_numeros.mapPartitions(processar_particao)
print("mapPartitions — pares dobrados (primeiros 10):", rdd_processado.take(10))

# COMMAND ----------

# Acumuladores e Broadcast Variables (nível RDD — também funcionam em DataFrame via sc)
contador_impares = sc.accumulator(0)

def contar_impares(x):
    global contador_impares
    if x % 2 != 0:
        contador_impares += 1
    return x

rdd_numeros.foreach(contar_impares)
print(f"Total de ímpares (via Accumulator): {contador_impares.value}")

# COMMAND ----------

# MAGIC %md
# ## 2. DataFrame — A API Principal do Spark Moderno

# COMMAND ----------

# MAGIC %md
# ### Por que DataFrame domina sobre RDD
#
# ```
# Pipeline idêntico — RDD vs DataFrame:
#
# RDD:
#   rdd.map(parse).filter(f).groupByKey().mapValues(sum).collect()
#   └── sem Catalyst, sem Tungsten, objetos Java, GC constante
#
# DataFrame:
#   df.filter(...).groupBy(...).agg(sum(...)).collect()
#   └── Catalyst otimiza o plano
#   └── Tungsten executa em UnsafeRow + WSCG
#   └── Vectorized reader para Parquet/Delta
#   └── Predicate pushdown, column pruning, join reordering
#
# Resultado: DataFrame é 5-100x mais rápido em operações típicas
# ```

# COMMAND ----------

# Criando DataFrame com schema explícito (sempre preferível para produção)
schema = StructType([
    StructField("id",     LongType(),   nullable=False),
    StructField("nome",   StringType(), nullable=True),
    StructField("estado", StringType(), nullable=True),
    StructField("valor",  DoubleType(), nullable=True),
])

dados = [(1, "Ana", "SP", 1500.0), (2, "Bruno", "RJ", 2300.0),
         (3, "Carla", "SP", 800.0), (4, "Diana", "MG", 3100.0),
         (5, "Eduardo", "SP", 950.0)]

df = spark.createDataFrame(dados, schema=schema)
df.printSchema()
df.show()

# COMMAND ----------

# DataFrame — mesma lógica do RDD, mas otimizada
resultado_df = (
    df
    .filter(col("estado") == "SP")
    .groupBy("estado")
    .agg(spark_sum("valor").alias("total_valor"))
)

resultado_df.show()
resultado_df.explain(mode="formatted")  # veja o plano otimizado

# COMMAND ----------

# MAGIC %md
# ### Schema inference vs Schema explícito
#
# | Abordagem | Quando usar | Risco |
# |---|---|---|
# | Schema explícito (`StructType`) | **Produção sempre** | Nenhum — você controla |
# | `inferSchema=True` | Exploração, notebooks ad-hoc | Lento (lê o arquivo duas vezes) + pode inferir tipo errado (ex: "001" → Int, perde o zero) |
# | `schema_of_json()` / `schema_of_csv()` | Geração programática de schema | Depende de amostra representativa |

# COMMAND ----------

# Leitura com schema explícito vs inferido — diferença de performance e corretude
schema_vendas = StructType([
    StructField("pedido_id", LongType(), False),
    StructField("cliente_id", LongType(), True),
    StructField("valor", DoubleType(), True),
    StructField("data", StringType(), True),
])

# Em produção: sempre use schema explícito
# df = spark.read.schema(schema_vendas).parquet("/caminho/para/dados")

# ⚠️ Nunca em produção:
# df = spark.read.option("inferSchema", "true").csv("/caminho")
# → 2 passes no arquivo + risco de tipo errado

print("Regra: schema explícito em TODA leitura de produção")

# COMMAND ----------

# MAGIC %md
# ## 3. Dataset[T] — API Tipada (Scala/Java) e sua relação com PySpark

# COMMAND ----------

# MAGIC %md
# ### Dataset em Scala/Java vs PySpark
#
# ```
# Scala/Java:
# ┌─────────────────────────────────────────────────────────────┐
# │  Dataset[T] — tipado em tempo de compilação                 │
# │                                                             │
# │  case class Venda(id: Long, cliente: String, valor: Double) │
# │  val ds: Dataset[Venda] = df.as[Venda]                     │
# │                                                             │
# │  ds.filter(_.valor > 1000)  // erro de compilação se errar  │
# │  ds.map(v => v.copy(valor = v.valor * 1.1)) // type-safe    │
# │                                                             │
# │  Dataset[Row] == DataFrame (alias)                          │
# └─────────────────────────────────────────────────────────────┘
#
# PySpark:
# ┌─────────────────────────────────────────────────────────────┐
# │  Dataset tipado NÃO EXISTE em Python                        │
# │  Python é dinamicamente tipado → não há verificação        │
# │  em tempo de compilação de qualquer forma                   │
# │                                                             │
# │  Em PySpark você trabalha SEMPRE com DataFrame              │
# │  (que internamente é Dataset[Row] na JVM)                   │
# │                                                             │
# │  Para tipagem em PySpark → use type hints + schema          │
# │  explícito + pydantic para validação                        │
# └─────────────────────────────────────────────────────────────┘
# ```

# COMMAND ----------

# Em PySpark: DataFrame é a API principal — sem Dataset separado
# Mas podemos obter Row objects tipados via Row namedtuple

from pyspark.sql import Row

# Row nomeado — similar a um namedtuple
VendaRow = Row("id", "nome", "estado", "valor")
linha = VendaRow(1, "Ana", "SP", 1500.0)
print(f"Row nomeado: id={linha.id}, nome={linha.nome}, valor={linha.valor}")

# Criar DataFrame via lista de Rows
dados_row = [
    VendaRow(1, "Ana", "SP", 1500.0),
    VendaRow(2, "Bruno", "RJ", 2300.0),
]
df_row = spark.createDataFrame(dados_row)
df_row.show()

# COMMAND ----------

# MAGIC %md
# ## 4. Comparativo completo: RDD × DataFrame × Dataset

# COMMAND ----------

# MAGIC %md
# ```
# ┌──────────────────────┬──────────────────┬──────────────────┬──────────────────┐
# │ Critério             │ RDD              │ DataFrame        │ Dataset[T]       │
# ├──────────────────────┼──────────────────┼──────────────────┼──────────────────┤
# │ Disponível em        │ Scala,Java,Python│ Scala,Java,Python│ Scala, Java      │
# │                      │ R                │ R, SQL           │ (não em Python)  │
# ├──────────────────────┼──────────────────┼──────────────────┼──────────────────┤
# │ Schema               │ Nenhum (opaco)   │ Schema fixo      │ Schema + tipo T  │
# ├──────────────────────┼──────────────────┼──────────────────┼──────────────────┤
# │ Tipo em compilação   │ Não              │ Não (Row é untyp)│ SIM              │
# ├──────────────────────┼──────────────────┼──────────────────┼──────────────────┤
# │ Catalyst Optimizer   │ NÃO              │ SIM              │ SIM (parcial*)   │
# ├──────────────────────┼──────────────────┼──────────────────┼──────────────────┤
# │ Tungsten / WSCG      │ NÃO (JVM objects)│ SIM (UnsafeRow) │ SIM (fase SQL)   │
# ├──────────────────────┼──────────────────┼──────────────────┼──────────────────┤
# │ Predicate pushdown   │ NÃO              │ SIM              │ SIM              │
# ├──────────────────────┼──────────────────┼──────────────────┼──────────────────┤
# │ Serialização shuffle │ Java serialization│ Kryo/UnsafeRow  │ Encoder custom   │
# ├──────────────────────┼──────────────────┼──────────────────┼──────────────────┤
# │ Performance típica   │ Linha de base    │ 5-100x mais rápid│ Similar ao DF    │
# ├──────────────────────┼──────────────────┼──────────────────┼──────────────────┤
# │ Quando usar          │ Ops não SQL,     │ 99% dos casos    │ APIs tipadas      │
# │                      │ ML pipelines,    │ ETL, SQL, análise│ Scala/Java       │
# │                      │ graph ops        │                  │                  │
# └──────────────────────┴──────────────────┴──────────────────┴──────────────────┘
# *Dataset.map() com lambda sai do Catalyst e usa encoders — similar à perda do WSCG
# ```

# COMMAND ----------

# MAGIC %md
# ## 5. Interoperabilidade — Convertendo entre APIs

# COMMAND ----------

# MAGIC %md
# ### DataFrame → RDD

# COMMAND ----------

# df.rdd retorna RDD[Row] — cada elemento é um objeto Row (não UnsafeRow)
# ⚠️ CUSTO: conversão de UnsafeRow → Row Java → overhead de serialização

df_sample = spark.createDataFrame(
    [(1, "Ana", 1500.0), (2, "Bruno", 2300.0), (3, "Carla", 800.0)],
    ["id", "nome", "valor"]
)

rdd_from_df = df_sample.rdd   # ← conversão lazy — não executa ainda
print("Tipo:", type(rdd_from_df))

# Processamento via RDD após conversão
resultado = rdd_from_df.map(lambda row: (row.nome, row.valor * 1.1)).collect()
print("Resultado:", resultado)

# COMMAND ----------

# MAGIC %md
# ### RDD → DataFrame

# COMMAND ----------

# Método 1: rdd.toDF() — inferência de schema a partir do tipo do RDD
rdd_tuplas = sc.parallelize([(1, "Ana", 1500.0), (2, "Bruno", 2300.0)])
df_from_rdd_infer = rdd_tuplas.toDF(["id", "nome", "valor"])
df_from_rdd_infer.printSchema()

# COMMAND ----------

# Método 2: spark.createDataFrame(rdd, schema) — sempre use schema explícito em produção
schema_explicit = StructType([
    StructField("id",    LongType(),   False),
    StructField("nome",  StringType(), True),
    StructField("valor", DoubleType(), True),
])

rdd_rows = sc.parallelize([Row(id=1, nome="Ana", valor=1500.0),
                            Row(id=2, nome="Bruno", valor=2300.0)])
df_from_rdd_typed = spark.createDataFrame(rdd_rows, schema=schema_explicit)
df_from_rdd_typed.printSchema()
df_from_rdd_typed.show()

# COMMAND ----------

# MAGIC %md
# ### DataFrame ↔ Pandas
#
# **Custo crítico:** `toPandas()` e `createDataFrame(pandas_df)` coletam/distribuem
# TODOS os dados pelo Driver. Só use para DataFrames pequenos.

# COMMAND ----------

# DataFrame → Pandas (coleta no Driver — cuidado com volume!)
df_para_pandas = df_sample.toPandas()
print("Tipo:", type(df_para_pandas))
print(df_para_pandas)

# COMMAND ----------

# Pandas → DataFrame (distribui do Driver para os Executors)
import pandas as pd

pandas_df = pd.DataFrame({
    "id": [1, 2, 3],
    "nome": ["Ana", "Bruno", "Carla"],
    "valor": [1500.0, 2300.0, 800.0]
})

# Sem schema (inferência)
df_do_pandas = spark.createDataFrame(pandas_df)
df_do_pandas.printSchema()

# Com schema explícito (preferível)
df_do_pandas_typed = spark.createDataFrame(pandas_df, schema=schema_explicit)
df_do_pandas_typed.show()

# COMMAND ----------

# MAGIC %md
# ### Temp Views — Ponte entre DataFrame e SQL

# COMMAND ----------

# createOrReplaceTempView: visível apenas nesta SparkSession
df_sample.createOrReplaceTempView("vendas_temp")

resultado_sql = spark.sql("""
    SELECT nome, valor * 1.1 AS valor_ajustado
    FROM vendas_temp
    WHERE valor > 1000
    ORDER BY valor_ajustado DESC
""")
resultado_sql.show()

# COMMAND ----------

# createOrReplaceGlobalTempView: visível em todas as SparkSessions do mesmo contexto
# Acessada com prefixo: global_temp.nome_da_view
df_sample.createOrReplaceGlobalTempView("vendas_global")

resultado_global = spark.sql("SELECT * FROM global_temp.vendas_global WHERE id > 1")
resultado_global.show()

# Retornar para DataFrame após SQL:
df_pos_sql = spark.sql("SELECT estado, SUM(valor) as total FROM vendas_temp GROUP BY estado")
# df_pos_sql é um DataFrame normal — pode continuar encadeando transformações
print(type(df_pos_sql))

# COMMAND ----------

# MAGIC %md
# ## 6. Quando ainda usar RDD em 2024+

# COMMAND ----------

# MAGIC %md
# ### Casos legítimos para RDD
#
# | Caso | Motivo | Exemplo |
# |---|---|---|
# | `mapPartitions` para I/O externo | Abre conexão 1× por partição | Gravar em MongoDB, enviar para API externa |
# | Operações arbitrárias sem equivalente SQL | Lógica complexa que não tem função nativa | Parsing de formato binário proprietário |
# | Interop com bibliotecas que exigem RDD | MLlib antigo, GraphX | Alguns algoritmos de Graph |
# | Controle fino de particionamento | `partitionBy` com `Partitioner` custom | Hash partitioner por chave de negócio |
# | Acumuladores e Broadcast com lógica custom | Contadores distribuídos, lookup tables grandes | Auditoria de registros processados |

# COMMAND ----------

# Exemplo real: mapPartitions para conexão eficiente com sistema externo
def escrever_em_banco_externo(partition_iterator):
    """
    Abre conexão 1 vez por partição (não 1 vez por linha).
    Em um DataFrame normal com UDF, a conexão seria aberta e fechada por linha.
    """
    registros_processados = []
    # conexao = MinhaDBConexao.connect(host, port)  # abre 1 vez
    for row in partition_iterator:
        # conexao.insert(row)  # usa a mesma conexão
        registros_processados.append(f"processado: {row}")
    # conexao.close()  # fecha 1 vez
    return iter(registros_processados)

# rdd_dados.mapPartitions(escrever_em_banco_externo).count()
print("mapPartitions: padrão ideal para I/O externo em processamento distribuído")

# COMMAND ----------

# MAGIC %md
# ## 7. Benchmark rápido: RDD vs DataFrame

# COMMAND ----------

n = 5_000_000
rdd_bench = sc.parallelize(range(n))
df_bench = spark.range(n)

# Operação equivalente: filtrar pares e somar
# RDD
start = time.time()
soma_rdd = rdd_bench.filter(lambda x: x % 2 == 0).reduce(lambda a, b: a + b)
tempo_rdd = time.time() - start
print(f"RDD  — Soma pares: {soma_rdd:,} | Tempo: {tempo_rdd:.2f}s")

# DataFrame
start = time.time()
soma_df = df_bench.filter(col("id") % 2 == 0).agg(spark_sum("id")).collect()[0][0]
tempo_df = time.time() - start
print(f"DataFrame — Soma pares: {soma_df:,} | Tempo: {tempo_df:.2f}s")

if tempo_rdd > 0 and tempo_df > 0:
    print(f"DataFrame foi {tempo_rdd/tempo_df:.1f}x mais rápido")

# COMMAND ----------

# MAGIC %md
# ## ⚠️ Resumo de armadilhas e pontos de prova
#
# | Tema | O que saber |
# |---|---|
# | RDD sem Catalyst | Nenhuma otimização automática — tudo é responsabilidade sua |
# | `df.rdd` é caro | Converte UnsafeRow → Row Java → overhead de serialização |
# | `toPandas()` coleta tudo | O Driver precisa ter memória para TODOS os dados |
# | Dataset não existe em Python | Em PySpark, DataFrame é tudo. Dataset é Scala/Java. |
# | `inferSchema` é lento | Faz 2 passes no arquivo. Use schema explícito em produção. |
# | `groupByKey` vs `reduceByKey` | `groupByKey` traz todos os valores para o Executor antes de agregar — use `reduceByKey` que combina localmente primeiro |
# | TempView vs GlobalTempView | TempView = SparkSession atual. GlobalTempView = todas as sessions, prefixo `global_temp.` |
# | mapPartitions > map para I/O | Abre/fecha recurso 1× por partição em vez de 1× por linha |
# | Quando usar RDD hoje | I/O externo via mapPartitions, ops sem equivalente SQL, GraphX/MLlib legado |

# COMMAND ----------
