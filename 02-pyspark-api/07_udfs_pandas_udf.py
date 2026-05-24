# Databricks notebook source

# MAGIC %md
# # 07 — UDFs e Pandas UDFs: UDF vs Pandas UDF (Arrow), Performance
#
# **Analogia:**
# Imagine uma linha de montagem em uma fábrica (o Spark processando dados).
# As peças passam pela esteira (partições) e robôs (funções nativas do Spark)
# processam tudo em alta velocidade, sem parar a esteira.
#
# **UDF Python pura** é como contratar um trabalhador manual externo: a esteira para,
# a peça é embalada em papel-bolha (serialização Python), enviada para o trabalhador,
# ele processa UMA peça de cada vez, embala de volta e devolve. Para cada peça,
# esse processo se repete. Lento e com muito overhead.
#
# **Pandas UDF (Arrow)** é como um trabalhador que recebe uma CAIXA inteira de peças
# de uma vez (batch via Apache Arrow), processa todas com ferramentas vetorizadas
# (pandas/numpy), e devolve a caixa. Muito mais eficiente — a transferência ocorre
# em formato colunar binário sem serialização Java↔Python linha por linha.
#
# **Funções nativas Spark** são os robôs da própria linha: nunca saem da esteira,
# processam em JVM com Whole-Stage CodeGen, sem nenhuma transferência de dados.
#
# **Conceito técnico:**
# - **UDF Python:** serializa cada Row via Pickle, envia para processo Python externo,
#   recebe de volta — O(N) roundtrips entre JVM e Python. Quebra WSCG e Catalyst.
# - **Pandas UDF (PyArrow):** transfere batches em formato Arrow (colunar, zero-copy),
#   processa com pandas vetorizado — um batch por chamada. Ainda quebra WSCG mas
#   é ordens de magnitude mais rápido que UDF puro.
# - **UDF Scala/Java:** roda dentro da JVM — sem overhead de comunicação.
#   Compatível com Tungsten/WSCG (em alguns casos).
#
# **Quando usar este conhecimento:**
# - Decidir QUANDO escrever uma UDF (e quando não escrever)
# - Escolher o tipo certo de UDF para o caso de uso
# - Diagnosticar pipelines lentos causados por UDFs Python
# - Entrevistas sênior: "como você otimizaria uma UDF lenta?"

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, udf, pandas_udf, lit, upper, length, regexp_replace,
    when, trim, lower
)
from pyspark.sql.types import (
    StringType, LongType, DoubleType, IntegerType, BooleanType,
    StructType, StructField, ArrayType
)
import pandas as pd
import numpy as np
import time
import re

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# ## 1. Por que evitar UDFs Python — o custo real

# COMMAND ----------

# MAGIC %md
# ### O que acontece internamente em uma UDF Python
#
# ```
# ┌─────────────────────────────────────────────────────────────────────┐
# │                        EXECUTOR JVM                                 │
# │                                                                     │
# │  ┌─────────────────────────────────────────────────────────────┐   │
# │  │ Dados em UnsafeRow (binário compacto, off-heap eficiente)   │   │
# │  └─────────────────────┬───────────────────────────────────────┘   │
# │                         │ Para cada linha:                         │
# │                         │ 1. Desserializa UnsafeRow → objeto Java   │
# │                         │ 2. Serializa com Pickle → bytes Python   │
# │                         │ 3. Envia via socket local                │
# │                         ▼                                          │
# │  ┌─────────────────────────────────────────────────────────────┐   │
# │  │ Processo Python (worker separado, fora da JVM)              │   │
# │  │  · Recebe bytes, deserializa Pickle → objeto Python        │   │
# │  │  · Executa a função UDF (1 linha por vez)                  │   │
# │  │  · Serializa resultado com Pickle → bytes                  │   │
# │  │  · Envia de volta via socket                               │   │
# │  └─────────────────────┬───────────────────────────────────────┘   │
# │                         │ Para cada linha:                         │
# │                         │ 4. Recebe bytes, desserializa Pickle     │
# │                         │ 5. Converte para UnsafeRow               │
# │                         ▼                                          │
# │  ┌─────────────────────────────────────────────────────────────┐   │
# │  │ Dados de volta em UnsafeRow → continua o pipeline          │   │
# │  └─────────────────────────────────────────────────────────────┘   │
# │                                                                     │
# │  Consequências:                                                     │
# │  ✗ Whole-Stage CodeGen QUEBRADO (boundary: BatchEvalPython)        │
# │  ✗ Catalyst não pode otimizar (caixa-preta)                        │
# │  ✗ N × (desserialização + socket + serialização) por partição      │
# │  ✗ Pressão de memória: objetos Python no worker externo            │
# └─────────────────────────────────────────────────────────────────────┘
# ```

# COMMAND ----------

# Visualizar o quebra no Physical Plan
df_bench = spark.range(1_000_000).withColumn("texto", lit("  Hello World  "))

# Função nativa — dentro do WSCG
df_nativo = df_bench.withColumn("processado", upper(trim(col("texto"))))
print("=== Função nativa — DENTRO do WSCG ===")
df_nativo.explain(mode="simple")

# COMMAND ----------

# UDF Python — quebra o WSCG
@udf(returnType=StringType())
def processar_python(texto):
    if texto is None:
        return None
    return texto.strip().upper()

df_udf = df_bench.withColumn("processado", processar_python(col("texto")))
print("=== UDF Python — FORA do WSCG (BatchEvalPython) ===")
df_udf.explain(mode="simple")
# Procure: BatchEvalPython → boundary do codegen

# COMMAND ----------

# MAGIC %md
# ## 2. UDF Python — Declaração e Uso

# COMMAND ----------

# MAGIC %md
# ### Formas de declarar UDFs

# COMMAND ----------

# Forma 1: decorator @udf
@udf(returnType=StringType())
def formatar_nome(nome):
    """Capitaliza e remove espaços extras"""
    if nome is None:
        return None
    return " ".join(part.capitalize() for part in nome.strip().split())

# Forma 2: udf() como função (mais explícita — preferível em produção)
def calcular_categoria(valor):
    """Categoriza por faixa de valor"""
    if valor is None:
        return "desconhecido"
    if valor < 1000:
        return "baixo"
    elif valor < 5000:
        return "medio"
    else:
        return "alto"

udf_categoria = udf(calcular_categoria, StringType())

# Forma 3: lambda simples
udf_dobra = udf(lambda x: x * 2 if x is not None else None, LongType())

# COMMAND ----------

# Uso das UDFs
df_exemplo = spark.createDataFrame([
    ("  ana silva  ", 500.0),
    ("BRUNO COSTA",   3200.0),
    ("carla",         8000.0),
    (None,            None),
], ["nome", "valor"])

df_resultado = (
    df_exemplo
    .withColumn("nome_formatado", formatar_nome(col("nome")))
    .withColumn("categoria",      udf_categoria(col("valor")))
    .withColumn("valor_dobrado",  udf_dobra(col("valor").cast(LongType())))
)

df_resultado.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ### UDF com tipo de retorno complexo (StructType, ArrayType)

# COMMAND ----------

# UDF que retorna um Struct (múltiplos campos)
schema_resultado = StructType([
    StructField("palavras",    LongType(),  True),
    StructField("caracteres",  LongType(),  True),
    StructField("tem_numero",  BooleanType(), True),
])

@udf(returnType=schema_resultado)
def analisar_texto(texto):
    if texto is None:
        return None
    texto_limpo = texto.strip()
    return {
        "palavras":   len(texto_limpo.split()),
        "caracteres": len(texto_limpo),
        "tem_numero": any(c.isdigit() for c in texto_limpo),
    }

df_texto = spark.createDataFrame(
    [("Vendas de 2024 foram ótimas",), ("Produto A",), (None,)],
    ["descricao"]
)

df_analise = (
    df_texto
    .withColumn("analise", analisar_texto(col("descricao")))
    .select("descricao",
            col("analise.palavras"),
            col("analise.caracteres"),
            col("analise.tem_numero"))
)
df_analise.show(truncate=False)

# COMMAND ----------

# UDF que retorna Array
@udf(returnType=ArrayType(StringType()))
def tokenizar(texto):
    if texto is None:
        return []
    return re.findall(r'\b\w+\b', texto.lower())

df_tokens = df_texto.withColumn("tokens", tokenizar(col("descricao")))
df_tokens.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ### ⚠️ Registro de UDFs para uso em SQL

# COMMAND ----------

# Para usar UDFs em spark.sql() é necessário registrá-las
spark.udf.register("formatar_nome_sql", formatar_nome)
spark.udf.register("calcular_categoria_sql", calcular_categoria, StringType())

df_exemplo.createOrReplaceTempView("dados")
spark.sql("""
    SELECT
        formatar_nome_sql(nome)        AS nome_formatado,
        calcular_categoria_sql(valor)  AS categoria
    FROM dados
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ## 3. Pandas UDF (Arrow) — Vetorizado e Eficiente

# COMMAND ----------

# MAGIC %md
# ### Como Pandas UDF funciona internamente
#
# ```
# ┌─────────────────────────────────────────────────────────────────────┐
# │                        EXECUTOR JVM                                 │
# │                                                                     │
# │  ┌─────────────────────────────────────────────────────────────┐   │
# │  │ Dados em UnsafeRow (colunar)                                │   │
# │  └──────────────────────┬──────────────────────────────────────┘   │
# │                          │ Por BATCH (não por linha):              │
# │                          │ 1. Converte batch para Apache Arrow     │
# │                          │    (formato colunar binário — zero-copy)│
# │                          ▼                                         │
# │  ┌─────────────────────────────────────────────────────────────┐   │
# │  │ Processo Python (worker com Arrow bridge)                   │   │
# │  │  · Recebe Arrow RecordBatch → pd.Series sem cópia          │   │
# │  │  · Executa função sobre TODA a Series de uma vez           │   │
# │  │  · Operações pandas/numpy são vetorizadas (C/Fortran)      │   │
# │  │  · Retorna pd.Series → Arrow RecordBatch                   │   │
# │  └──────────────────────┬──────────────────────────────────────┘   │
# │                          │ Por BATCH:                              │
# │                          │ 2. Recebe Arrow → converte p/ UnsafeRow │
# │                          ▼                                         │
# │  ┌─────────────────────────────────────────────────────────────┐   │
# │  │ Pipeline continua em UnsafeRow                              │   │
# │  └─────────────────────────────────────────────────────────────┘   │
# │                                                                     │
# │  Vantagens vs UDF Python:                                          │
# │  ✓ N roundtrips → 1 roundtrip por batch (muito menos overhead)    │
# │  ✓ Arrow: transferência colunar sem Pickle → menos CPU e memória   │
# │  ✓ pandas/numpy: operações vetorizadas em C                        │
# │                                                                     │
# │  Ainda limitado vs nativo:                                         │
# │  ✗ WSCG ainda quebrado (ArrowEvalPython boundary)                  │
# │  ✗ Catalyst não otimiza o interior da função                       │
# └─────────────────────────────────────────────────────────────────────┘
# ```

# COMMAND ----------

# Verificar se Arrow está ativo para Pandas UDFs
print("Arrow para PySpark:",
      spark.conf.get("spark.sql.execution.arrow.pyspark.enabled", "false"))

# Ativar se necessário (geralmente já ativo no Databricks)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# COMMAND ----------

# MAGIC %md
# ### Tipos de Pandas UDF (Spark 3.x)

# COMMAND ----------

# MAGIC %md
# ### Tipo 1: Series → Series (substituição de UDF escalar)

# COMMAND ----------

# A função recebe uma pd.Series e retorna uma pd.Series do mesmo tamanho
@pandas_udf(StringType())
def formatar_nome_pandas(serie: pd.Series) -> pd.Series:
    """Equivalente ao formatar_nome Python, mas vetorizado"""
    return serie.str.strip().str.title()

@pandas_udf(StringType())
def calcular_categoria_pandas(serie: pd.Series) -> pd.Series:
    """Categorização vetorizada com np.select"""
    condicoes = [serie < 1000, (serie >= 1000) & (serie < 5000), serie >= 5000]
    escolhas  = ["baixo", "medio", "alto"]
    return pd.Series(np.select(condicoes, escolhas, default="desconhecido"))

# Uso idêntico ao UDF Python — só muda a declaração
df_pandas = (
    df_exemplo
    .withColumn("nome_formatado",  formatar_nome_pandas(col("nome")))
    .withColumn("categoria",       calcular_categoria_pandas(col("valor")))
)
df_pandas.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ### Tipo 2: Series, Series → Series (múltiplas colunas de entrada)

# COMMAND ----------

@pandas_udf(DoubleType())
def calcular_desconto(valor: pd.Series, categoria: pd.Series) -> pd.Series:
    """Aplica desconto diferente por categoria — função com 2 inputs"""
    desconto = pd.Series(np.zeros(len(valor)))
    desconto = np.where(categoria == "alto",  valor * 0.10, desconto)
    desconto = np.where(categoria == "medio", valor * 0.05, desconto)
    return pd.Series(desconto)

df_desconto = (
    df_exemplo
    .withColumn("categoria", calcular_categoria_pandas(col("valor")))
    .withColumn("desconto",  calcular_desconto(col("valor"), col("categoria")))
)
df_desconto.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ### Tipo 3: Iterator de Series → Iterator de Series (batch processing com setup)

# COMMAND ----------

# MAGIC %md
# Ideal quando a função tem **estado compartilhado** que precisa ser inicializado
# uma vez por partição: carregar um modelo de ML, abrir uma conexão, etc.

# COMMAND ----------

from typing import Iterator

@pandas_udf(DoubleType())
def normalizar_com_modelo(
    iterator: Iterator[pd.Series]
) -> Iterator[pd.Series]:
    """
    Inicializa recursos 1x por partição (ex: carrega modelo ML),
    processa todos os batches com o mesmo estado.
    """
    # Setup: executado 1x por partição (não 1x por batch)
    # modelo = joblib.load("/dbfs/models/scaler.pkl")  # ← exemplo real
    media, std = 10000.0, 3000.0  # simulando parâmetros de normalização

    for batch in iterator:  # itera sobre todos os batches da partição
        # Usa o modelo/parâmetros carregados acima
        yield (batch - media) / std  # normalização z-score

df_norm = df.withColumn("receita_normalizada", normalizar_com_modelo(col("receita")))
df_norm.select("vendedor", "mes", "receita", "receita_normalizada").show(5)

# COMMAND ----------

# MAGIC %md
# ### Tipo 4: GroupedMap — Pandas UDF por grupo (applyInPandas)

# COMMAND ----------

# applyInPandas: recebe um pandas DataFrame por grupo e retorna um pandas DataFrame
# Equivalente a groupBy + apply em pandas, mas distribuído

schema_saida = StructType([
    StructField("vendedor",          StringType(), True),
    StructField("mes",               StringType(), True),
    StructField("receita",           DoubleType(), True),
    StructField("receita_normalizada", DoubleType(), True),
    StructField("z_score",           DoubleType(), True),
])

def normalizar_por_vendedor(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza a receita dentro de cada grupo (vendedor).
    Recebe um pd.DataFrame completo do grupo — pode usar qualquer lógica pandas.
    """
    media = pdf["receita"].mean()
    std   = pdf["receita"].std()
    pdf["receita_normalizada"] = (pdf["receita"] - media) / (std if std > 0 else 1)
    pdf["z_score"] = pdf["receita_normalizada"].round(2)
    return pdf

df_grouped = df.groupBy("vendedor").applyInPandas(
    normalizar_por_vendedor,
    schema=schema_saida
)

df_grouped.orderBy("vendedor", "mes").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ## 4. Benchmark: Nativo vs UDF Python vs Pandas UDF

# COMMAND ----------

N = 5_000_000
df_perf = (
    spark.range(N)
    .withColumn("texto", lit("  hello world spark  "))
    .withColumn("valor", (col("id") % 10000).cast(DoubleType()))
)
df_perf.cache()
df_perf.count()  # materializa o cache para benchmark justo

# COMMAND ----------

# Função nativa
start = time.time()
df_perf.withColumn("res", upper(trim(col("texto")))).count()
t_nativo = time.time() - start
print(f"Nativo (WSCG):     {t_nativo:.2f}s")

# COMMAND ----------

# UDF Python puro
@udf(StringType())
def udf_py(x):
    return x.strip().upper() if x else None

start = time.time()
df_perf.withColumn("res", udf_py(col("texto"))).count()
t_udf = time.time() - start
print(f"UDF Python:        {t_udf:.2f}s  ({t_udf/t_nativo:.1f}x mais lento que nativo)")

# COMMAND ----------

# Pandas UDF
@pandas_udf(StringType())
def pandas_udf_fn(s: pd.Series) -> pd.Series:
    return s.str.strip().str.upper()

start = time.time()
df_perf.withColumn("res", pandas_udf_fn(col("texto"))).count()
t_pandas = time.time() - start
print(f"Pandas UDF (Arrow): {t_pandas:.2f}s  ({t_pandas/t_nativo:.1f}x mais lento que nativo)")
print(f"Speedup Pandas vs Python UDF: {t_udf/t_pandas:.1f}x")

df_perf.unpersist()

# COMMAND ----------

# MAGIC %md
# ### Resultados típicos de benchmark
#
# ```
# ┌─────────────────────┬──────────┬─────────────────────────────────────────┐
# │ Abordagem           │ Tempo    │ Observações                             │
# ├─────────────────────┼──────────┼─────────────────────────────────────────┤
# │ Função nativa       │ 1x       │ WSCG ativo, zero overhead Python        │
# │ Pandas UDF (Arrow)  │ 3-8x     │ 1 roundtrip/batch, vetorizado           │
# │ Python UDF puro     │ 20-100x  │ 1 roundtrip/linha, Pickle               │
# └─────────────────────┴──────────┴─────────────────────────────────────────┘
# ```

# COMMAND ----------

# MAGIC %md
# ## 5. Quando usar (e quando NÃO usar) cada abordagem

# COMMAND ----------

# MAGIC %md
# ```
# HIERARQUIA DE ESCOLHA — do mais para o menos eficiente:
#
# 1. ✅ Funções nativas do Spark SQL / PySpark (col, when, regexp_replace, etc.)
#    → Sempre prefira. Consulte a documentação antes de escrever qualquer UDF.
#    → Spark 3.x tem centenas de funções: string, date, array, struct, JSON, regex...
#
# 2. ✅ Pandas UDF (Arrow) — quando função nativa não existe
#    → Lógica vetorizada com pandas/numpy que não tem equivalente nativo
#    → Modelos de ML aplicados a colunas (sklearn, scipy)
#    → Iterator UDF para carregar recursos pesados 1x por partição
#
# 3. ⚠️ Python UDF puro — último recurso
#    → Lógica que não pode ser vetorizada (ex: parser de formato proprietário linha a linha)
#    → Integração com biblioteca Python que não suporta operações em Series
#    → Código legado que precisa ser portado gradualmente
#
# 4. ❌ Nunca
#    → UDF que reimplementa o que uma função nativa já faz
#    → UDF em loop dentro de outra UDF (recursivo, iterativo)
#    → UDF com efeitos colaterais (escrita em banco, chamada HTTP por linha) → use mapPartitions
# ```

# COMMAND ----------

# MAGIC %md
# ### Verificando se existe função nativa antes de escrever UDF

# COMMAND ----------

# Exemplos de funções nativas que eliminam UDFs comuns
from pyspark.sql.functions import (
    # String
    regexp_extract, regexp_replace, split, concat_ws, substring, lpad, rpad,
    translate, initcap, locate, instr, format_string,
    # Data
    date_format, date_add, date_diff, date_trunc, year, month, dayofmonth,
    to_date, to_timestamp, unix_timestamp, from_unixtime,
    # Array
    explode, array_contains, array_distinct, array_union, array_intersect,
    flatten, transform, filter as array_filter, aggregate,
    # JSON
    from_json, to_json, get_json_object, json_tuple,
    # Condicional
    coalesce, nullif, greatest, least, nanvl,
    # Matemática
    abs, ceil, floor, log, log2, log10, pow, round as spark_round2, signum,
)

# Casos típicos onde devolvemos uma UDF desnecessária:
# "Preciso extrair o domínio do email" → regexp_extract, não UDF
# "Preciso formatar data como dd/mm/yyyy" → date_format, não UDF
# "Preciso calcular idade" → date_diff + date_trunc, não UDF
# "Preciso fazer parse de JSON" → from_json + schema, não UDF

# Exemplo: extrair domínio de email SEM UDF
df_emails = spark.createDataFrame(
    [("ana@empresa.com.br",), ("bruno@gmail.com",), ("carla@hotmail.com",)],
    ["email"]
)

df_emails.withColumn(
    "dominio",
    regexp_extract(col("email"), r"@(.+)$", 1)  # regex nativa — sem UDF
).show()

# COMMAND ----------

# MAGIC %md
# ## 6. Boas práticas e padrões de produção para UDFs

# COMMAND ----------

# MAGIC %md
# ### Tratamento de null — obrigatório em toda UDF

# COMMAND ----------

# UDFs recebem None para valores nulos — SEMPRE trate
@udf(StringType())
def udf_sem_null_check(texto):
    return texto.strip().upper()  # ← ERRO: AttributeError se texto=None

@udf(StringType())
def udf_com_null_check(texto):
    if texto is None:    # ← tratamento obrigatório
        return None
    return texto.strip().upper()

# Pandas UDF: pd.Series já propaga NaN automaticamente em operações vetorizadas
@pandas_udf(StringType())
def pandas_udf_null_safe(serie: pd.Series) -> pd.Series:
    return serie.str.strip().str.upper()  # ← NaN é preservado automaticamente

# COMMAND ----------

# MAGIC %md
# ### Evitar closures com objetos grandes

# COMMAND ----------

# ❌ Ruim: objeto grande capturado no closure → serializado e enviado para cada task
lista_bloqueados_grande = list(range(100_000))  # 100k elementos

@udf(BooleanType())
def udf_com_closure_ruim(id):
    return id not in lista_bloqueados_grande  # lista inteira serializada por task!

# ✅ Bom: broadcast da lista grande → enviada 1x para cada Executor
from pyspark.sql.functions import broadcast as bc_fn, array_contains
bloqueados_df = spark.createDataFrame([(i,) for i in range(100)], ["id"])

# Semi/anti join como alternativa (veja 05_joins_strategies.py)
df_filtrado = spark.range(1000).join(bloqueados_df, "id", "left_anti")
print(f"Registros não bloqueados: {df_filtrado.count()}")

# COMMAND ----------

# MAGIC %md
# ### UDF determinística vs não-determinística

# COMMAND ----------

import random

# UDF não-determinística: resultado muda a cada chamada
# O Spark pode executar a UDF mais de uma vez para a mesma linha (retry, speculative exec)
@udf(DoubleType())
def udf_nao_deterministica(x):
    return float(x) + random.random()  # ← diferente a cada execução!

# Declarar explicitamente como não-determinística evita otimizações que assumem determinismo
udf_nd = udf(lambda x: float(x) + random.random(), DoubleType()).asNondeterministic()

# UDF determinística (padrão): mesma entrada → sempre mesma saída
@udf(DoubleType())
def udf_deterministica(x):
    return float(x) * 2.0  # ← sempre o mesmo resultado

print("UDF determinística:", udf_deterministica.deterministic)
print("UDF não-determinística:", udf_nd.deterministic)

# COMMAND ----------

# MAGIC %md
# ### I/O externo em UDFs — use mapPartitions, não map

# COMMAND ----------

# ❌ Ruim: abre conexão por linha dentro da UDF (N conexões por partição)
@udf(StringType())
def enriquecer_via_api_ruim(id):
    # conexao = requests.get(f"http://api/{id}")  # 1 req por linha → N conexões!
    return f"enriquecido_{id}"

# ✅ Bom: mapPartitions para I/O externo (1 conexão por partição)
def enriquecer_particao(iterador):
    # sessao = requests.Session()  # 1 sessão por partição
    for row in iterador:
        # resultado = sessao.get(f"http://api/{row.id}").json()
        yield (row.id, f"enriquecido_{row.id}")

# df.rdd.mapPartitions(enriquecer_particao).toDF(["id", "resultado"])
print("I/O externo: sempre use mapPartitions para agrupar conexões por partição")

# COMMAND ----------

# MAGIC %md
# ## 7. Tabela comparativa completa

# COMMAND ----------

# MAGIC %md
# ```
# ┌──────────────────────┬────────────────┬─────────────────┬─────────────────┐
# │ Critério             │ UDF Python     │ Pandas UDF      │ Função Nativa   │
# ├──────────────────────┼────────────────┼─────────────────┼─────────────────┤
# │ Performance          │ Lenta (20-100x)│ Moderada (3-8x) │ Máxima (1x)     │
# │ WSCG                 │ Quebra         │ Quebra          │ Mantém          │
# │ Catalyst             │ Caixa-preta    │ Caixa-preta     │ Otimiza         │
# │ Serialização         │ Pickle p/ linha│ Arrow p/ batch  │ Nenhuma         │
# │ Null handling        │ Manual (None)  │ Automático (NaN)│ Automático      │
# │ Tipo de retorno      │ Simples/Struct │ Simples/Struct  │ Depende         │
# │ Acesso a libs Python │ Sim            │ Sim (melhor)    │ Não             │
# │ Modelos ML           │ Lento          │ Recomendado     │ Não             │
# │ Setup por partição   │ Não            │ Iterator UDF    │ Não             │
# │ Registrar em SQL     │ Sim            │ Sim             │ Já disponível   │
# │ Quando usar          │ Último recurso │ Lógica customiz.│ Sempre que poss.│
# └──────────────────────┴────────────────┴─────────────────┴─────────────────┘
# ```

# COMMAND ----------

# MAGIC %md
# ## ⚠️ Resumo de armadilhas e pontos de prova
#
# | Tema | O que saber |
# |---|---|
# | UDF quebra WSCG | `BatchEvalPython` no plano = WSCG interrompido. Pandas UDF: `ArrowEvalPython` |
# | Null em UDF Python | Sempre trate `None` explicitamente — AttributeError silencioso em produção |
# | Null em Pandas UDF | `pd.Series.str.*` e operações numpy propagam NaN automaticamente |
# | Closure com objetos grandes | Objeto capturado é serializado e enviado por task — use broadcast |
# | UDF não-determinística | Declare `.asNondeterministic()` para evitar otimizações incorretas |
# | I/O externo em UDF | UDF abre conexão por linha → use `mapPartitions` para agrupar |
# | Iterator Pandas UDF | Setup (carregar modelo, abrir conexão) executado 1x por partição — eficiente |
# | `applyInPandas` vs `pandas_udf` | `applyInPandas` recebe/retorna DataFrame completo do grupo — mais flexível, mais memória |
# | Registrar UDF para SQL | `spark.udf.register("nome", funcao, tipo)` — obrigatório para `spark.sql()` |
# | Sempre verifique funções nativas primeiro | `regexp_extract`, `date_format`, `from_json`, `array_contains` eliminam 80% das UDFs |

# COMMAND ----------
