# Databricks notebook source

# MAGIC %md
# # 02 — Schema e Tipos de Dados no Spark
#
# > **Arquivo:** `02-pyspark-api/02_schema_types.py`
# > **Módulo:** 02 — PySpark API
# > **Dependência:** `01_sparksession_config.py`
#
# ---
#
# ## Analogia
#
# O schema é como a planta baixa de um apartamento.
# Antes de construir (processar os dados), você define exatamente
# quantos cômodos existem (colunas), qual o tamanho de cada um (tipo),
# e se algum pode estar vazio (nullable).
#
# Deixar o Spark "inferir o schema" é como pedir para um pedreiro
# adivinhar a planta baixa medindo o apartamento depois de construído —
# ele vai chegar perto, mas vai errar detalhes, vai demorar mais,
# e você vai descobrir os erros na hora da mudança.
#
# ---
#
# ## Por que schema explícito é sempre melhor em produção
#
# | | Schema inferido | Schema explícito |
# |---|---|---|
# | **Performance** | Lê todos os dados duas vezes | Lê uma vez |
# | **Confiabilidade** | Infere `string` onde deveria ser `date` | Você define o tipo correto |
# | **Erros** | Descobre em runtime | Descobre na definição |
# | **Evolução** | Quebra silenciosamente com novo arquivo | Falha explicitamente (enforced) |
# | **Custo** | Job extra de inferência | Sem custo extra |

# COMMAND ----------

from pyspark.sql.types import (
    # Tipos numéricos inteiros
    ByteType,       # 1 byte  — valores -128 a 127
    ShortType,      # 2 bytes — valores -32.768 a 32.767
    IntegerType,    # 4 bytes — valores -2.1B a 2.1B
    LongType,       # 8 bytes — valores até 9.2 quintilhões (mais comum)

    # Tipos numéricos decimais
    FloatType,      # 4 bytes — precisão simples (evitar para dinheiro)
    DoubleType,     # 8 bytes — precisão dupla  (evitar para dinheiro)
    DecimalType,    # precisão exata — USE SEMPRE para valores monetários

    # Tipo texto
    StringType,     # string Unicode de comprimento variável

    # Tipo booleano
    BooleanType,    # true / false / null

    # Tipos de data e tempo
    DateType,       # apenas data: 2024-01-15
    TimestampType,  # data + hora + timezone: 2024-01-15 10:30:00
    TimestampNTZType, # data + hora SEM timezone (Spark 3.4+)

    # Tipo binário
    BinaryType,     # array de bytes — imagens, arquivos, hashes

    # Tipos complexos
    ArrayType,      # lista de elementos do mesmo tipo
    MapType,        # dicionário chave-valor
    StructType,     # objeto com campos nomeados (como uma linha/tabela)
    StructField,    # define um campo dentro de StructType
)
from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %md
# ## 1. StructType e StructField — anatomia de um schema

# COMMAND ----------

# ── StructField — um campo do schema ─────────────────────────────────────
#
# StructField(nome, tipo, nullable, metadata)
#   nome:     string — nome da coluna
#   tipo:     DataType — tipo de dados
#   nullable: bool — se aceita valor nulo (None/null)
#             True  = aceita null (padrão)
#             False = não aceita null — Spark lança erro se encontrar
#   metadata: dict — metadados extras (documentação, origem, etc.) — opcional

from pyspark.sql.types import StructField, LongType, StringType, DecimalType

campo_id = StructField(
    name="id",
    dataType=LongType(),
    nullable=False,    # ID nunca pode ser nulo
    metadata={"description": "Identificador único do pedido", "source": "oracle_erp"}
)

campo_valor = StructField(
    name="valor",
    dataType=DecimalType(precision=18, scale=2),
    nullable=True,     # valor pode ser nulo em pedidos rascunho
)

print(campo_id)
# StructField('id', LongType(), False)

# COMMAND ----------

# ── StructType — o schema completo (conjunto de StructFields) ─────────────

from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DecimalType,
    TimestampType, BooleanType, DateType
)

schema_pedidos = StructType([
    StructField("id",           LongType(),                nullable=False),
    StructField("id_cliente",   LongType(),                nullable=False),
    StructField("id_produto",   LongType(),                nullable=True),
    StructField("valor",        DecimalType(18, 2),        nullable=True),
    StructField("quantidade",   IntegerType(),             nullable=True),
    StructField("status",       StringType(),              nullable=True),
    StructField("canal",        StringType(),              nullable=True),
    StructField("criado_em",    TimestampType(),           nullable=True),
    StructField("data_pedido",  DateType(),                nullable=True),
    StructField("processado",   BooleanType(),             nullable=True),
])

# Inspecionar schema
print(schema_pedidos)
print()
print(schema_pedidos.simpleString())
# struct<id:bigint,id_cliente:bigint,...>

# COMMAND ----------

# ── Aplicando o schema na leitura ─────────────────────────────────────────

df_pedidos = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("sep", ";")
    .option("encoding", "UTF-8")
    .schema(schema_pedidos)    # schema explícito — sem inferência
    .load("/mnt/raw/pedidos/*.csv")
)

df_pedidos.printSchema()
df_pedidos.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# ## 2. Todos os tipos de dados — guia de uso

# COMMAND ----------

# MAGIC %md
# ### 2.1 Tipos numéricos inteiros

# COMMAND ----------

from pyspark.sql.types import ByteType, ShortType, IntegerType, LongType
from pyspark.sql import Row

dados_numericos = [Row(
    byte_val=127,
    short_val=32767,
    int_val=2147483647,
    long_val=9223372036854775807,
)]

schema_num = StructType([
    StructField("byte_val",  ByteType(),    True),
    StructField("short_val", ShortType(),   True),
    StructField("int_val",   IntegerType(), True),
    StructField("long_val",  LongType(),    True),
])

df_num = spark.createDataFrame(dados_numericos, schema=schema_num)
df_num.printSchema()

# COMMAND ----------

# ── Quando usar cada tipo inteiro ─────────────────────────────────────────
#
# ByteType   → flags, status codes com poucos valores (0-127)
# ShortType  → raramente útil em análise de dados
# IntegerType → IDs pequenos, contadores, anos, meses
# LongType   → IDs grandes, timestamps Unix (milissegundos), contagens altas
#
# Regra de ouro:
# → Se veio de banco relacional como INT: IntegerType()
# → Se veio de banco relacional como BIGINT: LongType()
# → Para IDs de sistemas modernos: sempre LongType() — seguro
# → Para o campo "ano": IntegerType() — 2024 cabe em 4 bytes

# COMMAND ----------

# MAGIC %md
# ### 2.2 Tipos decimais — o que usar para dinheiro

# COMMAND ----------

from pyspark.sql.types import FloatType, DoubleType, DecimalType

# ── FloatType e DoubleType — EVITAR para valores monetários ───────────────
#
# Float e Double usam representação binária de ponto flutuante (IEEE 754)
# Isso causa erros de arredondamento que são INACEITÁVEIS para finanças

dados_float = [(0.1 + 0.2,)]   # deveria ser 0.3
schema_f = StructType([StructField("resultado", DoubleType(), True)])
df_float = spark.createDataFrame(dados_float, schema=schema_f)
df_float.show()
# +--------------------+
# |           resultado|
# +--------------------+
# |0.30000000000000004|   ← ERRO de ponto flutuante!
# +--------------------+

# COMMAND ----------

# ── DecimalType — USE SEMPRE para valores monetários ─────────────────────
#
# DecimalType(precision, scale)
#   precision: número total de dígitos significativos
#   scale:     número de dígitos após o ponto decimal
#
# Exemplos:
#   DecimalType(10, 2)  → até 99.999.999,99  (valores em R$)
#   DecimalType(18, 2)  → até 9.999.999.999.999.999,99  (seguro para qualquer moeda)
#   DecimalType(38, 10) → máximo suportado pelo Spark

from pyspark.sql.functions import lit
from decimal import Decimal

dados_decimal = [(Decimal("0.1") + Decimal("0.2"),)]
schema_d = StructType([StructField("resultado", DecimalType(10, 1), True)])
df_decimal = spark.createDataFrame(dados_decimal, schema=schema_d)
df_decimal.show()
# +----------+
# | resultado|
# +----------+
# |       0.3|   ← correto ✅
# +----------+

# COMMAND ----------

# Regra de DecimalType para casos comuns:
decimais_comuns = {
    "valor_monetario_brl":  DecimalType(18, 2),   # até R$ 9.999.999.999.999.999,99
    "valor_monetario_usd":  DecimalType(18, 4),   # 4 casas para câmbio
    "percentual":           DecimalType(7, 4),    # ex: 99.9999%
    "latitude_longitude":   DecimalType(10, 7),   # ex: -23.5505199
    "taxa_juros":           DecimalType(9, 6),    # ex: 0.125000 (12.5%)
}
for nome, tipo in decimais_comuns.items():
    print(f"{nome:<28} → {tipo}")

# COMMAND ----------

# MAGIC %md
# ### 2.3 StringType — texto

# COMMAND ----------

# StringType é o tipo mais simples e o mais usado
# Aceita qualquer sequência Unicode, sem limite de comprimento

# Não existe VARCHAR(N) no Spark como em bancos relacionais
# Toda string é de comprimento variável

schema_texto = StructType([
    StructField("nome",     StringType(), True),
    StructField("cpf",      StringType(), True),  # guarde CPF como string, não int
    StructField("cep",      StringType(), True),  # CEP também — zeros à esquerda
    StructField("telefone", StringType(), True),
])

# ⚠️ Atenção: nunca armazene CPF, CNPJ, CEP, código de barras como IntegerType
# → "01310100" (CEP) viraria 1310100 — perde o zero à esquerda
# → "012.345.678-90" (CPF) — perderia formatação e zeros

# COMMAND ----------

# MAGIC %md
# ### 2.4 BooleanType

# COMMAND ----------

# BooleanType aceita: true, false, null
# Na leitura de CSV: "true"/"false", "1"/"0", "yes"/"no" são convertidos

schema_bool = StructType([
    StructField("ativo",      BooleanType(), True),
    StructField("verificado", BooleanType(), True),
])

dados_bool = [(True, None), (False, True), (None, False)]
df_bool = spark.createDataFrame(dados_bool, schema=schema_bool)
df_bool.show()

# Filtros com booleano
from pyspark.sql.functions import col
df_bool.filter(col("ativo") == True).show()
df_bool.filter(col("ativo")).show()         # equivalente — mais idiomático
df_bool.filter(~col("ativo")).show()        # NOT ativo
df_bool.filter(col("ativo").isNull()).show()

# COMMAND ----------

# MAGIC %md
# ### 2.5 DateType e TimestampType — datas e horas

# COMMAND ----------

from pyspark.sql.types import DateType, TimestampType, TimestampNTZType
from pyspark.sql.functions import (
    to_date, to_timestamp, current_date, current_timestamp,
    year, month, dayofmonth, date_format, date_add, datediff
)

# DateType → apenas a data, sem hora — armazena como dias desde 1970-01-01
# TimestampType → data + hora + fuso horário — armazena como microssegundos desde epoch
# TimestampNTZType → data + hora SEM fuso horário (Spark 3.4+)

schema_datas = StructType([
    StructField("data_pedido",  DateType(),      True),
    StructField("criado_em",    TimestampType(), True),
])

dados_datas = [("2024-01-15", "2024-01-15 10:30:00")]
df_datas = spark.createDataFrame(dados_datas, schema=["data_str", "ts_str"])

df_datas = (df_datas
    .withColumn("data_pedido", to_date(col("data_str"), "yyyy-MM-dd"))
    .withColumn("criado_em",   to_timestamp(col("ts_str"), "yyyy-MM-dd HH:mm:ss"))
)

df_datas.printSchema()
# root
#  |-- data_pedido: date (nullable = true)
#  |-- criado_em: timestamp (nullable = true)

# COMMAND ----------

# ── Formatos de data mais comuns ──────────────────────────────────────────
formatos_data = {
    "yyyy-MM-dd":           "2024-01-15",           # ISO 8601 — padrão internacional
    "dd/MM/yyyy":           "15/01/2024",           # Brasil
    "MM/dd/yyyy":           "01/15/2024",           # EUA
    "yyyyMMdd":             "20240115",             # formato compacto
    "yyyy-MM-dd HH:mm:ss":  "2024-01-15 10:30:00",  # datetime sem timezone
    "yyyy-MM-dd'T'HH:mm:ss": "2024-01-15T10:30:00", # ISO 8601 datetime
}

for fmt, exemplo in formatos_data.items():
    df_test = spark.createDataFrame([(exemplo,)], ["data_str"])
    df_test = df_test.withColumn("parsed", to_date(col("data_str"), fmt))
    result = df_test.select("parsed").collect()[0][0]
    print(f"{fmt:<30} → '{exemplo}' → {result}")

# COMMAND ----------

# MAGIC %md
# ### 2.6 Tipos complexos: ArrayType, MapType, StructType aninhado

# COMMAND ----------

from pyspark.sql.types import ArrayType, MapType

# ── ArrayType — lista de elementos do mesmo tipo ──────────────────────────

schema_array = StructType([
    StructField("id",   LongType(),                            False),
    StructField("tags", ArrayType(StringType(), True),         True),
    # ArrayType(elementType, containsNull)
    # containsNull: se os elementos do array podem ser null
])

dados_array = [
    (1, ["spark", "delta", "python"]),
    (2, ["databricks", None]),
    (3, []),
]
df_array = spark.createDataFrame(dados_array, schema=schema_array)
df_array.show(truncate=False)

# Operações com arrays
from pyspark.sql.functions import (
    explode, posexplode, array_contains, array_size,
    array_distinct, array_sort, flatten, array
)

# explode: uma linha por elemento do array
df_array.select("id", explode("tags").alias("tag")).show()

# posexplode: inclui o índice
df_array.select("id", posexplode("tags").alias("pos", "tag")).show()

# array_contains: verificar se elemento existe
df_array.filter(array_contains("tags", "spark")).show()

# COMMAND ----------

# ── MapType — dicionário chave-valor ──────────────────────────────────────

from pyspark.sql.types import MapType

schema_map = StructType([
    StructField("id",         LongType(), False),
    StructField("atributos",  MapType(StringType(), StringType(), True), True),
    # MapType(keyType, valueType, valueContainsNull)
])

dados_map = [
    (1, {"cor": "azul", "tamanho": "M", "material": "algodão"}),
    (2, {"cor": "preto", "tamanho": "G"}),
    (3, None),
]
df_map = spark.createDataFrame(dados_map, schema=schema_map)
df_map.show(truncate=False)

# Acessar valor por chave
from pyspark.sql.functions import map_keys, map_values, col
df_map.select("id", col("atributos")["cor"].alias("cor")).show()
df_map.select("id", map_keys("atributos").alias("chaves")).show()

# COMMAND ----------

# ── StructType aninhado — objeto dentro de objeto ─────────────────────────

schema_aninhado = StructType([
    StructField("id",       LongType(),  False),
    StructField("endereco", StructType([
        StructField("logradouro", StringType(),  True),
        StructField("numero",     StringType(),  True),
        StructField("cidade",     StringType(),  True),
        StructField("uf",         StringType(),  True),
        StructField("cep",        StringType(),  True),
    ]), True),
])

dados_struct = [
    (1, ("Rua das Flores", "100", "São Paulo", "SP", "01310100")),
    (2, ("Av Atlântica", "1702", "Rio de Janeiro", "RJ", "22021001")),
]
df_struct = spark.createDataFrame(dados_struct, schema=schema_aninhado)
df_struct.printSchema()
df_struct.show(truncate=False)

# Acessar campo aninhado com ponto
df_struct.select(
    "id",
    col("endereco.cidade").alias("cidade"),
    col("endereco.uf").alias("uf")
).show()

# COMMAND ----------

# MAGIC %md
# ## 3. Schema inference — quando usar e quando evitar

# COMMAND ----------

# ── Schema inference: o que acontece internamente ─────────────────────────
#
# Quando você usa inferSchema=True (CSV) ou lê Parquet/Delta sem schema,
# o Spark precisa "adivinhar" os tipos. O processo é:
#
# Para CSV:
#   1. Primeiro Job: lê TODOS os dados para coletar amostras
#   2. Para cada coluna, tenta converter: IntegerType → LongType → DoubleType → StringType
#   3. Segundo Job: lê os dados novamente com o schema inferido
#   → 2x o custo de I/O, mais o tempo de inferência
#
# Para Parquet e Delta:
#   Schema está embutido nos metadados do arquivo
#   → Inferência é barata — lê só o footer do Parquet
#   → Mas pode inferir tipos errados se os dados foram mal escritos

# COMMAND ----------

# ── Inferência em CSV — problemas comuns ──────────────────────────────────

# Cenário: arquivo CSV com coluna "cpf" contendo "012.345.678-90"
# Spark com inferSchema vai inferir StringType ✅ (correto aqui)

# Cenário: arquivo CSV com coluna "valor" contendo "1234.56"
# Spark pode inferir DoubleType — mas você queria DecimalType(18,2)
# → Erro silencioso: cálculos monetários ficam com arredondamento errado

# Cenário: arquivo CSV com coluna "data" contendo "15/01/2024"
# Spark vai inferir StringType (não consegue identificar como data)
# → Você vai precisar converter depois de qualquer forma

# COMMAND ----------

# ── Comparação explícito vs inferido ─────────────────────────────────────

caminho = "/mnt/raw/pedidos/pedidos_2024.csv"

# Sem schema — inferência automática
df_inferido = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")      # ← lê duas vezes
    .load(caminho)
)
df_inferido.printSchema()
# Provavelmente vai inferir "valor" como DoubleType — errado para dinheiro
# Pode inferir "data_pedido" como StringType — precisa converter depois

# COMMAND ----------

# Com schema explícito — correto e eficiente
schema_explicito = StructType([
    StructField("id",          LongType(),         False),
    StructField("id_cliente",  LongType(),         True),
    StructField("valor",       DecimalType(18, 2), True),  # correto para dinheiro
    StructField("data_pedido", DateType(),         True),  # correto para data
    StructField("status",      StringType(),       True),
])

df_explicito = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("sep", ";")
    .option("dateFormat", "dd/MM/yyyy")  # formato de data do arquivo
    .schema(schema_explicito)            # sem inferência
    .load(caminho)
)
df_explicito.printSchema()

# COMMAND ----------

# MAGIC %md
# ### Quando inferência é aceitável
#
# | Situação | Recomendação |
# |----------|-------------|
# | Exploração interativa no notebook | ✅ Inferência aceitável |
# | Leitura de Parquet/Delta em exploração | ✅ Schema embutido — barato |
# | Pipeline de produção com CSV | ❌ Sempre schema explícito |
# | Pipeline de produção com qualquer formato | ❌ Sempre schema explícito |
# | DLT com Autoloader | ⚠️ Inferência + schema evolution com rescue_data |

# COMMAND ----------

# MAGIC %md
# ## 4. Nullable — implicações práticas

# COMMAND ----------

from pyspark.sql.functions import col, isnull, isnotnull, count, when

# ── nullable=False: o que acontece na prática ─────────────────────────────
#
# nullable=False no schema NÃO impede que nulls existam no dado!
# É uma declaração de intenção ao Catalyst para otimização.
# Se dados nulos chegarem, o comportamento depende do contexto:
# - Leitura de Parquet: ignora a declaração, nullable=False vira aviso
# - Delta MERGE: respeita constraints se definidas separadamente
# - Para GARANTIR não-nulos: use NOT NULL constraint no CREATE TABLE

schema_strict = StructType([
    StructField("id",    LongType(),    nullable=False),  # declaração de intenção
    StructField("nome",  StringType(),  nullable=True),
    StructField("valor", DecimalType(18,2), nullable=True),
])

# COMMAND ----------

# ── Trabalhando com nulls ─────────────────────────────────────────────────

dados_nulos = [
    (1,    "Ana",    100.0),
    (2,    None,     200.0),
    (3,    "Carlos", None),
    (None, "Diana",  300.0),
]
schema_nulos = StructType([
    StructField("id",    LongType(),    True),
    StructField("nome",  StringType(),  True),
    StructField("valor", DoubleType(),  True),
])
df_nulos = spark.createDataFrame(dados_nulos, schema=schema_nulos)

# Detectar nulls
df_nulos.select([
    count(when(isnull(c), c)).alias(f"nulls_{c}")
    for c in df_nulos.columns
]).show()

# Filtrar nulls
df_nulos.filter(col("nome").isNull()).show()
df_nulos.filter(col("nome").isNotNull()).show()

# Substituir nulls
from pyspark.sql.functions import coalesce, lit
df_nulos.fillna({"nome": "DESCONHECIDO", "valor": 0.0}).show()

# coalesce: retorna o primeiro valor não-nulo
df_nulos.withColumn(
    "nome_safe",
    coalesce(col("nome"), lit("DESCONHECIDO"))
).show()

# COMMAND ----------

# MAGIC %md
# ## 5. Utilitários de schema

# COMMAND ----------

# ── DDL string — forma alternativa de definir schema ─────────────────────
#
# Para schemas simples, a string DDL é mais concisa que StructType

schema_ddl = "id BIGINT NOT NULL, nome STRING, valor DECIMAL(18,2), criado_em TIMESTAMP"
df_ddl = spark.read.schema(schema_ddl).format("csv").option("header","true").load(caminho)
df_ddl.printSchema()

# COMMAND ----------

# ── Converter schema para JSON e de volta ────────────────────────────────
import json

# Schema → JSON (útil para salvar em repositório ou catálogo)
schema_json = schema_pedidos.json()
print(json.dumps(json.loads(schema_json), indent=2))

# JSON → Schema (útil para carregar schema salvo)
schema_recuperado = StructType.fromJson(json.loads(schema_json))
print(schema_recuperado == schema_pedidos)  # True

# COMMAND ----------

# ── Validar schema antes de escrever ─────────────────────────────────────

def validar_schema(df, schema_esperado: StructType) -> bool:
    """
    Compara o schema do DataFrame com o schema esperado.
    Retorna True se compatível, False caso contrário.
    """
    campos_df = {f.name: f.dataType for f in df.schema.fields}
    campos_esp = {f.name: f.dataType for f in schema_esperado.fields}

    erros = []

    # Colunas faltando
    for col_name in campos_esp:
        if col_name not in campos_df:
            erros.append(f"Coluna ausente: '{col_name}'")

    # Tipos incorretos
    for col_name, tipo_esp in campos_esp.items():
        if col_name in campos_df:
            tipo_df = campos_df[col_name]
            if tipo_df != tipo_esp:
                erros.append(
                    f"Tipo incorreto em '{col_name}': "
                    f"esperado {tipo_esp}, encontrado {tipo_df}"
                )

    if erros:
        for e in erros: print(f"❌ {e}")
        return False

    print("✅ Schema validado com sucesso")
    return True

# Uso:
# validar_schema(df_pedidos, schema_pedidos)

# COMMAND ----------

# MAGIC %md
# ## Resumo — o que fixar deste arquivo
#
# | Conceito | O que saber |
# |----------|-------------|
# | `StructType` | Container de `StructField`s — define o schema completo |
# | `StructField(nome, tipo, nullable)` | Define uma coluna: nome, tipo e se aceita null |
# | `nullable=False` | Declaração de intenção ao optimizer — não bloqueia nulls por si só |
# | Dinheiro | Sempre `DecimalType(18, 2)` — nunca Float ou Double |
# | CPF/CEP/código | Sempre `StringType()` — nunca inteiro (perde zeros à esquerda) |
# | Inferência em CSV | Lê duas vezes, infere tipos errados — evitar em produção |
# | Inferência em Parquet/Delta | Schema no footer — aceitável para exploração |
# | Arrays | `ArrayType(elementType, containsNull)` + `explode()` para desaninhar |
# | Maps | `MapType(keyType, valueType)` + `col("map")["chave"]` para acessar |
# | Struct aninhado | `StructType` dentro de `StructType` + `col("obj.campo")` |
# | DDL string | Alternativa concisa: `"id BIGINT, nome STRING"` |
#
# ### Próximo arquivo
# `03_transformacoes_basicas.py` — select, filter, withColumn, when/otherwise,
# coalesce, cast, alias — as transformações que você usa em 90% dos pipelines.
