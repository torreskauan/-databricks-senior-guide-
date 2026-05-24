# Databricks notebook source

# MAGIC %md
# # 11 — Funções de Coluna — Referência Completa
#
# > **Arquivo:** `02-pyspark-api/11_column_functions.py`
# > **Módulo:** 02 — PySpark API
# > **Dependência:** `03_transformacoes_basicas.py`
#
# ---
#
# ## Analogia
#
# `pyspark.sql.functions` é a caixa de ferramentas do Spark.
# Assim como um marceneiro tem serras, plainas e lixas para cada
# tipo de trabalho na madeira, você tem funções específicas para
# cada tipo de dado: strings, datas, arrays, structs e JSON.
#
# A regra mais importante: **sempre prefira funções nativas a UDFs**.
# Funções nativas rodam dentro da JVM no Tungsten — sem serialização,
# sem overhead de Python. Uma UDF Python serializa cada linha para
# o interpretador Python e volta. Para 100 milhões de linhas,
# a diferença pode ser de minutos para horas.
#
# ---
#
# ## Imports deste arquivo

# COMMAND ----------

from pyspark.sql.functions import (
    # ── String ──────────────────────────────────────────────────────────
    upper, lower, initcap, length, trim, ltrim, rtrim,
    lpad, rpad, repeat, reverse, ascii, chr,
    concat, concat_ws, format_string,
    substring, substring_index,
    instr, locate, position,
    replace, translate, overlay,
    regexp_replace, regexp_extract, regexp_extract_all,
    split, sentences,
    like, rlike,
    levenshtein, soundex,

    # ── Data e Tempo ────────────────────────────────────────────────────
    current_date, current_timestamp,
    date_add, date_sub, add_months, months_between,
    datediff, timestampdiff,
    year, quarter, month, weekofyear, dayofyear,
    dayofmonth, dayofweek, hour, minute, second,
    to_date, to_timestamp, unix_timestamp, from_unixtime,
    date_format, date_trunc, trunc,
    last_day, next_day,
    make_date, make_timestamp,

    # ── Array ────────────────────────────────────────────────────────────
    array, array_contains, array_distinct, array_except,
    array_intersect, array_join, array_max, array_min,
    array_position, array_remove, array_repeat, array_size,
    array_sort, array_union, arrays_overlap, arrays_zip,
    flatten, sequence, slice, sort_array, shuffle,
    explode, explode_outer, posexplode, posexplode_outer,
    collect_list, collect_set,

    # ── Struct e Map ─────────────────────────────────────────────────────
    struct, named_struct,
    map_keys, map_values, map_entries, map_from_arrays,
    map_contains_key, element_at, size,

    # ── JSON ────────────────────────────────────────────────────────────
    to_json, from_json, get_json_object, json_tuple, schema_of_json,

    # ── Regex ───────────────────────────────────────────────────────────
    # (já importados acima: regexp_replace, regexp_extract, regexp_extract_all)

    # ── Nulos e condicionais ─────────────────────────────────────────────
    col, lit, when, coalesce, nullif, ifnull, nvl, nvl2,
    isnull, isnotnull, nanvl,

    # ── Matemáticas ──────────────────────────────────────────────────────
    abs as spark_abs, ceil, floor, round as spark_round,
    greatest, least, pow, sqrt, log, log2, log10, exp,
    rand, randn, hash, xxhash64, md5, sha1, sha2, crc32,
)
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    ArrayType, MapType, DoubleType, IntegerType
)

# DataFrame de exemplo
dados = [
    Row(id=1, nome="  ana SILVA  ", cpf="12345678901", data_str="15/01/2024",
        ts_str="2024-01-15 10:30:45", tags='["spark","delta","python"]',
        payload='{"acao":"compra","valor":199.90,"itens":["a","b"]}',
        lista=[1, 2, 3, 2, 1], notas={"mat": 8.5, "fis": 7.0}),
    Row(id=2, nome="BRUNO costa",   cpf="98765432100", data_str="20/02/2024",
        ts_str="2024-02-20 14:15:00", tags='["databricks","unity"]',
        payload='{"acao":"devolucao","valor":89.00,"itens":["c"]}',
        lista=[4, 5, 6], notas={"mat": 9.0, "fis": None}),
    Row(id=3, nome=None,            cpf=None,          data_str=None,
        ts_str=None, tags=None,
        payload='{"acao":"consulta","valor":0}',
        lista=None, notas=None),
]

schema = StructType([
    StructField("id",       LongType(),                      False),
    StructField("nome",     StringType(),                    True),
    StructField("cpf",      StringType(),                    True),
    StructField("data_str", StringType(),                    True),
    StructField("ts_str",   StringType(),                    True),
    StructField("tags",     StringType(),                    True),
    StructField("payload",  StringType(),                    True),
    StructField("lista",    ArrayType(IntegerType(), True),  True),
    StructField("notas",    MapType(StringType(), DoubleType(), True), True),
])

df = spark.createDataFrame(dados, schema=schema)
df.cache()
df.count()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ## 1. Funções de String

# COMMAND ----------

# MAGIC %md
# ### 1.1 Transformação de case e espaços

# COMMAND ----------

from pyspark.sql.functions import upper, lower, initcap, trim, ltrim, rtrim, length

df.select(
    "nome",
    upper(col("nome")).alias("upper"),       # ANA SILVA
    lower(col("nome")).alias("lower"),       # ana silva
    initcap(col("nome")).alias("initcap"),   # Ana Silva
    trim(col("nome")).alias("trim"),         # remove espaços dos dois lados
    ltrim(col("nome")).alias("ltrim"),       # remove espaços à esquerda
    rtrim(col("nome")).alias("rtrim"),       # remove espaços à direita
    length(col("nome")).alias("len"),        # número de caracteres
    length(trim(col("nome"))).alias("len_trim"),
).show(truncate=False)

# COMMAND ----------

# ── Padrão de limpeza de nome em produção ─────────────────────────────────
from pyspark.sql.functions import regexp_replace

df_nome_limpo = df.withColumn(
    "nome_clean",
    initcap(
        trim(
            regexp_replace(col("nome"), r"\s+", " ")  # colapsa múltiplos espaços em 1
        )
    )
)
df_nome_limpo.select("nome", "nome_clean").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ### 1.2 Concatenação e formatação

# COMMAND ----------

from pyspark.sql.functions import concat, concat_ws, format_string, lpad, rpad

# concat: une sem separador
df.select(
    concat(col("id").cast("string"), lit("-"), col("nome")).alias("id_nome")
).show(truncate=False)

# concat_ws: une com separador — ignora nulls
df.select(
    concat_ws(" | ", col("id").cast("string"), col("nome"), col("cpf"))
    .alias("joined")
).show(truncate=False)

# format_string: sprintf-style
df.select(
    format_string("ID: %05d | Nome: %-20s", col("id"), col("nome"))
    .alias("formatado")
).show(truncate=False)

# lpad / rpad: padding para comprimento fixo
df.select(
    lpad(col("id").cast("string"), 8, "0").alias("id_padded"),  # 00000001
    rpad(col("nome"), 30, ".").alias("nome_padded"),             # ana SILVA...........
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ### 1.3 Extração e posicionamento

# COMMAND ----------

from pyspark.sql.functions import substring, substring_index, instr, locate

# substring(col, pos, len) — pos começa em 1 (não 0!)
df.select(
    col("cpf"),
    substring(col("cpf"), 1, 3).alias("primeiros_3"),   # 123
    substring(col("cpf"), -2, 2).alias("ultimos_2"),     # 01 (índice negativo)
).show()

# substring_index: extrai N partes separadas por delimitador
df_email = spark.createDataFrame([("joao.silva@empresa.com.br",)], ["email"])
df_email.select(
    col("email"),
    substring_index(col("email"), "@", 1).alias("usuario"),    # joao.silva
    substring_index(col("email"), "@", -1).alias("dominio"),   # empresa.com.br
    substring_index(
        substring_index(col("email"), "@", -1), ".", 1
    ).alias("empresa"),                                         # empresa
).show(truncate=False)

# instr: posição da primeira ocorrência (1-indexed, 0 se não encontrar)
df_email.select(
    instr(col("email"), "@").alias("pos_arroba"),         # 11
    instr(col("email"), "xyz").alias("nao_encontrado"),   # 0
).show()

# COMMAND ----------

# MAGIC %md
# ### 1.4 Substituição

# COMMAND ----------

from pyspark.sql.functions import replace, translate

# replace: substitui string literal
df.select(
    col("nome"),
    replace(col("nome"), "  ", " ").alias("espacos_simples"),
).show(truncate=False)

# translate: substitui caractere por caractere (como tr no Unix)
df.select(
    col("cpf"),
    translate(col("cpf"), "0123456789", "ABCDEFGHIJ").alias("cpf_encoded"),
    # 1→B, 2→C, 3→D, etc.
).show()

# COMMAND ----------

# MAGIC %md
# ### 1.5 Split

# COMMAND ----------

from pyspark.sql.functions import split

df_csv_col = spark.createDataFrame([
    ("SP;RJ;MG;ES",),
    ("RS;SC;PR",),
    ("BA",),
    (None,),
], ["ufs_str"])

df_csv_col.select(
    col("ufs_str"),
    split(col("ufs_str"), ";").alias("ufs_array"),          # ["SP","RJ","MG","ES"]
    split(col("ufs_str"), ";")[0].alias("primeira_uf"),     # "SP"
    split(col("ufs_str"), ";", 2).alias("max_2_partes"),    # ["SP","RJ;MG;ES"]
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ## 2. Funções de Data e Tempo

# COMMAND ----------

# MAGIC %md
# ### 2.1 Parsing — string para date/timestamp

# COMMAND ----------

from pyspark.sql.functions import to_date, to_timestamp, unix_timestamp, from_unixtime

# to_date: string → DateType
df.select(
    col("data_str"),
    to_date(col("data_str"), "dd/MM/yyyy").alias("data"),
    to_date(col("data_str"), "dd/MM/yyyy").cast("string").alias("data_iso"),
).show()

# to_timestamp: string → TimestampType
df.select(
    col("ts_str"),
    to_timestamp(col("ts_str"), "yyyy-MM-dd HH:mm:ss").alias("ts"),
).show()

# unix_timestamp: string → epoch seconds (Long)
# from_unixtime: epoch seconds → string formatada
df.select(
    unix_timestamp(col("ts_str"), "yyyy-MM-dd HH:mm:ss").alias("epoch"),
    from_unixtime(
        unix_timestamp(col("ts_str"), "yyyy-MM-dd HH:mm:ss")
    ).alias("de_volta"),
).show()

# COMMAND ----------

# ── Formatos de data mais usados no Brasil e em sistemas ─────────────────

formatos = [
    ("dd/MM/yyyy",            "15/01/2024",            "Brasil"),
    ("yyyy-MM-dd",            "2024-01-15",            "ISO 8601 / bancos"),
    ("yyyyMMdd",              "20240115",              "Compacto / nomes de arquivo"),
    ("yyyy-MM-dd HH:mm:ss",   "2024-01-15 10:30:00",  "Datetime sem TZ"),
    ("yyyy-MM-dd'T'HH:mm:ss", "2024-01-15T10:30:00",  "ISO 8601 datetime"),
    ("dd/MM/yyyy HH:mm",      "15/01/2024 10:30",      "Brasil com hora"),
    ("EEE, dd MMM yyyy",      "Mon, 15 Jan 2024",      "HTTP / RFC 1123"),
]

for fmt, exemplo, desc in formatos:
    df_test = spark.createDataFrame([(exemplo,)], ["s"])
    result = df_test.select(to_date(col("s"), fmt)).collect()[0][0]
    print(f"  {fmt:<30} '{exemplo}' → {result}  ({desc})")

# COMMAND ----------

# MAGIC %md
# ### 2.2 Extração de componentes

# COMMAND ----------

from pyspark.sql.functions import (
    year, quarter, month, weekofyear, dayofyear,
    dayofmonth, dayofweek, hour, minute, second,
    date_format
)

df_datas = spark.createDataFrame([
    ("2024-03-15 14:35:22",),
    ("2024-12-31 23:59:59",),
], ["ts_str"])

df_datas = df_datas.withColumn("ts", to_timestamp(col("ts_str"), "yyyy-MM-dd HH:mm:ss"))

df_datas.select(
    "ts",
    year("ts").alias("ano"),             # 2024
    quarter("ts").alias("trimestre"),    # 1, 2, 3 ou 4
    month("ts").alias("mes"),            # 3
    weekofyear("ts").alias("semana"),    # 11
    dayofyear("ts").alias("dia_ano"),    # 75
    dayofmonth("ts").alias("dia_mes"),   # 15
    dayofweek("ts").alias("dia_semana"), # 1=domingo, 2=segunda, ..., 7=sábado
    hour("ts").alias("hora"),            # 14
    minute("ts").alias("minuto"),        # 35
    second("ts").alias("segundo"),       # 22
    date_format("ts", "EEEE").alias("nome_dia"),     # Friday / Sexta-feira
    date_format("ts", "MMMM").alias("nome_mes"),     # March / Março
    date_format("ts", "yyyy-MM").alias("ano_mes"),   # 2024-03
    date_format("ts", "yyyy-'Q'Q").alias("ano_tri"), # 2024-Q1
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ### 2.3 Aritmética de datas

# COMMAND ----------

from pyspark.sql.functions import (
    date_add, date_sub, add_months, months_between,
    datediff, last_day, next_day, date_trunc, trunc, current_date
)

df_datas.select(
    "ts",
    date_add("ts", 30).alias("mais_30_dias"),
    date_sub("ts", 7).alias("menos_7_dias"),
    add_months("ts", 3).alias("mais_3_meses"),
    add_months("ts", -1).alias("mes_anterior"),
    last_day("ts").alias("ultimo_dia_mes"),
    next_day("ts", "MON").alias("proxima_segunda"),
    date_trunc("month", "ts").alias("inicio_mes"),       # 2024-03-01 00:00:00
    date_trunc("year",  "ts").alias("inicio_ano"),       # 2024-01-01 00:00:00
    date_trunc("hour",  "ts").alias("inicio_hora"),      # 2024-03-15 14:00:00
    trunc("ts", "MM").alias("trunc_mes"),                # 2024-03-01 (DateType)
).show(truncate=False)

# datediff: diferença em dias
df_diff = spark.createDataFrame([("2024-01-01", "2024-03-15")], ["inicio", "fim"])
df_diff.select(
    datediff(
        to_date(col("fim"),    "yyyy-MM-dd"),
        to_date(col("inicio"), "yyyy-MM-dd")
    ).alias("dias"),                             # 74
    months_between(
        to_date(col("fim"),    "yyyy-MM-dd"),
        to_date(col("inicio"), "yyyy-MM-dd")
    ).alias("meses"),                            # 2.45...
).show()

# COMMAND ----------

# MAGIC %md
# ## 3. Funções de Array

# COMMAND ----------

# MAGIC %md
# ### 3.1 Criação e inspeção

# COMMAND ----------

from pyspark.sql.functions import (
    array, array_size, size, array_contains,
    array_min, array_max, array_distinct, array_sort, sort_array,
    array_position, element_at
)

df_arr = df.filter(col("lista").isNotNull())

df_arr.select(
    "lista",
    size("lista").alias("tamanho"),                    # 3
    array_size("lista").alias("tamanho2"),             # igual ao size
    array_min("lista").alias("minimo"),                # 1
    array_max("lista").alias("maximo"),                # 3
    array_distinct("lista").alias("sem_duplicatas"),   # [1, 2, 3]
    array_sort("lista").alias("ordenado"),             # [1, 1, 2, 2, 3]
    sort_array("lista", asc=False).alias("desc"),      # [3, 2, 2, 1, 1]
    array_contains("lista", 3).alias("tem_3"),         # true
    array_position("lista", 2).alias("pos_2"),         # 2 (1-indexed, 0 se não achar)
    element_at("lista", 1).alias("primeiro"),          # 1 (1-indexed)
    element_at("lista", -1).alias("ultimo"),           # 1 (índice negativo)
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ### 3.2 Operações de conjunto

# COMMAND ----------

from pyspark.sql.functions import (
    array_union, array_intersect, array_except,
    arrays_overlap, flatten, slice, array_repeat,
    array_remove, array_join
)

df_sets = spark.createDataFrame([
    ([1, 2, 3, 4], [3, 4, 5, 6]),
    ([10, 20],     [30, 40]),
], ["a", "b"])

df_sets.select(
    "a", "b",
    array_union("a", "b").alias("uniao"),            # [1,2,3,4,5,6]
    array_intersect("a", "b").alias("intersecao"),   # [3,4]
    array_except("a", "b").alias("diferenca"),       # [1,2] (a - b)
    arrays_overlap("a", "b").alias("tem_overlap"),   # true
).show(truncate=False)

# Outras operações
df_arr.select(
    "lista",
    flatten(array("lista", array(lit(10), lit(11)))).alias("achatado"),
    slice("lista", 2, 2).alias("fatia"),          # do índice 2, 2 elementos
    array_remove("lista", 2).alias("sem_2"),      # remove todas as ocorrências de 2
    array_repeat(lit("x"), 3).alias("repetido"),  # ["x","x","x"]
    array_join("lista".cast("array<string>"), ", ").alias("joined"),
    # ↑ array de inteiros precisa de cast para string primeiro
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ### 3.3 Explode — desaninhando arrays

# COMMAND ----------

from pyspark.sql.functions import explode, explode_outer, posexplode, posexplode_outer

df_tags = spark.createDataFrame([
    (1, ["spark", "delta"]),
    (2, ["databricks"]),
    (3, []),          # array vazio
    (4, None),        # null
], ["id", "tags"])

# explode: uma linha por elemento — pula arrays vazios e nulls
df_tags.select("id", explode("tags").alias("tag")).show()
# id=3 (vazio) e id=4 (null) não aparecem

# explode_outer: preserva linhas com array vazio/null como null
df_tags.select("id", explode_outer("tags").alias("tag")).show()
# id=3 → tag=null
# id=4 → tag=null

# posexplode: inclui índice de posição
df_tags.select("id", posexplode("tags").alias("pos", "tag")).show()
# pos: 0, 1, 2...

# posexplode_outer: posexplode preservando vazios/nulls
df_tags.select("id", posexplode_outer("tags").alias("pos", "tag")).show()

# COMMAND ----------

# MAGIC %md
# ### 3.4 collect_list e collect_set — inverso do explode

# COMMAND ----------

from pyspark.sql.functions import collect_list, collect_set, array_sort

# collect_list: agrega linhas em array (preserva duplicatas e ordem)
# collect_set:  agrega linhas em array (remove duplicatas, ordem indefinida)

df_pedidos = spark.createDataFrame([
    (1, "SP", "spark"),
    (1, "SP", "delta"),
    (1, "SP", "spark"),     # duplicata
    (2, "RJ", "databricks"),
    (2, "RJ", "databricks"), # duplicata
], ["id", "uf", "tag"])

df_pedidos.groupBy("id", "uf").agg(
    collect_list("tag").alias("tags_lista"),    # ["spark","delta","spark"]
    collect_set("tag").alias("tags_set"),       # ["delta","spark"] (sem ordem garantida)
    array_sort(collect_set("tag")).alias("tags_sorted"),  # ["delta","spark"] ordenado
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ## 4. Funções de Map e Struct

# COMMAND ----------

from pyspark.sql.functions import (
    map_keys, map_values, map_entries, map_from_arrays,
    map_contains_key, element_at, size
)

df_map = df.filter(col("notas").isNotNull())

df_map.select(
    "notas",
    map_keys("notas").alias("materias"),          # ["mat","fis"]
    map_values("notas").alias("valores"),         # [8.5, 7.0]
    map_entries("notas").alias("pares"),          # [{key:mat,value:8.5},{key:fis,value:7.0}]
    size("notas").alias("qtd_itens"),             # 2
    col("notas")["mat"].alias("nota_mat"),        # 8.5
    element_at("notas", "fis").alias("nota_fis"), # 7.0
    map_contains_key("notas", "bio").alias("tem_bio"), # false
).show(truncate=False)

# COMMAND ----------

# ── map_from_arrays — criar Map a partir de dois arrays ───────────────────

df_kv = spark.createDataFrame([
    (["a", "b", "c"], [1, 2, 3]),
    (["x", "y"],      [10, 20]),
], ["keys", "values"])

df_kv.select(
    map_from_arrays("keys", "values").alias("mapa")
).show(truncate=False)

# COMMAND ----------

# ── Struct — agrupar campos relacionados ──────────────────────────────────

from pyspark.sql.functions import struct, named_struct

df_struct = df.select(
    "id",
    struct("nome", "cpf").alias("identificacao"),
    named_struct(
        lit("cidade"), lit("São Paulo"),
        lit("uf"),     lit("SP"),
        lit("cep"),    lit("01310100")
    ).alias("endereco"),
)
df_struct.show(truncate=False)
df_struct.printSchema()

# Acessar campos do struct
df_struct.select(
    "id",
    col("identificacao.nome").alias("nome"),
    col("endereco.cidade").alias("cidade"),
).show()

# COMMAND ----------

# MAGIC %md
# ## 5. Funções de JSON

# COMMAND ----------

# MAGIC %md
# ### 5.1 get_json_object — extrair valor por path

# COMMAND ----------

from pyspark.sql.functions import get_json_object, json_tuple

# get_json_object: extrai UM campo por JSONPath — retorna StringType
df.select(
    "payload",
    get_json_object("payload", "$.acao").alias("acao"),
    get_json_object("payload", "$.valor").alias("valor_str"),
    get_json_object("payload", "$.itens[0]").alias("primeiro_item"),
    get_json_object("payload", "$.itens").alias("itens_json"),
).show(truncate=False)

# COMMAND ----------

# json_tuple: extrai múltiplos campos em uma chamada (mais eficiente que múltiplos get_json_object)
df.select(
    "payload",
    *[col(c) for c in ["acao", "valor"]],  # placeholder
).show(2)

df.select(
    "payload",
    json_tuple("payload", "acao", "valor").alias("acao", "valor_str"),
).show()

# COMMAND ----------

# MAGIC %md
# ### 5.2 from_json — string JSON para Struct tipado

# COMMAND ----------

from pyspark.sql.functions import from_json, schema_of_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

# Definir schema do JSON (recomendado em produção)
schema_payload = StructType([
    StructField("acao",   StringType(),                     True),
    StructField("valor",  DoubleType(),                     True),
    StructField("itens",  ArrayType(StringType(), True),    True),
])

df_parsed = df.withColumn(
    "payload_struct",
    from_json(col("payload"), schema_payload)
)
df_parsed.select(
    "id",
    "payload_struct.acao",
    "payload_struct.valor",
    "payload_struct.itens",
).show(truncate=False)

# COMMAND ----------

# ── schema_of_json — inferir schema a partir de amostra ──────────────────

amostra = '{"acao":"compra","valor":199.90,"itens":["a","b"],"cliente":{"id":1,"nome":"Ana"}}'
schema_inferido = schema_of_json(lit(amostra))

# Usar o schema inferido
df.select(from_json("payload", schema_of_json(lit(amostra))).alias("p")).printSchema()

# COMMAND ----------

# MAGIC %md
# ### 5.3 to_json — Struct/Array para string JSON

# COMMAND ----------

from pyspark.sql.functions import to_json

# Converter struct para JSON string
df_parsed.select(
    "id",
    to_json("payload_struct").alias("payload_de_volta"),
).show(truncate=False)

# Converter array para JSON string
df_arr.select(
    "id",
    to_json(col("lista")).alias("lista_json"),  # [1,2,3,2,1]
).show()

# COMMAND ----------

# MAGIC %md
# ## 6. Regex — Expressões Regulares

# COMMAND ----------

# MAGIC %md
# ### 6.1 regexp_replace — substituição por padrão

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

df_regex = spark.createDataFrame([
    ("(11) 98765-4321",),
    ("11987654321",),
    ("(11)98765-4321",),
    ("+55 11 9 8765-4321",),
], ["telefone"])

df_regex.select(
    col("telefone"),
    # Remover tudo que não é dígito
    regexp_replace(col("telefone"), r"[^\d]", "").alias("so_digitos"),
    # Formatar CPF: 12345678901 → 123.456.789-01
    regexp_replace(
        col("telefone"), r"(\d{3})(\d{3})(\d{3})(\d{2})", r"$1.$2.$3-$4"
    ).alias("cpf_fmt"),
).show(truncate=False)

# Casos comuns de regexp_replace
exemplos_replace = [
    ("Remover acentos (simplificado)",
     r"[àáâãä]", "a"),
    ("Remover HTML tags",
     r"<[^>]+>", ""),
    ("Normalizar espaços múltiplos",
     r"\s+", " "),
    ("Remover caracteres especiais",
     r"[^a-zA-Z0-9\s]", ""),
    ("Mascarar email",
     r"(?<=.{2}).(?=[^@]*@)", "*"),
]

for desc, pattern, repl in exemplos_replace:
    print(f"  {desc}")
    print(f"    pattern: {pattern!r}  →  replace: {repl!r}")

# COMMAND ----------

# MAGIC %md
# ### 6.2 regexp_extract — capturar grupos

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, regexp_extract_all

df_log = spark.createDataFrame([
    ("2024-03-15 10:30:45 ERROR NullPointerException in Pipeline.scala:42",),
    ("2024-03-15 10:31:02 WARN  Retrying connection attempt 3/5",),
    ("2024-03-15 10:31:15 INFO  Job completed: 15234 rows processed",),
], ["log_line"])

df_log.select(
    "log_line",
    # Grupo 1: data, Grupo 2: hora, Grupo 3: nível, Grupo 4: mensagem
    regexp_extract(col("log_line"), r"(\d{4}-\d{2}-\d{2})", 1).alias("data"),
    regexp_extract(col("log_line"), r"\d{4}-\d{2}-\d{2} (\d{2}:\d{2}:\d{2})", 1).alias("hora"),
    regexp_extract(col("log_line"), r"\s(ERROR|WARN|INFO)\s", 1).alias("nivel"),
    regexp_extract(col("log_line"), r"(\d+) rows processed", 1).alias("qtd_rows"),
).show(truncate=False)

# COMMAND ----------

# regexp_extract_all: retorna TODOS os matches como array (Spark 3.1+)
df_texto = spark.createDataFrame([
    ("Valores: R$ 100,00 e R$ 250,50 e R$ 1.500,00",),
    ("Sem valores monetários aqui",),
], ["texto"])

df_texto.select(
    "texto",
    regexp_extract_all(
        col("texto"),
        r"R\$\s*[\d.,]+",  # captura todos os valores R$
        0                   # grupo 0 = match completo
    ).alias("valores_encontrados"),
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ### 6.3 Padrões regex mais usados em dados

# COMMAND ----------

padroes_uteis = [
    ("CPF (formato limpo)",     r"^\d{11}$"),
    ("CPF (formatado)",         r"^\d{3}\.\d{3}\.\d{3}-\d{2}$"),
    ("CNPJ (formato limpo)",    r"^\d{14}$"),
    ("CEP (formato limpo)",     r"^\d{8}$"),
    ("CEP (formatado)",         r"^\d{5}-\d{3}$"),
    ("Email básico",            r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"),
    ("Telefone BR (com DDD)",   r"^\(?\d{2}\)?\s?9?\d{4}[-\s]?\d{4}$"),
    ("Data DD/MM/YYYY",         r"^\d{2}/\d{2}/\d{4}$"),
    ("Apenas letras e espaços", r"^[a-zA-ZÀ-ÿ\s]+$"),
    ("Apenas dígitos",          r"^\d+$"),
    ("Número decimal",          r"^-?\d+([.,]\d+)?$"),
]

# Usar regexp_extract para validar: retorna "" se não bater
from pyspark.sql.functions import rlike

df_cpfs = spark.createDataFrame([
    ("12345678901",),
    ("123.456.789-01",),
    ("1234567890",),   # curto demais
    (None,),
], ["cpf"])

df_cpfs.select(
    "cpf",
    col("cpf").rlike(r"^\d{11}$").alias("cpf_valido"),
).show()

# COMMAND ----------

# MAGIC %md
# ## 7. Funções de nulo e condicional

# COMMAND ----------

from pyspark.sql.functions import coalesce, nullif, ifnull, nvl, nvl2, nanvl

df_nulos = spark.createDataFrame([
    (1, None, 0.0, float("nan")),
    (2, "valor", None, 1.5),
    (3, "", 99.0, float("nan")),
], ["id", "texto", "numero", "com_nan"])

df_nulos.select(
    "id", "texto", "numero", "com_nan",
    # coalesce: primeiro não-nulo
    coalesce(col("texto"), lit("fallback")).alias("coalesce"),

    # nullif: retorna null se os dois valores forem iguais
    nullif(col("texto"), lit("")).alias("nullif_vazio"),  # "" → null

    # ifnull / nvl: equivalentes — retorna 2º arg se 1º for null
    ifnull(col("texto"), lit("N/A")).alias("ifnull"),
    nvl(col("numero"), lit(-1.0)).alias("nvl"),

    # nvl2: se 1º não-null → 2º arg; se null → 3º arg
    nvl2(col("texto"), lit("TEM VALOR"), lit("É NULO")).alias("nvl2"),

    # nanvl: se é NaN → retorna 2º arg
    nanvl(col("com_nan"), lit(0.0)).alias("nanvl"),
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ## 8. Referência rápida por categoria

# COMMAND ----------

referencia = {
    "String — transformação": [
        "upper/lower/initcap  — case",
        "trim/ltrim/rtrim     — espaços",
        "length               — comprimento",
        "lpad/rpad            — padding",
        "concat/concat_ws     — concatenação",
        "format_string        — sprintf-style",
    ],
    "String — extração": [
        "substring(col,pos,len)   — pos começa em 1",
        "substring_index(col,d,n) — N partes pelo delimitador",
        "instr(col,str)           — posição (0 se não achar)",
        "split(col,pattern)       — retorna ArrayType",
    ],
    "String — substituição/padrão": [
        "replace(col,old,new)          — literal",
        "regexp_replace(col,pat,repl)  — regex",
        "regexp_extract(col,pat,grp)   — captura grupo",
        "regexp_extract_all(col,pat,0) — todos os matches",
    ],
    "Data — parsing": [
        "to_date(col, formato)      — string → DateType",
        "to_timestamp(col, formato) — string → TimestampType",
        "unix_timestamp(col, fmt)   — string → epoch Long",
        "from_unixtime(col, fmt)    — epoch Long → string",
    ],
    "Data — extração": [
        "year/quarter/month/weekofyear/dayofyear",
        "dayofmonth/dayofweek/hour/minute/second",
        "date_format(col, 'yyyy-MM-dd') — formato livre",
    ],
    "Data — aritmética": [
        "date_add/date_sub        — dias",
        "add_months               — meses",
        "datediff                 — diferença em dias",
        "months_between           — diferença em meses (decimal)",
        "date_trunc/trunc         — truncar para granularidade",
        "last_day/next_day        — limites de período",
    ],
    "Array": [
        "explode/explode_outer   — uma linha por elemento",
        "posexplode              — com índice de posição",
        "collect_list/collect_set — linhas → array",
        "array_size/size         — tamanho",
        "array_contains          — membership test",
        "array_distinct/sort     — dedup e ordenação",
        "array_union/intersect/except — operações de conjunto",
        "flatten                 — achata array aninhado",
        "element_at(col, n)      — acesso por índice (1-indexed)",
    ],
    "JSON": [
        "get_json_object(col, '$.campo') — extrai 1 campo",
        "json_tuple(col, 'a','b','c')    — extrai N campos",
        "from_json(col, schema)          — string → Struct",
        "to_json(col)                    — Struct/Array → string",
        "schema_of_json(lit(amostra))    — infere schema",
    ],
}

for categoria, funcs in referencia.items():
    print(f"\n── {categoria} ──────────────────────────")
    for f in funcs:
        print(f"  {f}")

# COMMAND ----------

# MAGIC %md
# ## Resumo — o que fixar deste arquivo
#
# | Conceito | O que saber |
# |----------|-------------|
# | Funções nativas vs UDF | Sempre nativas — sem serialização, Tungsten as otimiza |
# | substring pos | Começa em **1** (não 0) — difere do Python |
# | split retorna | ArrayType — acesse com [0], [1] ou explode |
# | dayofweek | 1 = Domingo, 2 = Segunda, ..., 7 = Sábado |
# | explode vs explode_outer | outer preserva arrays vazios/nulls como linha null |
# | collect_list vs collect_set | list preserva duplicatas; set remove (sem ordem garantida) |
# | get_json_object | Retorna sempre StringType — faça cast se precisar de número |
# | from_json | Precisa de schema explícito — use schema_of_json para inferir de amostra |
# | regexp_extract | Retorna "" se não bater (não null) — cuidado ao checar resultado |
# | nullif | Converte valor específico em null — útil para strings vazias |
#
# ### Próximo arquivo
# `12_streaming_basico.py` — readStream, writeStream, triggers, checkpoints
# e os três outputModes que mudam completamente o comportamento do stream.
