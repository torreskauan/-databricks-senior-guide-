# Databricks notebook source

# MAGIC %md
# # 08 — Leitura e Escrita de Dados
#
# > **Arquivo:** `02-pyspark-api/08_leitura_escrita.py`
# > **Módulo:** 02 — PySpark API
# > **Dependência:** `02_schema_types.py`
#
# ---
#
# ## Analogia
#
# O Spark é um processador universal de dados, mas ele precisa
# de "adaptadores" para falar com cada formato de arquivo.
# Pensa como um carregador universal de tomada:
# o carregador (Spark) é sempre o mesmo, você só troca o plug
# (format + options) dependendo do país (formato de dado).
#
# A API é sempre a mesma estrutura:
#
# ```
# spark.read                        spark.write (ou df.write)
#   .format("delta")                  .format("delta")
#   .option("chave", "valor")         .option("chave", "valor")
#   .schema(meu_schema)               .mode("append")
#   .load("/caminho")                 .partitionBy("ano", "mes")
#                                     .save("/caminho")
# ```
#
# Saber essa estrutura resolve 90% dos problemas de I/O.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType,
    DecimalType, TimestampType, DateType, DoubleType, BooleanType
)
from pyspark.sql.functions import col, current_timestamp, lit, year, month

# Schema reutilizado nos exemplos deste arquivo
schema_pedidos = StructType([
    StructField("id",          LongType(),         False),
    StructField("id_cliente",  LongType(),         True),
    StructField("valor",       DecimalType(18, 2), True),
    StructField("status",      StringType(),       True),
    StructField("regiao",      StringType(),       True),
    StructField("criado_em",   TimestampType(),    True),
    StructField("data_pedido", DateType(),         True),
])

# COMMAND ----------

# MAGIC %md
# ## 1. A estrutura universal de leitura e escrita

# COMMAND ----------

# ── DataFrameReader — estrutura completa ─────────────────────────────────
#
# spark.read
#   .format(str)          → formato: "delta", "parquet", "csv", "json", "jdbc"
#   .option(k, v)         → opção específica do formato
#   .options(**kwargs)    → múltiplas opções de uma vez
#   .schema(schema)       → schema explícito (evita inferência)
#   .load(path)           → carrega do path (arquivos)
#   .table(nome)          → carrega de tabela registrada no catálogo
#   .csv(path)            → atalho: .format("csv").load(path)
#   .json(path)           → atalho: .format("json").load(path)
#   .parquet(path)        → atalho: .format("parquet").load(path)

# ── DataFrameWriter — estrutura completa ─────────────────────────────────
#
# df.write
#   .format(str)          → formato de saída
#   .option(k, v)         → opção específica do formato
#   .mode(str)            → "append" | "overwrite" | "ignore" | "error"
#   .partitionBy(*cols)   → particionar por colunas
#   .sortBy(*cols)        → ordenar dentro de cada partição (raro)
#   .bucketBy(n, *cols)   → bucketing (para joins recorrentes)
#   .save(path)           → escreve no path
#   .saveAsTable(nome)    → escreve e registra no catálogo

# COMMAND ----------

# MAGIC %md
# ### Write modes — o que cada um faz

# COMMAND ----------

# ── Os 4 modos de escrita ────────────────────────────────────────────────

modos = {
    "append":    "Adiciona dados sem verificar duplicatas. Seguro para ingestão incremental.",
    "overwrite": "Recria a tabela/arquivo do zero. CUIDADO: apaga tudo antes de escrever.",
    "ignore":    "Escreve apenas se o destino NÃO existe. Idempotente — falha silenciosa.",
    "error":     "Padrão. Lança erro se o destino já existe. Força decisão explícita.",
}
for modo, desc in modos.items():
    print(f"  {modo:<12} → {desc}")

# COMMAND ----------

# MAGIC %md
# ## 2. Parquet — o formato de analytics

# COMMAND ----------

# MAGIC %md
# ### Analogia
# Parquet é como organizar uma biblioteca por assunto — todos os livros
# de matemática juntos, todos de história juntos. Se você quer só os
# livros de matemática (filtrar por coluna), você vai direto à seção
# e não precisa varrer o acervo inteiro.
# É um formato colunar: armazena coluna por coluna, não linha por linha.

# COMMAND ----------

# ── Por que Parquet é o formato padrão para analytics ────────────────────
#
# ✅ Colunar: lê apenas as colunas necessárias (projection pushdown)
# ✅ Comprimido: Snappy por padrão (~50-75% menor que CSV)
# ✅ Schema embutido: tipo de cada coluna salvo nos metadados do arquivo
# ✅ Estatísticas por coluna: min, max, null count — permite data skipping
# ✅ Splittable: múltiplos executors leem partes diferentes em paralelo
#
# ❌ Não é legível por humanos (binário)
# ❌ Não é editável — sempre reescrito inteiro
# ❌ Ferramentas simples (Excel, Notepad) não abrem

# COMMAND ----------

# ── Leitura de Parquet ─────────────────────────────────────────────────

# Leitura básica — schema inferido do footer do arquivo
df_parquet = spark.read.format("parquet").load("/mnt/raw/pedidos/")

# Com schema explícito — recomendado em produção
df_parquet = (
    spark.read
    .format("parquet")
    .schema(schema_pedidos)
    .load("/mnt/raw/pedidos/")
)

# Atalho equivalente
df_parquet = spark.read.parquet("/mnt/raw/pedidos/")

# Lendo múltiplos paths
df_multi = spark.read.parquet(
    "/mnt/raw/pedidos/ano=2023/",
    "/mnt/raw/pedidos/ano=2024/",
)

# Lendo com glob pattern
df_glob = spark.read.parquet("/mnt/raw/pedidos/ano=202*/")

# COMMAND ----------

# ── Opções úteis de Parquet ───────────────────────────────────────────────

df_merge = (
    spark.read
    .format("parquet")
    .option("mergeSchema", "true")
    # true: unifica schemas de múltiplos arquivos com colunas diferentes
    # útil quando os arquivos evoluíram ao longo do tempo
    # false (padrão): usa o schema do primeiro arquivo encontrado
    .load("/mnt/raw/pedidos/")
)

# COMMAND ----------

# ── Escrita de Parquet ────────────────────────────────────────────────────

# Escrita simples
df.write.format("parquet").mode("overwrite").save("/mnt/silver/pedidos/")

# Com compressão explícita (Snappy é o padrão — outros: gzip, lz4, zstd)
df.write.format("parquet").option("compression", "snappy").mode("overwrite").save("/mnt/silver/pedidos/")

# Com particionamento — cria subdiretórios ano=2024/mes=1/
df.write.format("parquet") \
    .partitionBy("ano", "mes") \
    .mode("overwrite") \
    .save("/mnt/silver/pedidos/")

# COMMAND ----------

# ── Predicate pushdown e partition pruning ───────────────────────────────

# Partition pruning: lê apenas as pastas relevantes
df_2024 = spark.read.parquet("/mnt/silver/pedidos/").filter(col("ano") == 2024)
# Spark lê apenas /mnt/silver/pedidos/ano=2024/ — ignora outros anos

# Predicate pushdown: filter aplicado antes de carregar dados na memória
df_sp = spark.read.parquet("/mnt/silver/pedidos/").filter(col("regiao") == "Sudeste")
# Spark usa as estatísticas do footer do Parquet para pular row groups

# Para ver se pushdown está ocorrendo:
df_2024.explain()
# Procure "PushedFilters" no plano

# COMMAND ----------

# MAGIC %md
# ## 3. Delta Lake — o formato de produção

# COMMAND ----------

# MAGIC %md
# ### Analogia
# Delta é Parquet com superpoderes: é como adicionar um histórico de
# transações bancário ao arquivo. Cada operação gera um recibo no
# `_delta_log/`. Se algo der errado, você consulta o extrato e volta
# ao estado anterior — coisa que o Parquet puro não consegue fazer.

# COMMAND ----------

# ── Leitura de Delta ──────────────────────────────────────────────────────

# Por path
df_delta = spark.read.format("delta").load("/mnt/silver/pedidos/")

# Por tabela registrada no Unity Catalog (recomendado)
df_delta = spark.read.table("prod.silver.pedidos")

# Por nome com spark.sql
df_delta = spark.sql("SELECT * FROM prod.silver.pedidos")

# Time Travel — versão anterior
df_v5    = spark.read.format("delta").option("versionAsOf", 5).load("/mnt/silver/pedidos/")
df_ontem = spark.read.format("delta").option("timestampAsOf", "2024-01-14").load("/mnt/silver/pedidos/")

# Change Data Feed — só as mudanças entre versões
df_changes = (
    spark.read
    .format("delta")
    .option("readChangeFeed",  "true")
    .option("startingVersion", "10")
    .option("endingVersion",   "20")
    .table("prod.silver.pedidos")
)
# Coluna extra: _change_type = "insert" | "update_preimage" | "update_postimage" | "delete"

# COMMAND ----------

# ── Escrita de Delta ──────────────────────────────────────────────────────

# Append — ingestão incremental
df.write.format("delta").mode("append").save("/mnt/silver/pedidos/")

# Overwrite completo
df.write.format("delta").mode("overwrite").save("/mnt/silver/pedidos/")

# Salvar como tabela gerenciada no Unity Catalog
df.write.format("delta").mode("append").saveAsTable("prod.silver.pedidos")

# Overwrite de partição específica (não o table inteiro)
df_jan = df.filter(col("ano") == 2024).filter(col("mes") == 1)
(df_jan.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", "ano = 2024 AND mes = 1")
    .save("/mnt/silver/pedidos/")
)

# COMMAND ----------

# ── Opções importantes de escrita Delta ──────────────────────────────────

# mergeSchema: aceita novas colunas (schema evolution)
df_novo.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable("prod.silver.pedidos")

# overwriteSchema: troca o schema completamente (use com cuidado)
df_novo.write.format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("prod.silver.pedidos")

# optimizeWrite: Databricks agrupa writes em arquivos maiores
df.write.format("delta") \
    .option("optimizeWrite", "true") \
    .mode("append") \
    .saveAsTable("prod.silver.pedidos")

# COMMAND ----------

# ── MERGE INTO — upsert com DeltaTable ───────────────────────────────────

from delta.tables import DeltaTable

dt_destino = DeltaTable.forName(spark, "prod.silver.pedidos")

(dt_destino.alias("alvo")
    .merge(
        df_novos.alias("fonte"),
        "alvo.id = fonte.id"
    )
    .whenMatchedUpdate(set={
        "valor":      "fonte.valor",
        "status":     "fonte.status",
        "updated_at": "current_timestamp()"
    })
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

# MAGIC %md
# ## 4. CSV — leitura e escrita robusta

# COMMAND ----------

# MAGIC %md
# ### Analogia
# CSV é como um bilhete escrito à mão — qualquer um consegue ler,
# mas não tem garantia de formato. Dois bilhetes podem usar vírgula
# ou ponto-e-vírgula como separador, datas em formatos diferentes,
# encoding diferente. Você precisa dizer ao Spark exatamente como
# interpretar cada detalhe — nada é inferido corretamente por padrão.

# COMMAND ----------

# ── Leitura de CSV — opções essenciais ───────────────────────────────────

df_csv = (
    spark.read
    .format("csv")
    .option("header",           "true")   # primeira linha = nomes de colunas
    .option("sep",              ";")      # separador (padrão: ",")
    .option("encoding",         "UTF-8")  # encoding do arquivo
    .option("quote",            '"')      # caractere de aspas
    .option("escape",           "\\")     # caractere de escape
    .option("nullValue",        "NULL")   # string que representa null
    .option("emptyValue",       "")       # string vazia vira null ou string?
    .option("dateFormat",       "dd/MM/yyyy")       # formato de datas
    .option("timestampFormat",  "dd/MM/yyyy HH:mm:ss")
    .option("multiLine",        "false")  # true se campos contêm quebras de linha
    .option("ignoreLeadingWhiteSpace",  "true")
    .option("ignoreTrailingWhiteSpace", "true")
    .schema(schema_pedidos)               # SEMPRE em produção
    .load("/mnt/raw/pedidos/*.csv")
)

# COMMAND ----------

# ── Opções avançadas de CSV ───────────────────────────────────────────────

# mode: o que fazer com linhas malformadas
df_robusto = (
    spark.read
    .format("csv")
    .option("header",    "true")
    .option("sep",       ";")
    .option("mode",      "PERMISSIVE")   # padrão: linha malformada → null nas colunas
    #                   "DROPMALFORMED"  # descarta linha malformada
    #                   "FAILFAST"       # lança erro na primeira linha malformada
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    # Com PERMISSIVE: linhas malformadas são capturadas nesta coluna
    .schema(schema_pedidos.add("_corrupt_record", StringType(), True))
    .load("/mnt/raw/pedidos/*.csv")
)

# Inspecionar linhas malformadas
df_robusto.filter(col("_corrupt_record").isNotNull()).show(truncate=False)

# COMMAND ----------

# ── Escrita de CSV ────────────────────────────────────────────────────────

(df.write
    .format("csv")
    .option("header",          "true")
    .option("sep",             ";")
    .option("encoding",        "UTF-8")
    .option("dateFormat",      "dd/MM/yyyy")
    .option("timestampFormat", "dd/MM/yyyy HH:mm:ss")
    .option("nullValue",       "NULL")
    .mode("overwrite")
    .save("/mnt/output/pedidos_csv/")
)

# ── Escrever CSV como arquivo único (cuidado com dados grandes) ───────────
# coalesce(1) força tudo em uma única partição → um único arquivo de saída
(df.coalesce(1)
    .write
    .format("csv")
    .option("header", "true")
    .option("sep",    ";")
    .mode("overwrite")
    .save("/mnt/output/pedidos_unico/")
)
# ⚠️ Só use coalesce(1) para arquivos pequenos — coleta tudo no driver

# COMMAND ----------

# MAGIC %md
# ## 5. JSON — leitura e escrita

# COMMAND ----------

# ── Leitura de JSON ───────────────────────────────────────────────────────

# JSON lines (um objeto JSON por linha — o mais comum em pipelines)
df_json = (
    spark.read
    .format("json")
    .option("multiLine",        "false")  # false = JSON lines (padrão)
    #                           "true"   = arquivo JSON com array ou objeto multi-linha
    .option("allowComments",    "true")   # aceita comentários //
    .option("allowUnquotedFieldNames", "false")
    .option("dateFormat",       "yyyy-MM-dd")
    .option("timestampFormat",  "yyyy-MM-dd'T'HH:mm:ss")
    .option("mode",             "PERMISSIVE")
    .schema(schema_pedidos)
    .load("/mnt/raw/pedidos/*.json")
)

# COMMAND ----------

# ── JSON com struct aninhado ───────────────────────────────────────────────

# JSON com estrutura aninhada — inferência é aceitável para exploração
df_nested = spark.read.json("/mnt/raw/eventos/")
df_nested.printSchema()
# root
#  |-- id: long (nullable = true)
#  |-- payload: struct (nullable = true)
#  |    |-- acao: string (nullable = true)
#  |    |-- dados: struct (nullable = true)
#  |    |    |-- produto_id: long (nullable = true)
#  |    |    |-- valor: double (nullable = true)

# Acessar campos aninhados
df_nested.select(
    "id",
    col("payload.acao").alias("acao"),
    col("payload.dados.produto_id").alias("produto_id"),
    col("payload.dados.valor").alias("valor")
).show()

# COMMAND ----------

# ── Escrita JSON ──────────────────────────────────────────────────────────

(df.write
    .format("json")
    .option("dateFormat",      "yyyy-MM-dd")
    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
    .mode("overwrite")
    .save("/mnt/output/pedidos_json/")
)

# COMMAND ----------

# ── Converter coluna string JSON em struct (from_json) ────────────────────

from pyspark.sql.functions import from_json, to_json, schema_of_json

# Schema do JSON dentro da coluna
schema_payload = StructType([
    StructField("acao",       StringType(), True),
    StructField("produto_id", LongType(),   True),
    StructField("valor",      DoubleType(), True),
])

df_com_json_col = spark.createDataFrame([
    (1, '{"acao":"compra","produto_id":42,"valor":199.90}'),
    (2, '{"acao":"devolucao","produto_id":15,"valor":89.00}'),
], ["id", "payload_str"])

# Parsear coluna JSON
df_parsed = df_com_json_col.withColumn(
    "payload",
    from_json(col("payload_str"), schema_payload)
)
df_parsed.select("id", "payload.acao", "payload.valor").show()

# Converter struct de volta para string JSON
df_parsed.withColumn("payload_json", to_json(col("payload"))).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ## 6. JDBC — leitura de bancos relacionais

# COMMAND ----------

# MAGIC %md
# ### Analogia
# JDBC é uma mangueira conectando o Spark ao banco relacional.
# Uma mangueira fina (1 partição) demora muito para encher a piscina.
# Uma mangueira grossa com divisores (N partições) enche muito mais rápido.
# O truque é configurar corretamente o diâmetro (numPartitions) e os
# divisores (partitionColumn, lowerBound, upperBound).

# COMMAND ----------

# ── Configuração base de JDBC ─────────────────────────────────────────────

jdbc_options = {
    # URL de conexão por banco
    # SQL Server:  "jdbc:sqlserver://host:1433;database=mydb;encrypt=true"
    # Oracle:      "jdbc:oracle:thin:@host:1521:SID"
    # PostgreSQL:  "jdbc:postgresql://host:5432/mydb"
    # MySQL:       "jdbc:mysql://host:3306/mydb"
    "url":      dbutils.secrets.get("jdbc-scope", "url"),
    "user":     dbutils.secrets.get("jdbc-scope", "user"),
    "password": dbutils.secrets.get("jdbc-scope", "password"),

    # Driver JDBC — precisa estar instalado no cluster como library
    # SQL Server:  "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    # Oracle:      "oracle.jdbc.OracleDriver"
    # PostgreSQL:  "org.postgresql.Driver"
    "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

# COMMAND ----------

# ── Leitura simples — 1 partição ─────────────────────────────────────────
# ⚠️ Não use para tabelas grandes — tudo vai para um único executor

df_jdbc_simples = (
    spark.read
    .format("jdbc")
    .options(**jdbc_options)
    .option("dbtable", "dbo.pedidos")
    .load()
)

# COMMAND ----------

# ── Leitura com subquery — filtro no banco (pushdown) ────────────────────
# O banco executa o filtro antes de enviar dados para o Spark

df_jdbc_filtrado = (
    spark.read
    .format("jdbc")
    .options(**jdbc_options)
    .option("dbtable",
        "(SELECT * FROM dbo.pedidos WHERE data_pedido >= DATEADD(day,-7,GETDATE())) t"
    )
    # ⚠️ dbtable com subquery: DEVE ter alias no final → "... ) t"
    .load()
)

# COMMAND ----------

# ── Leitura paralela — para tabelas grandes ──────────────────────────────
# partitionColumn + lowerBound + upperBound + numPartitions
# → Spark divide o range [lowerBound, upperBound] em numPartitions partes
# → Cada partição faz uma query separada com WHERE partitionColumn BETWEEN x AND y

df_jdbc_paralelo = (
    spark.read
    .format("jdbc")
    .options(**jdbc_options)
    .option("dbtable",         "dbo.pedidos")
    .option("partitionColumn", "id")           # coluna numérica ou data
    .option("lowerBound",      "1")            # valor mínimo da coluna
    .option("upperBound",      "10000000")     # valor máximo da coluna
    .option("numPartitions",   "8")            # número de conexões paralelas
    # ⚠️ numPartitions também é o número máximo de conexões simultâneas
    # Não exceda o pool de conexões do banco — coordene com o DBA
    .load()
)

# COMMAND ----------

# ── Opções avançadas de JDBC ──────────────────────────────────────────────

df_jdbc_avancado = (
    spark.read
    .format("jdbc")
    .options(**jdbc_options)
    .option("dbtable",          "dbo.pedidos")
    .option("fetchsize",        "10000")
    # Número de linhas por fetch do JDBC. Padrão é muito baixo (muitos round-trips).
    # Aumentar para 10000-50000 melhora throughput significativamente.

    .option("numPartitions",    "8")
    .option("partitionColumn",  "id")
    .option("lowerBound",       "1")
    .option("upperBound",       "10000000")

    .option("pushDownPredicate", "true")
    # true (padrão): envia filtros do Spark para o banco executar
    # Reduz dados transferidos pela rede

    .option("sessionInitStatement",
            "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'")
    # Executa SQL antes da leitura — útil para configurar timezone, NLS, etc.
    # Suportado em Oracle. Para outros bancos: verificar suporte.

    .load()
)

# COMMAND ----------

# ── Escrita via JDBC ──────────────────────────────────────────────────────

(df.write
    .format("jdbc")
    .options(**jdbc_options)
    .option("dbtable",   "dbo.pedidos_processados")
    .option("batchsize", "10000")   # linhas por batch de INSERT
    .mode("append")
    .save()
)

# modes no JDBC:
# "append":    INSERT INTO
# "overwrite": DROP + CREATE + INSERT (CUIDADO: apaga a tabela)
# "ignore":    só escreve se a tabela não existe
# "error":     falha se a tabela existe

# COMMAND ----------

# MAGIC %md
# ## 7. Autoloader — ingestão incremental de arquivos

# COMMAND ----------

# ── Autoloader: detecta e ingere apenas arquivos novos ───────────────────

df_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",             "json")
    .option("cloudFiles.schemaLocation",     "/mnt/checkpoints/pedidos_schema/")
    # cloudFiles.schemaLocation: onde salvar o schema inferido
    # Permite evolução incremental de schema entre runs

    .option("cloudFiles.inferColumnTypes",   "true")
    # Infere tipos além de string — útil para JSON/CSV

    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    # addNewColumns: adiciona colunas novas automaticamente
    # rescue:        captura colunas não mapeadas em coluna _rescued_data
    # failOnNewColumns: lança erro se encontrar coluna nova

    .option("cloudFiles.maxFilesPerTrigger", "1000")
    # Quantos arquivos processar por micro-batch

    .schema(schema_pedidos)
    .load("/mnt/raw/pedidos/")
)

# Escrever stream como tabela Delta
(df_stream.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/pedidos/")
    .option("mergeSchema",        "true")
    .trigger(availableNow=True)   # processa tudo disponível e para
    .toTable("prod.bronze.pedidos")
)

# COMMAND ----------

# MAGIC %md
# ## 8. Particionamento — estratégia de escrita

# COMMAND ----------

# ── partitionBy: criar diretórios por valor de coluna ────────────────────

from pyspark.sql.functions import year, month

df_com_particoes = (df
    .withColumn("ano", year(col("data_pedido")))
    .withColumn("mes", month(col("data_pedido")))
)

# Escrita com particionamento
(df_com_particoes.write
    .format("delta")
    .partitionBy("ano", "mes")
    .mode("overwrite")
    .save("/mnt/silver/pedidos/")
)
# Gera:
# /mnt/silver/pedidos/ano=2024/mes=1/part-00000.snappy.parquet
# /mnt/silver/pedidos/ano=2024/mes=2/part-00000.snappy.parquet
# ...

# COMMAND ----------

# ── Boas práticas de particionamento ─────────────────────────────────────

boas_praticas = [
    "Cardinalidade baixa-média: ano, mes, regiao, status (10-1000 partições)",
    "Alta cardinalidade mata: id, cpf, timestamp — cria milhões de arquivos",
    "Tamanho alvo por arquivo: 128MB a 1GB após compressão",
    "Profundidade máxima de partição: 2-3 níveis (ex: ano/mes ou regiao/status)",
    "Colunas de filtro mais frequentes primeiro no partitionBy",
    "Use ZORDER para colunas de alta cardinalidade dentro das partições",
]

print("Boas práticas de particionamento:")
for p in boas_praticas:
    print(f"  ✅ {p}")

# COMMAND ----------

# MAGIC %md
# ## 9. Leitura de tabelas do catálogo

# COMMAND ----------

# ── spark.read.table vs spark.table vs spark.sql ─────────────────────────

# Todas equivalentes para leitura de tabela registrada
df1 = spark.read.table("prod.silver.pedidos")
df2 = spark.table("prod.silver.pedidos")
df3 = spark.sql("SELECT * FROM prod.silver.pedidos")

# spark.read.table é mais explícita — preferida em código de pipeline
# spark.sql é mais flexível — permite projeção, filtro e joins inline

# ── Leitura com filtro pushdown no catálogo ───────────────────────────────

# O Catalyst aplica partition pruning automaticamente
df_jan_2024 = (
    spark.read.table("prod.silver.pedidos")
    .filter((col("ano") == 2024) & (col("mes") == 1))
)
df_jan_2024.explain()
# Procure: PartitionFilters: [isnotnull(ano#12), (ano#12 = 2024), ...]

# COMMAND ----------

# MAGIC %md
# ## 10. Referência rápida — opções por formato

# COMMAND ----------

referencia = {
    "CSV": [
        ("header",          "true/false",    "Primeira linha como cabeçalho"),
        ("sep",             "string",        "Separador de colunas (padrão: ',')"),
        ("encoding",        "string",        "Encoding do arquivo (padrão: 'UTF-8')"),
        ("dateFormat",      "string",        "Formato de datas ex: 'dd/MM/yyyy'"),
        ("nullValue",       "string",        "String que representa null"),
        ("mode",            "string",        "PERMISSIVE / DROPMALFORMED / FAILFAST"),
        ("multiLine",       "true/false",    "Campos com quebra de linha"),
        ("inferSchema",     "true/false",    "Inferir tipos — EVITAR em produção"),
    ],
    "JSON": [
        ("multiLine",       "true/false",    "JSON multi-linha (padrão: false = JSON lines)"),
        ("dateFormat",      "string",        "Formato de datas"),
        ("timestampFormat", "string",        "Formato de timestamps"),
        ("mode",            "string",        "PERMISSIVE / DROPMALFORMED / FAILFAST"),
    ],
    "Parquet": [
        ("mergeSchema",     "true/false",    "Unifica schemas de múltiplos arquivos"),
        ("compression",     "string",        "snappy / gzip / lz4 / zstd"),
    ],
    "Delta": [
        ("mergeSchema",     "true/false",    "Aceita novas colunas na escrita"),
        ("overwriteSchema", "true/false",    "Substitui schema no overwrite"),
        ("replaceWhere",    "condição SQL",  "Overwrite de partição específica"),
        ("optimizeWrite",   "true/false",    "Databricks: agrupa writes em menos arquivos"),
        ("versionAsOf",     "int",           "Time travel por número de versão"),
        ("timestampAsOf",   "timestamp str", "Time travel por data"),
        ("readChangeFeed",  "true/false",    "Ler Change Data Feed"),
    ],
    "JDBC": [
        ("url",             "string",        "URL de conexão JDBC"),
        ("dbtable",         "string",        "Tabela ou subquery (com alias)"),
        ("driver",          "string",        "Classe do driver JDBC"),
        ("partitionColumn", "string",        "Coluna para paralelismo de leitura"),
        ("lowerBound",      "string",        "Valor mínimo do partitionColumn"),
        ("upperBound",      "string",        "Valor máximo do partitionColumn"),
        ("numPartitions",   "int",           "Número de partições/conexões paralelas"),
        ("fetchsize",       "int",           "Linhas por fetch (padrão muito baixo)"),
        ("batchsize",       "int",           "Linhas por batch de escrita"),
    ],
}

for formato, opts in referencia.items():
    print(f"\n── {formato} ──────────────────────────────────────")
    print(f"  {'Opção':<25} {'Valores':<20} {'Descrição'}")
    print(f"  {'─'*24} {'─'*19} {'─'*40}")
    for opt, vals, desc in opts:
        print(f"  {opt:<25} {vals:<20} {desc}")

# COMMAND ----------

# MAGIC %md
# ## Resumo — o que fixar deste arquivo
#
# | Conceito | O que saber |
# |----------|-------------|
# | Estrutura universal | `.format().option().schema().load()` — igual para todos os formatos |
# | Write modes | `append` acumula; `overwrite` apaga tudo; `ignore` idempotente; `error` é o padrão |
# | Parquet | Colunar, comprimido, schema embutido — padrão para analytics |
# | Delta | Parquet + ACID + time travel + MERGE — padrão para produção |
# | CSV mode | `PERMISSIVE` captura erros em `_corrupt_record`; `FAILFAST` lança exceção |
# | JDBC paralelo | `partitionColumn + lowerBound + upperBound + numPartitions` |
# | JDBC fetchsize | Aumentar de `10` para `10000` — melhora throughput drasticamente |
# | JDBC subquery | `dbtable` com subquery DEVE ter alias: `(SELECT ...) t` |
# | partitionBy | Baixa cardinalidade (ano, mes, regiao); nunca id ou timestamp |
# | replaceWhere | Overwrite de partição específica sem apagar a tabela inteira |
#
# ### Próximo arquivo
# `09_cache_persist.py` — quando colocar dados em memória, quais StorageLevels
# existem, e o custo real de cache mal usado.
