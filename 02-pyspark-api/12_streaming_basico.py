# Databricks notebook source

# MAGIC %md
# # 12 — Structured Streaming — Fundamentos
#
# > **Arquivo:** `02-pyspark-api/12_streaming_basico.py`
# > **Módulo:** 02 — PySpark API
# > **Dependência:** `08_leitura_escrita.py`
#
# ---
#
# ## Analogia
#
# Batch processing é como lavar uma pilha de roupa acumulada:
# você espera juntar tudo, lava de uma vez, dobra e guarda.
#
# Streaming é como uma lavanderia self-service que funciona 24h:
# cada peça de roupa que chega é processada imediatamente —
# sem esperar acumular, sem prazo de lote, sem batch window.
#
# O Structured Streaming não é um sistema diferente do Spark.
# É a mesma API DataFrame/SQL, mas aplicada a uma fonte de dados
# que cresce continuamente. O Spark trata o stream como uma tabela
# que recebe novas linhas o tempo todo — você escreve a lógica
# como se fosse batch, o framework cuida do "quando executar".
#
# ---
#
# ## Batch vs Streaming — a diferença conceitual
#
# ```
# Batch:     tabela finita  → lê tudo de uma vez → processa → escreve
# Streaming: tabela infinita → lê incrementalmente → processa → escreve continuamente
#
# spark.read   → DataFrame estático  (batch)
# spark.readStream → DataFrame de streaming (unbounded)
# ```

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, window,
    current_timestamp, to_timestamp, expr,
    from_json, to_json, struct
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, TimestampType, IntegerType
)

# COMMAND ----------

# MAGIC %md
# ## 1. readStream — criando um DataFrame de streaming

# COMMAND ----------

# MAGIC %md
# ### 1.1 Anatomia do readStream

# COMMAND ----------

# ── A diferença entre read e readStream ──────────────────────────────────

# BATCH — lê o estado atual dos dados (tabela finita)
df_batch = spark.read.table("prod.bronze.pedidos")
print(type(df_batch))           # pyspark.sql.dataframe.DataFrame
print(df_batch.isStreaming)     # False

# STREAMING — monitora e lê novos dados continuamente
df_stream = spark.readStream.table("prod.bronze.pedidos")
print(type(df_stream))          # pyspark.sql.dataframe.DataFrame (mesma classe!)
print(df_stream.isStreaming)    # True

# A API é idêntica — mesma classe, mesmo tipo de retorno
# A diferença está no comportamento em runtime

# COMMAND ----------

# MAGIC %md
# ### 1.2 Fontes de streaming disponíveis

# COMMAND ----------

# ── 1. Tabela Delta como fonte de stream ──────────────────────────────────
# Mais comum no Databricks — detecta novos commits na tabela

schema_pedidos = StructType([
    StructField("id",          LongType(),      False),
    StructField("id_cliente",  LongType(),      True),
    StructField("valor",       DoubleType(),    True),
    StructField("status",      StringType(),    True),
    StructField("regiao",      StringType(),    True),
    StructField("evento_ts",   TimestampType(), True),
])

df_delta_stream = (
    spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger",  "1")
    # maxFilesPerTrigger: máximo de arquivos Delta por micro-batch
    # Controla a taxa de ingestão — útil para não sobrecarregar downstream
    .option("maxBytesPerTrigger", "10mb")
    # Alternativa: limitar por tamanho em vez de número de arquivos
    .table("prod.bronze.pedidos")
)

# COMMAND ----------

# ── 2. Autoloader (cloudFiles) — detecta novos arquivos em object storage ─

df_autoloader = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",          "json")
    .option("cloudFiles.schemaLocation",  "/mnt/checkpoints/schema/pedidos/")
    # Obrigatório: onde o Autoloader salva o schema inferido/evoluído
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    # addNewColumns: aceita novas colunas silenciosamente
    # rescue:         coloca campos não mapeados em _rescued_data
    # failOnNewColumns: lança erro
    .option("cloudFiles.maxFilesPerTrigger", "100")
    .schema(schema_pedidos)
    .load("/mnt/raw/pedidos/")
)
# Autoloader rastreia quais arquivos já foram processados
# Seguro para reprocessamento — não lê o mesmo arquivo duas vezes

# COMMAND ----------

# ── 3. Kafka como fonte de stream ─────────────────────────────────────────

df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("subscribe",               "pedidos-topic")
    # subscribe: um tópico
    # subscribePattern: regex para múltiplos tópicos
    # assign: partições específicas via JSON
    .option("startingOffsets",         "latest")
    # latest:   só mensagens novas (a partir de agora)
    # earliest: todas as mensagens desde o início do tópico
    # JSON específico: '{"topico":{"0":100,"1":200}}'
    .option("maxOffsetsPerTrigger",    "10000")
    # Máximo de mensagens por micro-batch
    .option("failOnDataLoss",          "true")
    # false: continua se offsets não estiverem mais disponíveis
    # true (padrão): lança erro — mais seguro
    .load()
)
# Schema do Kafka é fixo: key, value, topic, partition, offset, timestamp, timestampType
# Tudo como binary → você precisa fazer cast/from_json no value

# COMMAND ----------

# ── 4. Rate source — para testes e desenvolvimento ────────────────────────

df_rate = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", "10")   # gera N linhas por segundo
    .option("numPartitions", "2")
    .load()
    # Schema: timestamp (TimestampType), value (LongType)
)

# Útil para testar a lógica do pipeline sem fonte real

# COMMAND ----------

# MAGIC %md
# ## 2. Transformações em streaming

# COMMAND ----------

# MAGIC %md
# ### A maioria das transformações batch funciona em streaming

# COMMAND ----------

# ── Transformações stateless — funcionam igual ao batch ──────────────────

df_transformado = (
    df_delta_stream
    .filter(col("valor") > 0)
    .filter(col("status").isin("PAGO", "PENDENTE"))
    .withColumn("processado_em", current_timestamp())
    .select(
        "id", "id_cliente", "valor",
        "status", "regiao", "evento_ts", "processado_em"
    )
)
# Stateless: cada linha é processada independentemente
# Não mantém estado entre micro-batches

# COMMAND ----------

# ── Transformações stateful — requerem cuidado especial ──────────────────

# groupBy SEM window → requer outputMode="complete" (guarda resultado inteiro)
df_agg_completo = (
    df_delta_stream
    .groupBy("regiao")
    .agg(
        count("*").alias("qtd"),
        spark_sum("valor").alias("total"),
        avg("valor").alias("media")
    )
)
# Spark mantém o estado acumulado de TODAS as regiões em memória
# A cada micro-batch, recalcula o resultado completo

# COMMAND ----------

# ── Kafka — deserializando o payload ─────────────────────────────────────

schema_kafka_valor = StructType([
    StructField("id",       LongType(),   True),
    StructField("valor",    DoubleType(), True),
    StructField("status",   StringType(), True),
    StructField("evento_ts",StringType(), True),
])

df_kafka_parsed = (
    df_kafka
    .select(
        col("key").cast("string").alias("key"),
        from_json(col("value").cast("string"), schema_kafka_valor).alias("dados"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_ts"),
    )
    .select("key", "dados.*", "topic", "partition", "offset", "kafka_ts")
)

# COMMAND ----------

# MAGIC %md
# ## 3. writeStream — escrevendo o stream

# COMMAND ----------

# MAGIC %md
# ### 3.1 Estrutura do writeStream

# COMMAND ----------

# ── Estrutura completa do writeStream ────────────────────────────────────
#
# df_stream.writeStream
#   .format(str)                 → destino: "delta", "console", "memory", "kafka"
#   .option("checkpointLocation", path)  → OBRIGATÓRIO para fault-tolerance
#   .outputMode(str)             → "append" | "update" | "complete"
#   .trigger(...)                → frequência de execução
#   .partitionBy(*cols)          → particionamento no destino
#   .queryName(str)              → nome para identificar no Spark UI
#   .start()                     → inicia o stream (retorna StreamingQuery)
#   .toTable(nome)               → atalho: escreve em tabela Delta e registra

# COMMAND ----------

# MAGIC %md
# ## 4. Output Modes — o conceito mais importante

# COMMAND ----------

# MAGIC %md
# ### Analogia dos outputModes
#
# Imagine um placar de futebol ao vivo:
#
# **append** — só anuncia gols novos: "Gol do Brasil no minuto 35!"
# Você nunca fica sabendo o placar total, só eventos novos.
#
# **update** — só mostra as linhas que mudaram: "Brasil: 1, Argentina: 0"
# e quando muda: "Brasil: 2, Argentina: 0". Só o que foi atualizado.
#
# **complete** — mostra o placar completo toda vez que há mudança:
# "Placar atual: Brasil 2x1 Argentina" — resultado inteiro a cada trigger.

# COMMAND ----------

# ── append: apenas linhas novas adicionadas desde o último trigger ────────
#
# ✅ Funciona com: transformações stateless (filter, select, withColumn, join)
# ✅ Funciona com: watermark + window aggregations
# ❌ NÃO funciona com: groupBy sem watermark (linhas podem ser atualizadas)

query_append = (
    df_transformado.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/checkpoints/pedidos_processados/")
    .trigger(processingTime="30 seconds")
    .toTable("prod.silver.pedidos_stream")
)

# COMMAND ----------

# ── complete: resultado completo a cada trigger ───────────────────────────
#
# ✅ Funciona com: aggregations (groupBy + agg)
# ❌ NÃO funciona com: transformações sem aggregation (escreveria tudo de novo)
# ⚠️ Cuidado: toda vez, TODAS as linhas do resultado são escritas
#    Para tabelas grandes isso pode ser muito caro

query_complete = (
    df_agg_completo.writeStream
    .format("memory")           # "memory": armazena na memória do driver (só para debug)
    .queryName("agg_regiao")    # acesse com: spark.table("agg_regiao")
    .outputMode("complete")
    .trigger(processingTime="10 seconds")
    .start()
)

# Consultar resultado da query "memory" (útil para debug)
spark.table("agg_regiao").show()

# COMMAND ----------

# ── update: apenas linhas atualizadas ou inseridas desde o último trigger ─
#
# ✅ Funciona com: aggregations sem watermark
# ✅ Mais eficiente que complete para aggregations grandes
# ❌ NÃO funciona com: Delta Lake como sink (Delta não suporta update mode)
# ✅ Funciona com: console, memory, Kafka como sink

query_update = (
    df_agg_completo.writeStream
    .format("console")
    .outputMode("update")
    .trigger(processingTime="10 seconds")
    .start()
)

# COMMAND ----------

# ── Tabela de compatibilidade de outputMode ───────────────────────────────

compatibilidade = [
    ("Transformação",                       "append", "update", "complete"),
    ("filter / select / withColumn",        "✅",     "✅",     "❌"),
    ("groupBy + agg (sem watermark)",       "❌",     "✅",     "✅"),
    ("groupBy + agg (com watermark)",       "✅",     "✅",     "❌"),
    ("join stream-batch",                   "✅",     "❌",     "❌"),
    ("join stream-stream",                  "✅",     "❌",     "❌"),
    ("distinct",                            "❌",     "❌",     "✅"),
]

print(f"\n  {'Transformação':<42} {'append':<10} {'update':<10} {'complete'}")
print("  " + "─" * 75)
for row in compatibilidade[1:]:
    print(f"  {row[0]:<42} {row[1]:<10} {row[2]:<10} {row[3]}")

# COMMAND ----------

# MAGIC %md
# ## 5. Triggers — quando executar o micro-batch

# COMMAND ----------

# MAGIC %md
# ### Analogia dos Triggers
#
# processingTime = relógio de cozinha: executa a cada N segundos/minutos
# once = trabalhador avulso: executa uma vez e vai embora (legado)
# availableNow = trabalhador contratado por tarefa: processa tudo pendente e para
# continuous = torneira aberta: executa constantemente com milissegundos de latência

# COMMAND ----------

from pyspark.sql.streaming import DataStreamWriter

# ── processingTime — micro-batch periódico ────────────────────────────────
# Executa a cada N segundos/minutos/horas
# Se o batch anterior não terminou: espera terminar antes de iniciar o próximo

query = (
    df_transformado.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/checkpoints/pedidos/")
    .trigger(processingTime="30 seconds")   # a cada 30s
    .trigger(processingTime="5 minutes")    # a cada 5min
    .trigger(processingTime="1 hour")       # a cada 1h
    .toTable("prod.silver.pedidos_stream")
)

# COMMAND ----------

# ── availableNow — processa tudo disponível e para (substitui once) ───────
# Processa todos os arquivos/commits pendentes em múltiplos batches e termina
# Cada batch respeita maxFilesPerTrigger e maxBytesPerTrigger
# ✅ Recomendado para jobs agendados (workflows diários, por exemplo)

query = (
    df_autoloader.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/checkpoints/pedidos_autoloader/")
    .trigger(availableNow=True)   # processa tudo pendente e para
    .toTable("prod.bronze.pedidos")
)

# Aguardar a conclusão quando usar availableNow (no Databricks Workflow)
query.awaitTermination()

# COMMAND ----------

# ── once — legado (substituído por availableNow) ──────────────────────────
# Processa TODO o backlog em UM único batch e para
# Diferença de availableNow: não respeita maxFilesPerTrigger

query = (
    df_autoloader.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/checkpoints/pedidos_once/")
    .trigger(once=True)   # processa tudo em 1 batch e para
    .toTable("prod.bronze.pedidos")
)

# ── Comparação once vs availableNow ──────────────────────────────────────
print("""
  once:          1 batch com TUDO pendente (pode ser muito grande)
  availableNow:  N batches respeitando maxFilesPerTrigger (mais seguro)
  
  Recomendação: sempre usar availableNow em vez de once
""")

# COMMAND ----------

# ── continuous — ultra-baixa latência (experimental) ─────────────────────
# Executa continuamente — latência de milissegundos
# Restrições: só transformações stateless, sem aggregations

query = (
    df_kafka_parsed.writeStream
    .format("kafka")
    .outputMode("append")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("topic", "pedidos-processados")
    .option("checkpointLocation", "/mnt/checkpoints/kafka_out/")
    .trigger(continuous="1 second")   # checkpoint a cada 1s
    .start()
)

# COMMAND ----------

# ── Sem trigger (default) — micro-batch contínuo ─────────────────────────
# Executa novo batch imediatamente quando o anterior termina
# Sem pausa entre batches — máximo throughput com latência baixa

query = (
    df_transformado.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/checkpoints/pedidos_cont/")
    # sem .trigger() → executa continuamente
    .toTable("prod.silver.pedidos_stream")
)

# COMMAND ----------

# MAGIC %md
# ## 6. Checkpointing — fault-tolerance obrigatório

# COMMAND ----------

# MAGIC %md
# ### Analogia
# O checkpoint é como um jogo de videogame com save automático.
# Se o Spark cair no meio do processamento (queda de executor,
# reinicialização de cluster), ele retoma do último save —
# não do início do nível.
# Sem checkpoint, você volta do começo e processa tudo novamente.

# COMMAND ----------

# ── O que o checkpoint armazena ───────────────────────────────────────────
#
# /mnt/checkpoints/meu-stream/
# ├── commits/
# │   ├── 0          ← "batch 0 concluído com sucesso"
# │   ├── 1
# │   └── 2
# ├── offsets/
# │   ├── 0          ← "batch 0 leu até este offset/versão"
# │   ├── 1
# │   └── 2
# └── state/         ← estado de aggregations (se houver)
#     └── 0/
#         └── default/

# COMMAND ----------

# ── Regras do checkpoint ──────────────────────────────────────────────────

regras = [
    ("Obrigatório",
     "Toda query de stream em produção DEVE ter checkpointLocation",
     "Sem checkpoint: falha reinicia do zero — dados podem ser reprocessados ou perdidos"),
    ("Um checkpoint por query",
     "Cada query de stream precisa de um checkpointLocation exclusivo",
     "Dois streams no mesmo checkpoint: comportamento indefinido e corrompido"),
    ("Checkpoint deve persistir",
     "Use cloud storage (S3, ADLS) — nunca disco local do driver",
     "Disco local é perdido quando o cluster reinicia"),
    ("Não delete o checkpoint",
     "Deletar o checkpoint reseta o stream para o início",
     "Para reprocessar tudo: delete checkpoint E garanta idempotência no sink"),
    ("Compatibilidade",
     "Mudanças no schema ou na query podem invalidar o checkpoint",
     "Teste em dev antes de alterar queries em produção"),
]

for nome, regra, detalhe in regras:
    print(f"\n  ✅ {nome}")
    print(f"     {regra}")
    print(f"     → {detalhe}")

# COMMAND ----------

# ── Padrão de checkpointLocation recomendado ─────────────────────────────

# Nomeie o checkpoint de forma que identifique inequivocamente a query
# Use o nome da tabela de destino como base

def checkpoint_path(tabela: str, versao: str = "v1") -> str:
    """
    Gera path de checkpoint padronizado.
    Inclui versão para facilitar reset quando necessário.
    """
    return f"/mnt/checkpoints/{tabela.replace('.', '/')}/{versao}/"

print(checkpoint_path("prod.silver.pedidos_stream"))
# /mnt/checkpoints/prod/silver/pedidos_stream/v1/

# COMMAND ----------

# MAGIC %md
# ## 7. StreamingQuery — gerenciando queries ativas

# COMMAND ----------

# ── StreamingQuery — o objeto retornado por .start() ─────────────────────

query = (
    df_transformado.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/checkpoints/pedidos/v1/")
    .trigger(processingTime="30 seconds")
    .queryName("pipeline-pedidos-silver")
    .toTable("prod.silver.pedidos_stream")
)

# Informações sobre a query
print(f"ID:     {query.id}")              # UUID estável — mesmo após restart
print(f"RunID:  {query.runId}")           # UUID desta execução específica
print(f"Nome:   {query.name}")            # "pipeline-pedidos-silver"
print(f"Ativa:  {query.isActive}")        # True

# COMMAND ----------

# Status e progresso
print(query.status)
# {
#   "message": "Processing new data",
#   "isDataAvailable": true,
#   "isTriggerActive": true
# }

print(query.lastProgress)
# Métricas do último batch: linhas processadas, latência, taxa de ingestão

# COMMAND ----------

# ── Controle do ciclo de vida da query ───────────────────────────────────

# Aguardar conclusão (bloqueia — útil com availableNow ou once)
query.awaitTermination()
query.awaitTermination(timeout=300)   # timeout em segundos

# Parar a query
query.stop()

# Listar todas as queries ativas no cluster
queries_ativas = spark.streams.active
for q in queries_ativas:
    print(f"  {q.name}: {q.status['message']}")

# COMMAND ----------

# ── Tratamento de erros na query ─────────────────────────────────────────

import time

query = (
    df_transformado.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/checkpoints/pedidos/v1/")
    .trigger(processingTime="30 seconds")
    .toTable("prod.silver.pedidos_stream")
)

try:
    query.awaitTermination()
except Exception as e:
    print(f"❌ Stream falhou: {e}")
    # Verificar a exceção real
    if query.exception():
        print(f"Exceção do stream: {query.exception()}")
finally:
    if query.isActive:
        query.stop()

# COMMAND ----------

# MAGIC %md
# ## 8. foreachBatch — lógica customizada por batch

# COMMAND ----------

# MAGIC %md
# ### Quando usar foreachBatch
#
# `foreachBatch` expõe cada micro-batch como um DataFrame estático.
# Isso permite usar qualquer lógica batch no stream:
# MERGE INTO, escrita em múltiplos destinos, validações customizadas.

# COMMAND ----------

from delta.tables import DeltaTable

def processar_batch(df_batch, batch_id):
    """
    Função chamada para cada micro-batch.
    df_batch: DataFrame estático com os dados do batch atual
    batch_id: identificador numérico do batch (0, 1, 2, ...)
    """
    print(f"Processando batch {batch_id}: {df_batch.count()} linhas")

    # ── Idempotência com batch_id ─────────────────────────────────────
    # Em caso de retry, o mesmo batch_id pode ser processado novamente
    # Garanta que a lógica seja idempotente

    # ── Exemplo 1: MERGE INTO (upsert) ───────────────────────────────
    if DeltaTable.isDeltaTable(spark, "/mnt/silver/pedidos/"):
        dt = DeltaTable.forPath(spark, "/mnt/silver/pedidos/")
        (dt.alias("alvo")
            .merge(df_batch.alias("fonte"), "alvo.id = fonte.id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        # Primeira vez — tabela não existe, cria com overwrite
        df_batch.write.format("delta").mode("overwrite").save("/mnt/silver/pedidos/")

    # ── Exemplo 2: Escrever em múltiplos destinos ─────────────────────
    # Cache o batch se for escrito em mais de um destino
    df_batch.cache()

    # Destino 1: tabela principal
    df_batch.write.format("delta").mode("append").saveAsTable("prod.silver.pedidos")

    # Destino 2: só pedidos de alto valor para análise imediata
    (df_batch
        .filter(col("valor") > 10000)
        .write.format("delta").mode("append")
        .saveAsTable("prod.gold.pedidos_alto_valor")
    )

    df_batch.unpersist()

# Usando foreachBatch
query = (
    df_delta_stream.writeStream
    .foreachBatch(processar_batch)
    .option("checkpointLocation", "/mnt/checkpoints/foreachbatch/v1/")
    .trigger(processingTime="1 minute")
    .start()
)

# COMMAND ----------

# MAGIC %md
# ## 9. Watermark — lidando com dados atrasados

# COMMAND ----------

# MAGIC %md
# ### Analogia
# Imagine uma corrida de rua onde os resultados chegam por WhatsApp.
# A maioria dos corredores manda o resultado em até 10 minutos após
# cruzar a linha de chegada. Alguns mandam depois de uma hora.
# Você precisa decidir: quanto tempo esperar antes de publicar
# o resultado oficial e não aceitar mais mensagens atrasadas?
# O watermark é exatamente esse "tempo de espera".

# COMMAND ----------

from pyspark.sql.functions import window, col, count, spark_sum

# withWatermark(eventTime, delay)
# → eventTime: coluna de timestamp dos eventos
# → delay: quanto tempo esperar por dados atrasados

df_com_watermark = (
    df_delta_stream
    .withWatermark("evento_ts", "10 minutes")
    # Aceita dados com até 10 minutos de atraso
    # Eventos com evento_ts < (max_evento_ts_visto - 10min) → descartados
)

# ── Aggregation com window de tempo ──────────────────────────────────────

df_windowed = (
    df_com_watermark
    .groupBy(
        window(col("evento_ts"), "1 hour", "30 minutes"),
        # window(coluna, tamanho, slide)
        # tamanho: duração de cada janela
        # slide:   frequência de criação de nova janela
        # slide < tamanho → janelas se sobrepõem (sliding window)
        # slide = tamanho → janelas não se sobrepõem (tumbling window)
        col("regiao")
    )
    .agg(
        count("*").alias("qtd_pedidos"),
        spark_sum("valor").alias("valor_total"),
    )
)

query_windowed = (
    df_windowed.writeStream
    .format("delta")
    .outputMode("append")      # com watermark: append é possível
    .option("checkpointLocation", "/mnt/checkpoints/windowed/v1/")
    .trigger(processingTime="5 minutes")
    .toTable("prod.gold.pedidos_por_hora_regiao")
)

# COMMAND ----------

# ── Tumbling window vs Sliding window ────────────────────────────────────

# TUMBLING WINDOW (não se sobrepõe):
# window("ts", "1 hour")   →  slide = tamanho = 1h
# [10:00, 11:00)
# [11:00, 12:00)
# [12:00, 13:00)
# Cada evento pertence a EXATAMENTE 1 janela

# SLIDING WINDOW (se sobrepõe):
# window("ts", "1 hour", "30 minutes")  →  slide = 30min, tamanho = 1h
# [10:00, 11:00)
# [10:30, 11:30)
# [11:00, 12:00)
# Cada evento pode pertencer a MÚLTIPLAS janelas

# SESSION WINDOW (baseada em atividade):
# from pyspark.sql.functions import session_window
# session_window("ts", "10 minutes")
# Janela se abre no 1º evento, fecha após 10min de inatividade

# COMMAND ----------

# MAGIC %md
# ## 10. Padrões de pipeline streaming em produção

# COMMAND ----------

# ── Padrão 1: Bronze streaming com Autoloader ─────────────────────────────

query_bronze = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",        "json")
    .option("cloudFiles.schemaLocation", "/mnt/checkpoints/schema/bronze_pedidos/")
    .load("/mnt/landing/pedidos/")
    .withColumn("_ingest_ts", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/checkpoints/bronze_pedidos/v1/")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable("prod.bronze.pedidos")
)
query_bronze.awaitTermination()

# COMMAND ----------

# ── Padrão 2: Silver streaming com filtro e enriquecimento ────────────────

query_silver = (
    spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", "10")
    .table("prod.bronze.pedidos")
    .filter(col("valor").isNotNull())
    .filter(col("valor") > 0)
    .filter(col("id").isNotNull())
    .withColumn("processado_em", current_timestamp())
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/checkpoints/silver_pedidos/v1/")
    .trigger(processingTime="5 minutes")
    .toTable("prod.silver.pedidos")
)

# COMMAND ----------

# ── Padrão 3: Gold com foreachBatch + MERGE (SCD Type 1) ─────────────────

def upsert_gold(df_batch, batch_id):
    if DeltaTable.isDeltaTable(spark, "prod.gold.pedidos_resumo"):
        dt = DeltaTable.forName(spark, "prod.gold.pedidos_resumo")
        resumo_batch = (df_batch
            .groupBy("id_cliente", "regiao")
            .agg(
                count("*").alias("qtd"),
                spark_sum("valor").alias("total")
            )
        )
        (dt.alias("alvo")
            .merge(
                resumo_batch.alias("fonte"),
                "alvo.id_cliente = fonte.id_cliente AND alvo.regiao = fonte.regiao"
            )
            .whenMatchedUpdate(set={
                "qtd":   "alvo.qtd + fonte.qtd",
                "total": "alvo.total + fonte.total",
                "updated_at": "current_timestamp()"
            })
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (df_batch
            .groupBy("id_cliente", "regiao")
            .agg(count("*").alias("qtd"), spark_sum("valor").alias("total"))
            .write.format("delta").mode("overwrite")
            .saveAsTable("prod.gold.pedidos_resumo")
        )

query_gold = (
    spark.readStream.table("prod.silver.pedidos")
    .writeStream
    .foreachBatch(upsert_gold)
    .option("checkpointLocation", "/mnt/checkpoints/gold_resumo/v1/")
    .trigger(processingTime="10 minutes")
    .start()
)

# COMMAND ----------

# MAGIC %md
# ## 11. Debugging de streams

# COMMAND ----------

# ── console sink — visualizar saída no notebook ───────────────────────────

query_debug = (
    df_transformado.writeStream
    .format("console")
    .outputMode("append")
    .option("numRows", "20")        # quantas linhas mostrar
    .option("truncate", "false")    # não truncar colunas
    .trigger(processingTime="10 seconds")
    .start()
)

# Parar após ver alguns batches
import time
time.sleep(30)
query_debug.stop()

# COMMAND ----------

# ── memory sink — consultar resultado como tabela ─────────────────────────

query_mem = (
    df_agg_completo.writeStream
    .format("memory")
    .queryName("debug_agg")
    .outputMode("complete")
    .trigger(processingTime="5 seconds")
    .start()
)

time.sleep(15)
spark.table("debug_agg").show()   # consulta o resultado atual

query_mem.stop()

# COMMAND ----------

# MAGIC %md
# ## Resumo — o que fixar deste arquivo
#
# | Conceito | O que saber |
# |----------|-------------|
# | readStream vs read | Mesma API — `isStreaming` diferencia. Mesmo código de transformação. |
# | outputMode append | Novas linhas apenas — stateless ou watermark+window |
# | outputMode complete | Resultado inteiro — aggregations sem watermark |
# | outputMode update | Só linhas alteradas — aggregations, não funciona com Delta sink |
# | Trigger processingTime | Micro-batch periódico — frequência configurável |
# | Trigger availableNow | Processa backlog em N batches e para — substitui once |
# | Trigger once | Processa tudo em 1 batch e para — legado, preferir availableNow |
# | checkpoint | OBRIGATÓRIO — um por query, em cloud storage, nunca delete |
# | foreachBatch | Expõe cada micro-batch como DataFrame estático — permite MERGE |
# | withWatermark | Define tolerância a dados atrasados — habilita append mode em aggregations |
# | window | Aggregation por janela de tempo — tumbling (sem overlap) ou sliding |
# | console / memory sink | Apenas para debug — nunca em produção |
#
# ### Conexão com a certificação Associate
# - A prova testa: diferença entre os 3 outputModes, quando usar cada trigger,
#   o que o checkpoint armazena, diferença entre `once` e `availableNow`,
#   e por que `outputMode("complete")` é necessário para aggregations sem watermark
#
# ### Próximo módulo
# `03-spark-sql/` — DDL, DML, CTEs, MERGE INTO, EXPLAIN e funções SQL —
# tudo que você já fez em PySpark, agora com a sintaxe SQL pura.
