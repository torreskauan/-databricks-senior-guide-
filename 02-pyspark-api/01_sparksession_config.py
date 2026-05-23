# Databricks notebook source

# MAGIC %md
# # 01 — SparkSession: O Ponto de Entrada do Spark
#
# > **Arquivo:** `02-pyspark-api/01_sparksession_config.py`
# > **Módulo:** 02 — PySpark API
# > **Nível:** Fundação — entender bem antes de qualquer outro tópico
#
# ---
#
# ## Analogia
#
# A SparkSession é como a recepção de um hotel cinco estrelas.
# Você não precisa saber como a cozinha funciona, quem limpa os quartos
# ou como o estacionamento é gerenciado. Você vai à recepção, faz o
# check-in uma única vez e, a partir daí, a recepção coordena tudo
# para você: reserva de recursos, acesso ao catálogo de dados,
# execução de queries.
#
# Tudo no Spark começa e passa pela SparkSession.
#
# ---
#
# ## Conceito técnico
#
# A `SparkSession` é o ponto de entrada unificado para todas as
# funcionalidades do Spark desde a versão 2.0. Ela encapsula:
#
# - `SparkContext`  → conexão com o cluster, agendamento de tasks
# - `SQLContext`    → execução de Spark SQL
# - `HiveContext`   → acesso ao metastore (Hive ou Unity Catalog)
#
# Antes do Spark 2.0 existiam três objetos separados. A SparkSession
# os unificou em um único ponto de entrada.
#
# No Databricks, a SparkSession já está criada e disponível como
# variável global `spark`. Você nunca precisa criá-la manualmente.
# Em ambientes externos (scripts locais, testes, AWS Glue), você cria
# via `.builder`.

# COMMAND ----------

# MAGIC %md
# ## 1. SparkSession no Databricks vs fora do Databricks

# COMMAND ----------

# ── No Databricks ─────────────────────────────────────────────────────────
# A SparkSession já existe. Basta usar.

print(spark)
# Saída: <pyspark.sql.session.SparkSession object at 0x...>

print(spark.version)
# Saída: 3.5.0 (ou a versão do seu DBR)

print(type(spark))
# Saída: <class 'pyspark.sql.session.SparkSession'>

# COMMAND ----------

# ── Fora do Databricks (scripts locais, testes, outros ambientes) ─────────
# Você precisa criar a SparkSession via builder

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("MeuPipeline")               # nome visível no Spark UI
    .master("local[*]")                   # local[*] = todos os cores da máquina
    .getOrCreate()                        # cria nova ou retorna a existente
)

# COMMAND ----------

# MAGIC %md
# ## 2. O padrão `.builder` — cada método explicado

# COMMAND ----------

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder

    # ── Identidade da aplicação ──────────────────────────────────────────
    .appName("pipeline-vendas-incremental")
    # Nome que aparece no Spark UI (aba Jobs e no cabeçalho)
    # Boas práticas: nome descritivo do pipeline, não genérico
    # Evite: "MyApp", "teste", "notebook"
    # Prefira: "pipeline-pedidos-incremental", "silver-clientes-dedup"

    # ── Master — onde o Spark vai rodar ─────────────────────────────────
    .master("local[*]")
    # local        → 1 thread (sem paralelismo)
    # local[4]     → 4 threads
    # local[*]     → todos os cores disponíveis
    # yarn         → cluster YARN (Hadoop)
    # spark://host → cluster Spark standalone
    # No Databricks: master é gerenciado automaticamente, não configurar

    # ── Configurações de execução ────────────────────────────────────────
    .config("spark.sql.shuffle.partitions", "8")
    # Número de partições após operações de shuffle (groupBy, join)
    # Padrão: 200 — alto demais para datasets pequenos em desenvolvimento
    # Regra: ~2-4x o número de cores disponíveis para datasets médios
    # Para produção com datasets grandes: 400-800 ou deixe o AQE ajustar

    .config("spark.sql.adaptive.enabled", "true")
    # AQE: Adaptive Query Execution — reotimiza o plano em runtime
    # Já é true por padrão no Spark 3.2+ e no Databricks
    # Inclui: coalesce automático, dynamic join strategy, skew handling

    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    # AQE reduz automaticamente partições pequenas após shuffle
    # Resolve o problema de ter 200 partições com 1KB cada

    .config("spark.sql.autoBroadcastJoinThreshold", "10485760")
    # Threshold para BroadcastHashJoin: 10MB (10 * 1024 * 1024)
    # Tabelas menores que isso são transmitidas para todos os executors
    # Aumentar para 52428800 (50MB) se tiver memória disponível
    # -1 desativa broadcast completamente

    # ── Catálogo e metastore ─────────────────────────────────────────────
    .config("spark.sql.catalogImplementation", "hive")
    # "hive"     → usa Hive Metastore (ou Unity Catalog no Databricks)
    # "in-memory"→ catálogo temporário, sem persistência entre sessões
    # No Databricks: gerenciado automaticamente pelo Unity Catalog

    # ── Delta Lake (fora do Databricks) ─────────────────────────────────
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0")
    # Necessário APENAS fora do Databricks para habilitar Delta Lake
    # No Databricks: Delta já vem incluído no runtime

    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    # Registra as extensões SQL do Delta (MERGE, DESCRIBE HISTORY, etc.)
    # Necessário APENAS fora do Databricks

    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Registra o catálogo Delta como catálogo padrão
    # Necessário APENAS fora do Databricks

    # ── Memória e performance ────────────────────────────────────────────
    .config("spark.driver.memory", "4g")
    # Memória heap do Driver
    # Aumentar se receber OutOfMemoryError no driver
    # Relevante apenas em modo local — no cluster, configurar no cluster

    .config("spark.executor.memory", "4g")
    # Memória heap de cada Executor
    # Relevante apenas em modo local — no cluster, configurar no cluster

    .config("spark.memory.fraction", "0.6")
    # Fração da heap para Execution + Storage Memory (pool unificado)
    # Padrão: 0.6 (60%) — restante para User Memory e overhead interno

    # ── Logging ──────────────────────────────────────────────────────────
    .config("spark.eventLog.enabled", "false")
    # true: salva eventos para reconstrução do Spark History Server
    # false: desativar em desenvolvimento local para evitar I/O extra

    .getOrCreate()
    # Comportamento do getOrCreate:
    # → Se não existe sessão ativa: CRIA uma nova com as configs acima
    # → Se já existe sessão ativa: RETORNA a existente (ignora as configs!)
    # IMPORTANTE: configs passadas no builder só funcionam na CRIAÇÃO
    # Para alterar configs em runtime, use spark.conf.set() após criar
)

# COMMAND ----------

# MAGIC %md
# ## 3. getOrCreate — o detalhe que pega todo mundo

# COMMAND ----------

# ── Comportamento do getOrCreate ──────────────────────────────────────────

# PRIMEIRA CHAMADA — cria a sessão com as configs
spark1 = (
    SparkSession.builder
    .appName("app-a")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

print(spark1.conf.get("spark.sql.shuffle.partitions"))  # "8" ✅

# SEGUNDA CHAMADA — retorna a sessão EXISTENTE, ignora novas configs
spark2 = (
    SparkSession.builder
    .appName("app-b")                                    # ignorado
    .config("spark.sql.shuffle.partitions", "400")       # ignorado
    .getOrCreate()
)

print(spark2 is spark1)                                  # True — mesma sessão
print(spark2.conf.get("spark.sql.shuffle.partitions"))   # ainda "8", não "400"

# COMMAND ----------

# MAGIC %md
# ### ⚠️ Armadilha clássica
#
# Se você tentar mudar configs via builder depois que a sessão já foi criada,
# as novas configs são silenciosamente ignoradas. Não há erro, não há aviso.
#
# Para alterar configs em runtime, use `spark.conf.set()`:

# COMMAND ----------

# ── Alterar configs em runtime com spark.conf.set ─────────────────────────

# Ler valor atual
print(spark.conf.get("spark.sql.shuffle.partitions"))
# "200" (padrão) ou o valor configurado

# Alterar em runtime — funciona mesmo após a sessão estar criada
spark.conf.set("spark.sql.shuffle.partitions", "50")
print(spark.conf.get("spark.sql.shuffle.partitions"))
# "50" ✅

# Alterar múltiplas configs de uma vez (helper pattern)
configs = {
    "spark.sql.shuffle.partitions":               "50",
    "spark.sql.adaptive.enabled":                 "true",
    "spark.sql.autoBroadcastJoinThreshold":       "52428800",  # 50MB
    "spark.sql.adaptive.skewJoin.enabled":        "true",
    "spark.databricks.delta.optimizeWrite.enabled": "true",
}
for key, value in configs.items():
    spark.conf.set(key, value)
    print(f"  SET {key} = {value}")

# COMMAND ----------

# MAGIC %md
# ## 4. SparkSession, SparkContext e SQLContext — a relação entre eles

# COMMAND ----------

# ── Hierarquia de objetos ─────────────────────────────────────────────────
#
# SparkSession (ponto de entrada unificado — use sempre este)
#   └── SparkContext (sc)    → comunicação com o cluster
#       └── RDD API, acumuladores, variáveis broadcast
#
# A SparkSession encapsula SparkContext internamente.
# Você acessa o SparkContext via spark.sparkContext quando necessário.

# Acessar o SparkContext a partir da SparkSession
sc = spark.sparkContext

print(f"App Name:      {sc.appName}")
print(f"Master:        {sc.master}")
print(f"Spark Version: {sc.version}")
print(f"Default parallelism (cores): {sc.defaultParallelism}")
print(f"Executor memory: {sc.getConf().get('spark.executor.memory', 'não configurado')}")

# COMMAND ----------

# Quando usar SparkContext diretamente (casos específicos):

# 1. Broadcast variables
lookup_dict = {"SP": "São Paulo", "RJ": "Rio de Janeiro"}
broadcast_lookup = sc.broadcast(lookup_dict)
print(broadcast_lookup.value)  # {"SP": "São Paulo", "RJ": "Rio de Janeiro"}

# 2. Accumulators
contador_erros = sc.accumulator(0)

# 3. RDDs (raramente necessário em PySpark moderno)
rdd = sc.parallelize([1, 2, 3, 4, 5])
print(rdd.collect())  # [1, 2, 3, 4, 5]

# COMMAND ----------

# MAGIC %md
# ## 5. Inspecionando a SparkSession

# COMMAND ----------

# ── Informações úteis sobre a sessão ativo ────────────────────────────────

# Versão do Spark
print(f"Spark version: {spark.version}")

# Databricks Runtime (no Databricks)
try:
    dbr_version = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
    print(f"DBR: {dbr_version}")
except Exception:
    print("Não está no Databricks")

# Configurações ativas
print("\n── Configs de shuffle e AQE ─────────────────")
configs_para_inspecionar = [
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.sql.warehouse.dir",
]
for cfg in configs_para_inspecionar:
    try:
        print(f"  {cfg} = {spark.conf.get(cfg)}")
    except Exception:
        print(f"  {cfg} = (não definida)")

# COMMAND ----------

# Catálogos disponíveis (Unity Catalog)
try:
    spark.sql("SHOW CATALOGS").show()
except Exception as e:
    print(f"Unity Catalog não disponível: {e}")

# Schema e tabelas do catálogo atual
spark.sql("SHOW DATABASES").show()

# COMMAND ----------

# MAGIC %md
# ## 6. SparkSession em testes unitários com pytest

# COMMAND ----------

# ── Padrão para testes locais com pytest + chispa ─────────────────────────
#
# Em testes, você quer uma SparkSession isolada, leve e rápida.
# O padrão é usar uma fixture do pytest que cria e destrói a sessão
# por módulo de teste — não recria para cada teste individual.
#
# Arquivo: tests/conftest.py

"""
# tests/conftest.py
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    \"\"\"
    SparkSession para testes — criada uma vez por sessão de testes.
    Configurada para ser leve: local[2], sem Hive, Delta incluído.
    \"\"\"
    spark = (
        SparkSession.builder
        .appName("test-suite")
        .master("local[2]")            # 2 threads — suficiente para testes
        .config("spark.sql.shuffle.partitions", "4")   # mínimo para testes
        .config("spark.sql.adaptive.enabled", "false") # desligar AQE em testes
        .config("spark.ui.enabled", "false")           # sem Spark UI em testes
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    yield spark
    spark.stop()  # destrói ao fim de todos os testes


# tests/test_meu_pipeline.py
from pyspark.sql import Row
from chispa import assert_df_equality


def test_filtro_status_ativo(spark):
    dados_entrada = [
        Row(id=1, status="ATIVO",    valor=100.0),
        Row(id=2, status="INATIVO",  valor=200.0),
        Row(id=3, status="ATIVO",    valor=300.0),
    ]
    df_entrada = spark.createDataFrame(dados_entrada)

    # Função que queremos testar
    from meu_pipeline import filtrar_ativos
    df_resultado = filtrar_ativos(df_entrada)

    dados_esperados = [
        Row(id=1, status="ATIVO", valor=100.0),
        Row(id=3, status="ATIVO", valor=300.0),
    ]
    df_esperado = spark.createDataFrame(dados_esperados)

    assert_df_equality(df_resultado, df_esperado, ignore_row_order=True)
"""

# COMMAND ----------

# MAGIC %md
# ## 7. Configurações por ambiente — padrão profissional

# COMMAND ----------

# ── Config dinâmica por ambiente ──────────────────────────────────────────
#
# Em produção, as configurações variam: desenvolvimento usa poucos recursos
# e é verboso, produção usa mais recursos e é otimizado para throughput.
# O padrão é carregar configs por ambiente a partir de variáveis de ambiente
# ou de um arquivo de configuração.

import os
from pyspark.sql import SparkSession

def criar_spark_session(app_name: str, env: str = None) -> SparkSession:
    """
    Cria SparkSession com configurações adequadas ao ambiente.

    Args:
        app_name: Nome descritivo do pipeline
        env: "development" | "staging" | "production"
             Se None, lê de ENVIRONMENT no .env

    Returns:
        SparkSession configurada
    """
    if env is None:
        env = os.environ.get("ENVIRONMENT", "development")

    # Configs base — aplicadas em todos os ambientes
    configs_base = {
        "spark.sql.adaptive.enabled":                  "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled":         "true",
    }

    # Configs por ambiente
    configs_env = {
        "development": {
            "spark.sql.shuffle.partitions":            "8",
            "spark.sql.autoBroadcastJoinThreshold":    "10485760",   # 10MB
            "spark.driver.memory":                     "2g",
        },
        "staging": {
            "spark.sql.shuffle.partitions":            "100",
            "spark.sql.autoBroadcastJoinThreshold":    "52428800",   # 50MB
            "spark.driver.memory":                     "4g",
        },
        "production": {
            "spark.sql.shuffle.partitions":            "200",
            "spark.sql.autoBroadcastJoinThreshold":    "104857600",  # 100MB
            "spark.driver.memory":                     "8g",
            "spark.databricks.delta.optimizeWrite.enabled": "true",
            "spark.databricks.delta.autoCompact.enabled":   "true",
        },
    }

    # Mesclar configs
    todas_configs = {**configs_base, **configs_env.get(env, configs_env["development"])}

    # Construir sessão
    builder = SparkSession.builder.appName(f"{app_name}[{env}]")
    for key, value in todas_configs.items():
        builder = builder.config(key, value)

    session = builder.getOrCreate()

    print(f"✅ SparkSession criada: {app_name} [{env}]")
    print(f"   Spark version: {session.version}")
    print(f"   shuffle.partitions: {session.conf.get('spark.sql.shuffle.partitions')}")

    return session


# Uso:
# spark = criar_spark_session("pipeline-pedidos-incremental")
# spark = criar_spark_session("pipeline-pedidos-incremental", env="production")

# COMMAND ----------

# MAGIC %md
# ## 8. spark.stop() — quando e por que usar

# COMMAND ----------

# ── spark.stop() ──────────────────────────────────────────────────────────
#
# spark.stop() encerra a SparkSession e libera todos os recursos do cluster.
# A JVM do Driver é encerrada após a chamada.

# Quando usar spark.stop():
#   ✅ Scripts standalone (não Databricks) — ao final do script
#   ✅ Testes unitários com pytest — após todos os testes (fixture teardown)
#   ✅ Processos long-running que criam múltiplas sessões sequenciais
#
# Quando NÃO usar spark.stop():
#   ❌ No Databricks — a sessão é gerenciada pelo cluster, não encerre
#   ❌ No meio de um pipeline — destruiria a sessão de todos os notebooks
#   ❌ Em aplicações interativas — perderia o estado em cache

# Padrão correto em script standalone:
"""
if __name__ == "__main__":
    spark = criar_spark_session("meu-pipeline")
    try:
        resultado = executar_pipeline(spark)
        print(f"✅ Pipeline concluído: {resultado}")
    except Exception as e:
        print(f"❌ Erro no pipeline: {e}")
        raise
    finally:
        spark.stop()   # sempre executado, mesmo com erro
"""

# COMMAND ----------

# MAGIC %md
# ## 9. Configs mais importantes — referência rápida

# COMMAND ----------

# ── Tabela de referência das configs essenciais ───────────────────────────

configs_referencia = [
    # (config, padrão, recomendação, motivo)
    ("spark.sql.shuffle.partitions",
     "200",
     "8-50 em dev / 200-800 em prod",
     "Controla paralelismo após shuffle. 200 é alto para datasets pequenos."),

    ("spark.sql.adaptive.enabled",
     "true (Spark 3.2+)",
     "Manter true sempre",
     "AQE reotimiza plano em runtime. Só benefícios."),

    ("spark.sql.adaptive.coalescePartitions.enabled",
     "true",
     "Manter true sempre",
     "Funde partições pequenas após shuffle automaticamente."),

    ("spark.sql.adaptive.skewJoin.enabled",
     "true",
     "Manter true sempre",
     "Divide partições com skew automaticamente."),

    ("spark.sql.autoBroadcastJoinThreshold",
     "10485760 (10MB)",
     "52428800 (50MB) se tiver memória",
     "Tabelas menores que isso viram BroadcastHashJoin (sem shuffle)."),

    ("spark.driver.memory",
     "1g",
     "4-8g dependendo do workload",
     "Memória da JVM do Driver. Aumentar para .collect() de dados grandes."),

    ("spark.executor.memory",
     "1g",
     "4-8g por executor",
     "Memória por Executor. Aumentar se ver spill to disk."),

    ("spark.memory.fraction",
     "0.6",
     "Manter 0.6 (padrão)",
     "Fração da heap para Execution + Storage pool unificado."),

    ("spark.databricks.delta.optimizeWrite.enabled",
     "false",
     "true em produção",
     "Databricks agrupa escritas em menos arquivos automaticamente."),

    ("spark.databricks.delta.autoCompact.enabled",
     "false",
     "true em produção",
     "Compacta arquivos pequenos automaticamente após escritas."),
]

print(f"{'Config':<55} {'Padrão':<20} {'Recomendação'}")
print("─" * 110)
for config, padrao, recomendacao, motivo in configs_referencia:
    print(f"{config:<55} {padrao:<20} {recomendacao}")
    print(f"  → {motivo}")
    print()

# COMMAND ----------

# MAGIC %md
# ## Resumo — o que fixar deste arquivo
#
# | Conceito | O que saber |
# |----------|-------------|
# | No Databricks | `spark` já existe, não precisa criar |
# | Fora do Databricks | `SparkSession.builder.appName().config().getOrCreate()` |
# | `getOrCreate()` | Retorna sessão existente se já houver — configs do builder são ignoradas |
# | Mudar config em runtime | `spark.conf.set("chave", "valor")` |
# | SparkContext | Acessado via `spark.sparkContext` — use para broadcast e accumulators |
# | `shuffle.partitions` | Padrão 200 — reduzir para 8-50 em desenvolvimento |
# | AQE | Ativar sempre — só benefícios |
# | `spark.stop()` | Apenas em scripts standalone — NUNCA no Databricks |
# | Testes | Fixture com `scope="session"` + `getOrCreate()` + `spark.stop()` no teardown |
#
# ---
#
# ### Conexão com a certificação Associate
#
# - **Domínio 2 (ELT com Spark):** SparkSession é a base de toda a API DataFrame
# - A prova não testa a criação da SparkSession diretamente (Databricks a cria)
# - Mas testa configurações como `shuffle.partitions`, `autoBroadcastJoinThreshold`
#   e o comportamento do AQE — todos configurados via `spark.conf.set()`
#
# ### Próximo arquivo
# `02_schema_types.py` — StructType, todos os tipos de dados, nullable e
# por que schema explícito é sempre melhor que inferência em produção.
