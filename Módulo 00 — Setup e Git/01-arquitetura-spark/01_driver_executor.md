# Databricks notebook source

# MAGIC %md
# # 01 — Driver, Executors, Slots e Heartbeat
#
# **Analogia:**
# Imagine um canteiro de obras. O **Driver** é o engenheiro-chefe: ele tem a planta do projeto,
# sabe o que precisa ser feito e distribui tarefas. Os **Executors** são as equipes de pedreiros:
# cada equipe tem um número fixo de trabalhadores (slots) e executa o que o engenheiro manda.
# O **heartbeat** é o rádio de comunicação — se uma equipe para de responder, o engenheiro
# assume que algo deu errado e pode realocar o trabalho.
#
# **Conceito técnico:**
# O Spark roda em um modelo mestre/trabalhador. O **Driver** é um processo JVM que hospeda o
# `SparkContext`, o DAG Scheduler e o Task Scheduler. Os **Executors** são processos JVM
# separados, rodando nos nós worker, responsáveis por executar tasks e armazenar dados em cache.
# Cada Executor reporta seu status ao Driver periodicamente via **heartbeat**.
#
# **Quando usar este conhecimento:**
# - Ao dimensionar clusters (quanto dar de memória ao Driver vs Executors)
# - Ao depurar erros de OOM (OutOfMemory) — saber se o problema é no Driver ou no Executor
# - Ao interpretar o Spark UI — entender quem está fazendo o quê
# - Entrevistas e prova de certificação Databricks Associate/Professional

# COMMAND ----------

# MAGIC %md
# ## 1. O Driver — Cérebro do Spark

# COMMAND ----------

# O Driver hospeda o SparkContext e coordena toda a execução.
# Em Databricks, o Driver roda no nó "driver" do cluster.

# Para inspecionar configurações do Driver na sessão atual:
print("Driver host:", spark.conf.get("spark.driver.host"))
print("Driver port:", spark.conf.get("spark.driver.port"))

# Memória do Driver (configurada no cluster ou via spark-submit):
# spark.driver.memory          → heap JVM do Driver (default: 1g)
# spark.driver.memoryOverhead  → memória extra fora do heap (default: max(384m, 10% do heap))
# spark.driver.maxResultSize   → limite de dados que podem ser coletados no Driver (default: 1g)

# COMMAND ----------

# MAGIC %md
# ### ⚠️ Armadilha clássica: Driver OOM
#
# O Driver fica sem memória quando você traz dados grandes para ele com:
# - `df.collect()` — traz TODAS as linhas para a memória do Driver
# - `df.toPandas()` — idem
# - `df.show(n)` — seguro, busca apenas n linhas
#
# **Regra:** nunca use `collect()` em DataFrames grandes em produção.
# Use `write`, `show(n)` ou processe os dados nos Executors.

# COMMAND ----------

# Exemplo: como NÃO fazer em produção com dados grandes
# resultado = df.collect()  # ← traz tudo para o Driver → risco de OOM

# Como fazer certo:
# df.write.format("delta").save("/path/to/output")  # processa nos Executors
# df.show(20)                                        # só 20 linhas no Driver

# COMMAND ----------

# MAGIC %md
# ## 2. Os Executors — Mão de Obra do Spark

# COMMAND ----------

# Executors são processos JVM nos nós worker.
# Cada Executor tem:
#   - Memória própria (heap + overhead)
#   - Um número fixo de cores (slots de execução)
#   - Armazenamento local para cache e shuffle

# Para ver os Executors ativos:
sc = spark.sparkContext
print("Executors ativos:", len(sc._jsc.sc().statusTracker().getExecutorInfos()))

# Configurações principais dos Executors:
# spark.executor.memory          → heap JVM de cada Executor (ex: 4g, 8g)
# spark.executor.memoryOverhead  → memória fora do heap: Python workers, NIO buffers (default: max(384m, 10%))
# spark.executor.cores           → cores por Executor (controla paralelismo)
# spark.executor.instances       → número de Executors (em modo estático)

# COMMAND ----------

# MAGIC %md
# ### Unified Memory Model — como o Executor divide a memória
#
# ```
# ┌─────────────────────────────────────────────────────┐
# │              spark.executor.memory (ex: 8g)          │
# │                                                      │
# │  ┌──────────────────────────────────────────────┐   │
# │  │   Reserved Memory (~300 MB fixo — Spark sys)  │   │
# │  └──────────────────────────────────────────────┘   │
# │                                                      │
# │  ┌──────────────────────────────────────────────┐   │
# │  │   Usable Memory = total - 300MB               │   │
# │  │                                               │   │
# │  │  spark.memory.fraction (default: 0.6)         │   │
# │  │  ┌──────────────────────────────────────┐    │   │
# │  │  │ Unified Memory Pool (60% do usable)  │    │   │
# │  │  │                                      │    │   │
# │  │  │  ├─ Execution Memory  (shuffle,sort) │    │   │
# │  │  │  └─ Storage Memory    (cache, bcast) │    │   │
# │  │  │                                      │    │   │
# │  │  │  Os dois compartilham e se emprestam │    │   │
# │  │  └──────────────────────────────────────┘    │   │
# │  │                                               │   │
# │  │  spark.memory.storageFraction (default: 0.5)  │   │
# │  │  └─ parte do pool protegida para Storage      │   │
# │  │                                               │   │
# │  │  User Memory (40% do usable)                  │   │
# │  │  └─ estruturas Python/Scala do seu código     │   │
# │  └──────────────────────────────────────────────┘   │
# └─────────────────────────────────────────────────────┘
#
# + spark.executor.memoryOverhead (fora do heap JVM)
#   └─ Python workers (PySpark), NIO buffers, overhead SO
# ```

# COMMAND ----------

# MAGIC %md
# ## 3. Slots — O Paralelismo Real

# COMMAND ----------

# Um "slot" é a capacidade de executar 1 task por vez.
# Cada core de um Executor = 1 slot.
#
# Total de slots do cluster = (número de Executors) × (cores por Executor)
#
# Exemplo:
#   - 4 Executors × 4 cores cada = 16 slots = 16 tasks simultâneas

# Para ver o número de slots disponíveis:
num_executors = len(sc._jsc.sc().statusTracker().getExecutorInfos()) - 1  # -1 exclui o Driver
cores_por_executor = int(spark.conf.get("spark.executor.cores", "1"))
total_slots = num_executors * cores_por_executor
print(f"Executors: {num_executors}")
print(f"Cores por Executor: {cores_por_executor}")
print(f"Total de slots (paralelismo): {total_slots}")

# COMMAND ----------

# MAGIC %md
# ### Relação entre slots e partições
#
# | Situação | Comportamento |
# |---|---|
# | partições < slots | Alguns slots ficam ociosos — cluster subutilizado |
# | partições = slots | Uso ideal — todas as tasks rodam em paralelo |
# | partições >> slots | Tasks ficam em fila — normal e esperado |
#
# **Regra prática:** mire em 2–4× mais partições do que slots.
# Com 16 slots → 32 a 64 partições é um bom ponto de partida.

# Verificar o paralelismo padrão configurado:
print("spark.default.parallelism:", spark.conf.get("spark.default.parallelism", "não definido"))
print("spark.sql.shuffle.partitions:", spark.conf.get("spark.sql.shuffle.partitions"))

# Em Databricks com AQE ativado, o número de partições de shuffle
# é ajustado automaticamente — não precisa setar manualmente na maioria dos casos.

# COMMAND ----------

# MAGIC %md
# ## 4. Heartbeat — Comunicação entre Executor e Driver

# COMMAND ----------

# O Executor envia um sinal (heartbeat) ao Driver a cada intervalo.
# O heartbeat informa:
#   - Que o Executor ainda está vivo
#   - Métricas de progresso das tasks em execução (linhas processadas, bytes lidos/escritos)
#   - Status de uso de memória

# Configurações de heartbeat:
# spark.executor.heartbeatInterval  → frequência do heartbeat (default: 10s)
# spark.network.timeout             → tempo máximo sem resposta antes de considerar morto (default: 120s)
#                                     deve ser MAIOR que heartbeatInterval
# spark.executor.heartbeat.maxFailures → tentativas antes de remover o Executor (default: 2? depende versão)

print("Heartbeat interval:", spark.conf.get("spark.executor.heartbeatInterval"))
print("Network timeout:", spark.conf.get("spark.network.timeout"))

# COMMAND ----------

# MAGIC %md
# ### ⚠️ Quando o heartbeat falha
#
# Se o Driver não recebe heartbeat de um Executor dentro de `spark.network.timeout`:
# 1. O Driver marca o Executor como perdido (lost executor)
# 2. As tasks daquele Executor são reagendadas em outros Executors
# 3. Se estava em modo de falha total, o job pode falhar com `ExecutorLostFailure`
#
# **Causas comuns de perda de Executor:**
# - Executor ficou sem memória → processo JVM morreu
# - GC muito longo → Executor não conseguiu enviar heartbeat a tempo
# - Problema de rede ou nó worker encerrado (spot instance preemptada)
#
# **O que você verá no Spark UI:**
# - Aba "Executors" → Executor com status "Dead" ou "Removed"
# - Aba "Stages" → tasks com status "FAILED" e "RESUBMITTED"

# COMMAND ----------

# MAGIC %md
# ## 5. Fluxo Completo — Do código ao cluster

# COMMAND ----------

# MAGIC %md
# ```
# Seu código Python/SQL
#         │
#         ▼
# ┌───────────────────────────────┐
# │           DRIVER              │
# │                               │
# │  SparkContext                 │
# │  ├─ DAG Scheduler             │  ← converte transformações em Stages
# │  ├─ Task Scheduler            │  ← envia Tasks para os Executors
# │  └─ Block Manager (driver)    │  ← coordena broadcast e resultados
# └───────────────┬───────────────┘
#                 │  Tasks + Dados de Shuffle
#     ┌───────────┼───────────┐
#     │           │           │
#     ▼           ▼           ▼
# ┌───────┐   ┌───────┐   ┌───────┐
# │Exec 1 │   │Exec 2 │   │Exec 3 │
# │       │   │       │   │       │
# │ slot1 │   │ slot1 │   │ slot1 │  ← cada slot = 1 task simultânea
# │ slot2 │   │ slot2 │   │ slot2 │
# │ slot3 │   │ slot3 │   │ slot3 │
# │ slot4 │   │ slot4 │   │ slot4 │
# └───┬───┘   └───┬───┘   └───┬───┘
#     │           │           │
#     └───────────┴───────────┘
#                 │  Heartbeat (a cada 10s)
#                 ▼
#              DRIVER ← recebe métricas e resultados parciais
# ```

# COMMAND ----------

# MAGIC %md
# ## 6. Configurações de referência rápida

# COMMAND ----------

# Imprimir as configurações mais relevantes do cluster atual
configs_para_checar = [
    "spark.driver.memory",
    "spark.driver.memoryOverhead",
    "spark.driver.maxResultSize",
    "spark.executor.memory",
    "spark.executor.memoryOverhead",
    "spark.executor.cores",
    "spark.executor.heartbeatInterval",
    "spark.network.timeout",
    "spark.memory.fraction",
    "spark.memory.storageFraction",
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.enabled",
]

print("=" * 55)
print(f"{'Configuração':<40} {'Valor':>12}")
print("=" * 55)
for config in configs_para_checar:
    try:
        valor = spark.conf.get(config)
    except Exception:
        valor = "(não definido)"
    print(f"{config:<40} {valor:>12}")
print("=" * 55)

# COMMAND ----------

# MAGIC %md
# ## ⚠️ Resumo de armadilhas e pontos de prova
#
# | Tema | O que saber |
# |---|---|
# | `collect()` | Traz TUDO para o Driver → OOM se DataFrame grande |
# | Driver memory | Separado da memória dos Executors — configurar diferente |
# | Slots | `executors × cores` — define paralelismo real |
# | Partições vs Slots | Ideal: 2–4× mais partições que slots |
# | Heartbeat timeout | `network.timeout` deve ser maior que `heartbeatInterval` |
# | Executor perdido | Tasks são resubmitidas — job não falha imediatamente |
# | MemoryOverhead | PySpark precisa de overhead maior que Scala — sempre considere |
# | Unified Memory | Execution e Storage compartilham e se emprestam dinamicamente |

# COMMAND ----------
