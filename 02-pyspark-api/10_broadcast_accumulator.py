# Databricks notebook source

# MAGIC %md
# # 10 — Broadcast Variables e Accumulators
#
# **Analogia:**
# Imagine um gerente de operações coordenando 50 equipes espalhadas pelo país.
#
# **Broadcast Variable** é como o gerente enviar o mesmo manual de procedimentos
# para todas as equipes UMA ÚNICA VEZ — cada equipe tem a própria cópia e consulta
# localmente sem precisar ligar para o gerente a cada dúvida. Se o gerente precisasse
# enviar o manual para cada funcionário individualmente a cada consulta, o telefone
# explodiria. Broadcast = distribuição eficiente de dados somente-leitura.
#
# **Accumulator** é como cada equipe enviar ao final do dia um contador de quantas
# entregas fez. O gerente soma tudo no final — ele nunca precisa entrar em contato
# com cada equipe durante o trabalho, só recebe os totais ao final.
# Accumulator = agregação eficiente de métricas dos Executors para o Driver.
#
# **Conceito técnico:**
# - **Broadcast Variable:** copia um objeto imutável para a memória de cada Executor
#   uma única vez via `SparkContext.broadcast()`. O objeto fica no Storage Memory
#   (não é reshuffled a cada task). Ideal para lookup tables, dicionários de configuração,
#   modelos pequenos de ML, listas de valores de referência.
# - **Accumulator:** variável compartilhada que os Executors só podem **incrementar**
#   (nunca ler). O Driver lê o valor acumulado via `.value`. Garante consistência
#   sem coordenação entre tasks. Ideal para contadores, somas, auditoria de pipeline.
#
# **Quando usar este conhecimento:**
# - Substituir closures com objetos grandes por Broadcast Variables
# - Implementar auditoria e contadores de qualidade em pipelines ETL
# - Entender por que Accumulators em transformações lazy podem dar resultados incorretos
# - Entrevistas sênior: comportamento de Accumulator em tasks re-executadas

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, pandas_udf, when, lit
from pyspark.sql.types import StringType, LongType, DoubleType, BooleanType
from pyspark import AccumulatorParam
import pandas as pd
import time

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# COMMAND ----------

# MAGIC %md
# ## PARTE 1 — BROADCAST VARIABLES

# COMMAND ----------

# MAGIC %md
# ## 1. O problema que Broadcast resolve

# COMMAND ----------

# MAGIC %md
# ### Sem Broadcast: closure serializado por task
#
# ```
# dicionario_grande = {"SP": "São Paulo", "RJ": "Rio", ...}  # 10 MB
#
# @udf(StringType())
# def nome_estado(sigla):
#     return dicionario_grande.get(sigla)  # dicionario capturado no closure
#
# Quando esta UDF roda em 100 tasks:
# → dicionario_grande é serializado e enviado 100 vezes (uma por task)
# → 100 × 10 MB = 1 GB de tráfego de rede desnecessário
# → Cada task desserializa o dicionário na memória → pressão de GC
# ```
#
# ### Com Broadcast: enviado 1x por Executor
#
# ```
# bc_dict = sc.broadcast(dicionario_grande)
#
# @udf(StringType())
# def nome_estado(sigla):
#     return bc_dict.value.get(sigla)  # acessa a cópia local do Executor
#
# Quando esta UDF roda em 100 tasks em 4 Executors:
# → dicionario_grande é enviado apenas 4 vezes (uma por Executor)
# → Fica em memória no Executor — todas as tasks do mesmo Executor compartilham
# → 4 × 10 MB = 40 MB de tráfego total
# ```

# COMMAND ----------

# MAGIC %md
# ## 2. Criando e usando Broadcast Variables

# COMMAND ----------

# Exemplo 1: dicionário de lookup (código → descrição)
tabela_ufs = {
    "SP": ("São Paulo",     "Sudeste", 45_919_049),
    "RJ": ("Rio de Janeiro","Sudeste", 16_054_524),
    "MG": ("Minas Gerais",  "Sudeste", 21_292_666),
    "RS": ("Rio Grande do Sul","Sul",   11_466_630),
    "BA": ("Bahia",         "Nordeste", 14_873_064),
    "PR": ("Paraná",        "Sul",      11_516_840),
    "PE": ("Pernambuco",    "Nordeste",  9_616_621),
    "CE": ("Ceará",         "Nordeste",  9_187_103),
}

# Broadcast: enviado 1x para cada Executor e mantido no Storage Memory
bc_ufs = sc.broadcast(tabela_ufs)

print(f"Tipo: {type(bc_ufs)}")
print(f"Acesso no Driver via .value: {bc_ufs.value.get('SP')}")

# COMMAND ----------

# Usando a Broadcast Variable em uma UDF
@udf(StringType())
def nome_estado(sigla):
    dados = bc_ufs.value.get(sigla)
    return dados[0] if dados else "Desconhecido"

@udf(StringType())
def regiao_estado(sigla):
    dados = bc_ufs.value.get(sigla)
    return dados[1] if dados else "Desconhecida"

@udf(LongType())
def populacao_estado(sigla):
    dados = bc_ufs.value.get(sigla)
    return dados[2] if dados else None

# Dataset de vendas por UF
df_vendas = spark.createDataFrame([
    (1, "SP", 15000.0), (2, "RJ",  8000.0), (3, "MG", 12000.0),
    (4, "RS",  6000.0), (5, "BA",  9000.0), (6, "XX",  1000.0),
    (7, "SP", 22000.0), (8, "PR",  7500.0), (9, "PE",  5000.0),
], ["pedido_id", "uf", "valor"])

df_enriquecido = (
    df_vendas
    .withColumn("estado",    nome_estado(col("uf")))
    .withColumn("regiao",    regiao_estado(col("uf")))
    .withColumn("populacao", populacao_estado(col("uf")))
)

df_enriquecido.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ## 3. Broadcast com Pandas UDF — versão vetorizada

# COMMAND ----------

# Para performance máxima: Broadcast Variable + Pandas UDF (sem Pickle por linha)
@pandas_udf(StringType())
def nome_estado_pandas(serie: pd.Series) -> pd.Series:
    lookup = bc_ufs.value  # acessa a cópia local do Executor
    return serie.map(lambda sigla: lookup.get(sigla, ("Desconhecido",))[0])

@pandas_udf(StringType())
def regiao_pandas(serie: pd.Series) -> pd.Series:
    lookup = bc_ufs.value
    return serie.map(lambda sigla: lookup.get(sigla, (None, "Desconhecida"))[1])

df_pandas_enriched = (
    df_vendas
    .withColumn("estado", nome_estado_pandas(col("uf")))
    .withColumn("regiao", regiao_pandas(col("uf")))
)
df_pandas_enriched.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ## 4. Broadcast de DataFrame vs sc.broadcast()

# COMMAND ----------

# MAGIC %md
# ```
# broadcast(df) — hint para o Catalyst:
# ┌──────────────────────────────────────────────────────────────────────┐
# │  · Usado em joins: df_grande.join(broadcast(df_pequeno), "chave")   │
# │  · O Catalyst serializa o DataFrame inteiro e envia para os Executors│
# │  · Gerenciado pelo BroadcastExchange no Physical Plan               │
# │  · Spark coleta via Driver antes de distribuir                      │
# │  · Threshold automático: spark.sql.autoBroadcastJoinThreshold       │
# └──────────────────────────────────────────────────────────────────────┘
#
# sc.broadcast(objeto) — broadcast manual de objeto Python:
# ┌──────────────────────────────────────────────────────────────────────┐
# │  · Usado para dicionários, listas, modelos, configs dentro de UDFs  │
# │  · Você controla quando criar, atualizar e destruir                 │
# │  · Fica no Storage Memory de cada Executor                         │
# │  · Não é gerenciado pelo Catalyst — é um objeto Python puro        │
# └──────────────────────────────────────────────────────────────────────┘
# ```

# COMMAND ----------

# Broadcast de DataFrame para join (revisto do módulo de joins)
from pyspark.sql.functions import broadcast

df_estados = spark.createDataFrame(
    [(k, v[0], v[1]) for k, v in tabela_ufs.items()],
    ["uf", "estado", "regiao"]
)

# broadcast() no join → BroadcastHashJoin (sem shuffle do lado grande)
df_join_bc = df_vendas.join(broadcast(df_estados), on="uf", how="left")
print("=== BroadcastHashJoin via hint broadcast() ===")
df_join_bc.explain(mode="simple")
df_join_bc.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ### Comparativo: sc.broadcast() vs broadcast() em join
#
# | Aspecto | `sc.broadcast(objeto)` | `broadcast(df)` em join |
# |---|---|---|
# | Uso | Dentro de UDFs, mapPartitions | Joins DataFrame |
# | Tipo | Qualquer objeto Python serializável | DataFrame Spark |
# | Gerenciamento | Manual (você chama `.unpersist()`) | Automático pelo Catalyst |
# | Visível no Physical Plan | Não | Sim (`BroadcastExchange`) |
# | Tamanho típico | Até ~500 MB por Executor | Até autoBroadcastJoinThreshold |
# | Re-broadcast | Não suporta update — precisa criar novo | Recalculado se o DF mudar |

# COMMAND ----------

# MAGIC %md
# ## 5. Ciclo de vida e gerenciamento de Broadcast Variables

# COMMAND ----------

# Broadcast variables ficam no Storage Memory de cada Executor até:
# 1. Serem explicitamente destruídas
# 2. O SparkContext ser encerrado

# Verificar quanto de memória está ocupado por broadcasts
print("RDDs/Broadcasts em memória:")
for info in sc._jsc.sc().getRDDStorageInfo():
    print(f"  ID: {info.id()} | "
          f"Memória: {info.memSize() / 1024:.1f} KB | "
          f"Nome: {info.name()}")

# COMMAND ----------

# unpersist(): remove dos Executors mas mantém a referência no Driver
# Útil quando você precisa liberar memória mas pode precisar re-broadcast
bc_ufs.unpersist()
print("Broadcast removida dos Executors (mas ainda referenciável no Driver)")

# destroy(): remove dos Executors E invalida a referência
# Após destroy(), qualquer uso de bc_ufs.value lança erro
# bc_ufs.destroy()  # ← descomente se quiser destruir completamente

# Re-criar broadcast com dados atualizados
tabela_ufs_v2 = dict(tabela_ufs)
tabela_ufs_v2["SC"] = ("Santa Catarina", "Sul", 7_609_601)

bc_ufs_v2 = sc.broadcast(tabela_ufs_v2)
print(f"Nova broadcast com SC: {bc_ufs_v2.value.get('SC')}")

# COMMAND ----------

# MAGIC %md
# ## 6. Broadcast de modelos de ML — padrão Iterator Pandas UDF

# COMMAND ----------

# Padrão de produção: modelo treinado no Driver, broadcast para os Executors
# Aqui simulamos com um objeto "modelo" — em produção seria sklearn, XGBoost, etc.

class ModeloSimulado:
    """Simula um modelo de classificação de risco"""
    def __init__(self, threshold_alto=8000, threshold_medio=4000):
        self.threshold_alto  = threshold_alto
        self.threshold_medio = threshold_medio

    def predict_batch(self, valores: pd.Series) -> pd.Series:
        return pd.Series(
            ["alto"  if v >= self.threshold_alto  else
             "medio" if v >= self.threshold_medio else
             "baixo"
             for v in valores]
        )

# "Treinamento" no Driver
modelo_treinado = ModeloSimulado(threshold_alto=10000, threshold_medio=5000)

# Broadcast para os Executors
bc_modelo = sc.broadcast(modelo_treinado)

# Iterator Pandas UDF: carrega o modelo 1x por partição (já está em broadcast)
from typing import Iterator

@pandas_udf(StringType())
def classificar_risco(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    modelo = bc_modelo.value  # acessa a cópia local do Executor
    for batch in iterator:
        yield modelo.predict_batch(batch)

df_risco = df_vendas.withColumn("risco", classificar_risco(col("valor")))
df_risco.show(truncate=False)

bc_modelo.unpersist()

# COMMAND ----------

# MAGIC %md
# ---
# ## PARTE 2 — ACCUMULATORS

# COMMAND ----------

# MAGIC %md
# ## 7. O que são Accumulators e quando usar

# COMMAND ----------

# MAGIC %md
# ### Fluxo de comunicação com Accumulators
#
# ```
# ┌─────────────────────────────────────────────────────────────────────┐
# │                         DRIVER                                      │
# │  acc = sc.accumulator(0)       ← cria o acumulador com valor inicial│
# │  ...                                                                │
# │  acc.value                     ← lê o valor acumulado (somente aqui)│
# └───────────────┬────────────────────────────────────────────────────┘
#                 │ enviado para cada Executor junto com a task
#                 ▼
# ┌──────────────────────────────────────────────────────────────────┐
# │  Executor 1            Executor 2            Executor 3          │
# │  acc += 5              acc += 3              acc += 7            │
# │  acc += 2              acc += 8              acc += 1            │
# │                                                                  │
# │  · Executors só ESCREVEM (+=) — nunca leem                      │
# │  · Cada Executor envia seu total parcial ao Driver via heartbeat │
# └───────────────┬──────────────────────────────────────────────────┘
#                 │ parciais enviados ao Driver
#                 ▼
# ┌─────────────────────────────────────────────────────────────────────┐
# │  DRIVER soma todos os parciais: 5+2+3+8+7+1 = 26                   │
# │  acc.value → 26                                                     │
# └─────────────────────────────────────────────────────────────────────┘
#
# Garantia: cada task contribui exatamente 1x para o acumulador
# (task re-executada contribui novamente — ver seção 10)
# ```

# COMMAND ----------

# MAGIC %md
# ## 8. Accumulators built-in: Long, Double, Collection

# COMMAND ----------

# Accumulator Long (inteiro) — o mais comum
acc_total_registros  = sc.accumulator(0)
acc_registros_validos = sc.accumulator(0)
acc_registros_nulos  = sc.accumulator(0)
acc_valor_total      = sc.accumulator(0.0)  # Double Accumulator

print(f"Acumuladores criados:")
print(f"  total_registros  = {acc_total_registros.value}")
print(f"  registros_validos = {acc_registros_validos.value}")
print(f"  registros_nulos  = {acc_registros_nulos.value}")
print(f"  valor_total      = {acc_valor_total.value}")

# COMMAND ----------

# Usando accumulators em uma operação de ETL com auditoria
df_com_nulos = spark.createDataFrame([
    (1, "SP", 15000.0),
    (2, "RJ",  None),       # valor nulo
    (3, "MG", 12000.0),
    (4,  None, 8000.0),     # UF nula
    (5, "BA",  9000.0),
    (6, "SP", -500.0),      # valor negativo — inválido
    (7, "PR",  7500.0),
], ["pedido_id", "uf", "valor"])

def auditar_e_filtrar(row):
    """Processa cada linha, incrementa accumulators e retorna linha limpa ou None"""
    acc_total_registros.add(1)

    # Verificar qualidade dos dados
    if row.uf is None or row.valor is None:
        acc_registros_nulos.add(1)
        return None

    if row.valor <= 0:
        acc_registros_nulos.add(1)
        return None

    acc_registros_validos.add(1)
    acc_valor_total.add(float(row.valor))
    return row

# Executar a auditoria via foreachPartition (mais eficiente que foreach)
resultados_validos = []

def processar_particao(iterator):
    for row in iterator:
        resultado = auditar_e_filtrar(row)
        if resultado is not None:
            resultados_validos.append(resultado)

# Action para disparar a execução e popular os accumulators
df_com_nulos.foreach(auditar_e_filtrar)

print("\n=== RELATÓRIO DE AUDITORIA DO PIPELINE ===")
print(f"  Total de registros processados : {acc_total_registros.value}")
print(f"  Registros válidos              : {acc_registros_validos.value}")
print(f"  Registros com problemas        : {acc_registros_nulos.value}")
print(f"  Valor total dos válidos        : R$ {acc_valor_total.value:,.2f}")
print(f"  Taxa de qualidade              : "
      f"{acc_registros_validos.value / acc_total_registros.value * 100:.1f}%")

# COMMAND ----------

# MAGIC %md
# ## 9. Accumulator customizado — AccumulatorParam

# COMMAND ----------

# MAGIC %md
# Accumulators customizados permitem acumular estruturas mais complexas:
# dicionários, listas, conjuntos — qualquer estrutura que suporte uma operação
# associativa e comutativa (para garantir resultados corretos em paralelo).

# COMMAND ----------

# Accumulator de dicionário: conta ocorrências por categoria
class DictAccumulatorParam(AccumulatorParam):
    """Acumula contagens em um dicionário {chave: contagem}"""

    def zero(self, initialValue):
        """Valor inicial — retornado quando o accumulator é criado"""
        return dict(initialValue)

    def addInPlace(self, dict1, dict2):
        """
        Combina dois dicionários somando os valores das chaves em comum.
        Chamado tanto nos Executors (parcial) quanto no Driver (merge final).
        Deve modificar dict1 in-place e retorná-lo.
        """
        for k, v in dict2.items():
            dict1[k] = dict1.get(k, 0) + v
        return dict1

# Criar o accumulator com valor inicial vazio
acc_por_uf = sc.accumulator({}, DictAccumulatorParam())
acc_erros_por_tipo = sc.accumulator({}, DictAccumulatorParam())

# COMMAND ----------

# Usar o accumulator customizado
def auditar_com_dict(row):
    """Conta registros por UF e classifica erros por tipo"""
    # Contar por UF
    if row.uf is not None:
        acc_por_uf.add({row.uf: 1})

    # Classificar erros
    if row.valor is None:
        acc_erros_por_tipo.add({"valor_nulo": 1})
    elif row.valor <= 0:
        acc_erros_por_tipo.add({"valor_invalido": 1})

    if row.uf is None:
        acc_erros_por_tipo.add({"uf_nula": 1})

df_com_nulos.foreach(auditar_com_dict)

print("Contagem por UF:")
for uf, qtd in sorted(acc_por_uf.value.items()):
    print(f"  {uf}: {qtd} pedido(s)")

print("\nErros por tipo:")
for tipo, qtd in sorted(acc_erros_por_tipo.value.items()):
    print(f"  {tipo}: {qtd} ocorrência(s)")

# COMMAND ----------

# Accumulator de conjunto (set) — para coletar valores únicos
class SetAccumulatorParam(AccumulatorParam):
    """Acumula valores únicos em um set"""

    def zero(self, initialValue):
        return set(initialValue)

    def addInPlace(self, set1, set2):
        set1.update(set2)
        return set1

acc_ufs_processadas = sc.accumulator(set(), SetAccumulatorParam())

def registrar_uf(row):
    if row.uf is not None:
        acc_ufs_processadas.add({row.uf})

df_com_nulos.foreach(registrar_uf)
print(f"\nUFs únicas processadas: {sorted(acc_ufs_processadas.value)}")

# COMMAND ----------

# MAGIC %md
# ## 10. ⚠️ A armadilha mais importante: Accumulators em transformações lazy

# COMMAND ----------

# MAGIC %md
# ### O problema
#
# ```
# acc = sc.accumulator(0)
#
# # ERRADO: accumulator em transformação (lazy — pode ser executado mais de uma vez)
# df_transformado = df.filter(lambda row: acc.add(1) or True)  # transformação!
# df_transformado.count()   # Job 1: acc = N
# df_transformado.show()    # Job 2: acc = 2N  ← transformação executada novamente!
#
# # CORRETO: accumulator em action (executado exatamente 1x por job)
# df.foreach(lambda row: acc.add(1))   # action → executado exatamente 1x
# ```

# COMMAND ----------

# Demonstração do problema — transformação vs action
acc_demo_transformacao = sc.accumulator(0)
acc_demo_action        = sc.accumulator(0)

df_demo = spark.range(100)

# Accumulator em transformação (map é lazy)
df_com_acc = df_demo.rdd.map(lambda x: (acc_demo_transformacao.add(1), x)[1])

print("Antes de qualquer action:")
print(f"  acc_transformacao = {acc_demo_transformacao.value}")  # 0

# Primeira action — executa o map uma vez
df_com_acc.count()
print(f"\nApós primeiro count():")
print(f"  acc_transformacao = {acc_demo_transformacao.value}")  # 100

# Segunda action — executa o map NOVAMENTE (sem cache)
df_com_acc.count()
print(f"\nApós segundo count():")
print(f"  acc_transformacao = {acc_demo_transformacao.value}")  # 200! (duplicado!)

# COMMAND ----------

# Solução 1: usar cache antes de iterar múltiplas vezes
acc_correto = sc.accumulator(0)
df_cacheado = df_demo.cache()
df_com_acc_cacheado = df_cacheado.rdd.map(lambda x: (acc_correto.add(1), x)[1])

df_com_acc_cacheado.count()  # executa e materializa cache
print(f"\nAcc com cache após primeiro count: {acc_correto.value}")  # 100

df_com_acc_cacheado.count()  # lê do cache — map NÃO é reexecutado
# ⚠️ Ainda pode variar dependendo do StorageLevel e da serialização da função

df_cacheado.unpersist()

# COMMAND ----------

# Solução 2 (recomendada): accumulator em action, não em transformação
acc_recomendado = sc.accumulator(0)

# Fazer o processamento em uma única action
df_demo.foreach(lambda row: acc_recomendado.add(1))
print(f"Acc em action: {acc_recomendado.value}")  # sempre 100

# COMMAND ----------

# MAGIC %md
# ### O problema das tasks re-executadas (falha + retry)
#
# ```
# Speculative execution ou task failure + retry:
#
# Task 5 começa a executar → acc += 50
# Task 5 fica lenta → Spark lança cópia especulativa
# Cópia especulativa completa → acc += 50 (novamente!)
# Task original ainda termina → acc += 50 (terceira vez!)
#
# Resultado: acc pode ser maior que o esperado
#
# Regra:
# · Para ações idempotentes (contar, somar) → ok em produção,
#   mas pode ter leve overcounting em caso de retry
# · Para lógica crítica que não pode ter duplicação →
#   use Delta Lake MERGE ou deduplicação por chave
# · Accumulators são para OBSERVABILIDADE (logs, métricas),
#   não para lógica de negócio crítica
# ```

# COMMAND ----------

# MAGIC %md
# ## 11. Padrão de produção: pipeline ETL com auditoria completa

# COMMAND ----------

# Padrão completo: Broadcast + Accumulators em um pipeline de produção
print("=" * 60)
print("  PIPELINE ETL COM BROADCAST E AUDITORIA VIA ACCUMULATORS")
print("=" * 60)

# --- CONFIGURAÇÃO ---
# Tabela de referência via Broadcast (evita join ou closure grande)
regras_validacao = {
    "valor_minimo":  0.01,
    "valor_maximo":  1_000_000.0,
    "ufs_validas":   {"SP", "RJ", "MG", "RS", "BA", "PR", "PE", "CE", "SC", "GO"},
}
bc_regras = sc.broadcast(regras_validacao)

# Accumulators para observabilidade do pipeline
acc_total     = sc.accumulator(0)
acc_ok        = sc.accumulator(0)
acc_uf_inv    = sc.accumulator(0)
acc_val_inv   = sc.accumulator(0)
acc_nulo      = sc.accumulator(0)

# --- DATASET ---
df_pipeline = spark.createDataFrame([
    (1,  "SP",  15000.0), (2,  "RJ",   8000.0), (3,  "XX",   500.0),
    (4,  "MG",  -100.0),  (5,  "BA",   9000.0), (6,  None,   200.0),
    (7,  "SP",  22000.0), (8,  "RS",      0.0), (9,  "PR",  7500.0),
    (10, "CE",   None),   (11, "SC",  12000.0), (12, "GO",   4500.0),
], ["id", "uf", "valor"])

# --- LÓGICA DE VALIDAÇÃO ---
def validar_registro(row):
    """Valida cada registro e incrementa os accumulators correspondentes"""
    regras = bc_regras.value
    acc_total.add(1)

    if row.uf is None or row.valor is None:
        acc_nulo.add(1)
        return False

    if row.uf not in regras["ufs_validas"]:
        acc_uf_inv.add(1)
        return False

    if not (regras["valor_minimo"] <= row.valor <= regras["valor_maximo"]):
        acc_val_inv.add(1)
        return False

    acc_ok.add(1)
    return True

# --- EXECUÇÃO ---
# Action para rodar a auditoria
df_pipeline.foreach(validar_registro)

# --- RELATÓRIO ---
total = acc_total.value
print(f"\n{'Métrica':<35} {'Valor':>10}  {'%':>8}")
print("-" * 58)
print(f"{'Total processado':<35} {total:>10}")
print(f"{'✅ Válidos':<35} {acc_ok.value:>10}  "
      f"{acc_ok.value/total*100:>7.1f}%")
print(f"{'❌ UF inválida':<35} {acc_uf_inv.value:>10}  "
      f"{acc_uf_inv.value/total*100:>7.1f}%")
print(f"{'❌ Valor inválido/zero/neg':<35} {acc_val_inv.value:>10}  "
      f"{acc_val_inv.value/total*100:>7.1f}%")
print(f"{'❌ Campos nulos':<35} {acc_nulo.value:>10}  "
      f"{acc_nulo.value/total*100:>7.1f}%")
print("-" * 58)
print(f"{'Taxa de qualidade':<35} {'':>10}  "
      f"{acc_ok.value/total*100:>7.1f}%")

bc_regras.unpersist()

# COMMAND ----------

# MAGIC %md
# ## 12. Referência rápida de configurações relacionadas

# COMMAND ----------

configs_bc_acc = {
    "spark.broadcast.blockSize":
        "Tamanho dos blocos de broadcast (default: 4096 KB)",
    "spark.sql.broadcastTimeout":
        "Timeout para broadcast chegar nos Executors (default: 300s)",
    "spark.sql.autoBroadcastJoinThreshold":
        "Tamanho máximo para broadcast automático em joins (default: 10 MB)",
    "spark.executor.heartbeatInterval":
        "Frequência de envio de métricas (inclui parciais de acc) ao Driver (default: 10s)",
}

print(f"\n{'Configuração':<45} {'Valor Atual':<15} {'Descrição'}")
print("=" * 120)
for config, descricao in configs_bc_acc.items():
    try:
        valor = spark.conf.get(config)
    except Exception:
        valor = "(default)"
    print(f"{config:<45} {valor:<15} {descricao}")

# COMMAND ----------

# MAGIC %md
# ## ⚠️ Resumo de armadilhas e pontos de prova
#
# | Tema | O que saber |
# |---|---|
# | `sc.broadcast()` vs `broadcast(df)` | `sc.broadcast()` = objeto Python em UDFs. `broadcast(df)` = hint de join para o Catalyst |
# | Broadcast é somente-leitura | Executors nunca modificam — só leem via `.value` |
# | Accumulator: só Executors escrevem | Driver NUNCA incrementa (só lê via `.value`) — Executors nunca leem |
# | Accumulator em transformação lazy | Pode ser executado N vezes (uma por action) → overcounting. Use em actions |
# | Accumulator com task retry | Task re-executada incrementa de novo → overcounting. Use para observabilidade, não lógica crítica |
# | `.unpersist()` vs `.destroy()` | `unpersist`: remove dos Executors, mantém referência. `destroy`: invalida completamente |
# | AccumulatorParam customizado | Precisa implementar `zero()` (valor inicial) e `addInPlace()` (merge associativo e comutativo) |
# | Broadcast no Storage Memory | Ocupa Storage Memory do Executor — libere com `.unpersist()` quando não precisar mais |
# | Closure grande vs Broadcast | Objeto capturado em UDF é serializado por task. Broadcast é enviado 1x por Executor |
# | Accumulator para observabilidade | Padrão correto: contadores, somas, métricas de qualidade — não para lógica de negócio |

# COMMAND ----------
