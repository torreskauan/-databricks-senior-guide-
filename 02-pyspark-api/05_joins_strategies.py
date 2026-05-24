# Databricks notebook source

# MAGIC %md
# # 05 — Join Strategies: Todos os Joins, Broadcast, Semi e Anti
#
# **Analogia:**
# Imagine duas listas: a lista de **pedidos** (esquerda) e a lista de **clientes** (direita).
#
# - **inner:** só aparecem pedidos COM cliente cadastrado (interseção)
# - **left:** todos os pedidos, mesmo sem cliente (cliente vem nulo se não existir)
# - **right:** todos os clientes, mesmo sem pedido (pedido vem nulo se não existir)
# - **full outer:** todos dos dois lados, nulos onde não há correspondência
# - **left semi:** lista de pedidos que TÊM cliente — mas sem trazer dados do cliente
# - **left anti:** lista de pedidos que NÃO TÊM cliente — os órfãos
# - **cross:** todo pedido combinado com todo cliente — produto cartesiano
#
# **Conceito técnico:**
# O Spark suporta 7 tipos de join lógico. Cada um mapeia para uma estratégia física
# dependendo do tamanho dos DataFrames: BHJ (broadcast), SMJ (sort-merge) ou SHJ
# (shuffle-hash). Semi e anti são operações de filtragem — não duplicam colunas.
# A escolha da estratégia física (BHJ, SMJ, SHJ) foi coberta em `04_physical_plan_joins.py`.
# Aqui o foco é nos **tipos lógicos** e seus comportamentos, com ênfase em semi/anti.
#
# **Quando usar este conhecimento:**
# - Modelagem de pipelines ETL (deduplicação, enriquecimento, validação)
# - Diagnóstico de duplicação inesperada de linhas após joins
# - Escolher o join certo para cada padrão (semi/anti para filtros de existência)
# - Entrevistas sênior e prova Databricks Associate/Professional

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, count, lit, coalesce
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Datasets base para todos os exemplos
clientes = spark.createDataFrame([
    (1, "Ana",     "SP", "Gold"),
    (2, "Bruno",   "RJ", "Silver"),
    (3, "Carla",   "MG", "Gold"),
    (4, "Diana",   "SP", "Bronze"),
    (5, "Eduardo", "RS", "Silver"),
    # cliente 6 não existe → aparecerá em anti joins de pedidos
], ["cliente_id", "nome", "estado", "tier"])

pedidos = spark.createDataFrame([
    (101, 1, 1500.0, "2024-01"),   # Ana
    (102, 1, 2000.0, "2024-02"),   # Ana (segundo pedido)
    (103, 2,  800.0, "2024-01"),   # Bruno
    (104, 3, 3200.0, "2024-02"),   # Carla
    (105, 6,  500.0, "2024-01"),   # cliente 6 → não existe em clientes
    (106, 6,  700.0, "2024-02"),   # cliente 6 → não existe em clientes
    # clientes 4 e 5 não têm pedidos → aparecerão em right/full/anti de clientes
], ["pedido_id", "cliente_id", "valor", "mes"])

print("=== CLIENTES ===")
clientes.show()
print("=== PEDIDOS ===")
pedidos.show()

# COMMAND ----------

# MAGIC %md
# ## 1. INNER JOIN — Apenas correspondências

# COMMAND ----------

# MAGIC %md
# ```
# Pedidos:  101(c1), 102(c1), 103(c2), 104(c3), 105(c6), 106(c6)
# Clientes: c1, c2, c3, c4, c5
#
# Inner join → apenas onde cliente_id existe nos dois lados:
# Resultado: 101(c1), 102(c1), 103(c2), 104(c3)
# Excluídos: 105(c6), 106(c6) [cliente não existe]
#            c4, c5 [sem pedidos — não entram]
# ```

# COMMAND ----------

inner = pedidos.join(clientes, on="cliente_id", how="inner")
# ou: how="inner" é o default — pode omitir
print(f"Inner join: {inner.count()} linhas (esperado: 4)")
inner.show()

# COMMAND ----------

# MAGIC %md
# ### ⚠️ Multiplicação de linhas no inner join
#
# Se a chave não é única em AMBOS os lados → o join gera produto cartesiano parcial.
# Ana (c1) tem 2 pedidos e 1 registro em clientes → 2 linhas no resultado.
# Se Ana tivesse 2 registros em clientes → geraria 2 × 2 = 4 linhas (duplicação silenciosa).

# COMMAND ----------

# Demonstrar multiplicação: o que acontece com chave duplicada no lado direito
clientes_dup = clientes.union(
    spark.createDataFrame([(1, "Ana_duplicada", "SP", "Platinum")],
                          ["cliente_id", "nome", "estado", "tier"])
)

inner_dup = pedidos.filter(col("cliente_id") == 1).join(clientes_dup, on="cliente_id")
print(f"Com cliente duplicado: {inner_dup.count()} linhas (era 2, virou 4 — produto cartesiano!)")
inner_dup.show()

# COMMAND ----------

# MAGIC %md
# ## 2. LEFT JOIN (LEFT OUTER) — Todos do lado esquerdo

# COMMAND ----------

# MAGIC %md
# ```
# Todos os pedidos aparecem.
# Para os pedidos de c6 (não existe em clientes): campos do cliente = null
# ```

# COMMAND ----------

left = pedidos.join(clientes, on="cliente_id", how="left")
# equivalente: how="left_outer"
print(f"Left join: {left.count()} linhas (esperado: 6 — todos os pedidos)")
left.show()

# As linhas 105 e 106 (cliente_id=6) aparecem com nome, estado, tier = null

# COMMAND ----------

# MAGIC %md
# ### Padrão: identificar órfãos com left join + filtro de null

# COMMAND ----------

# Pedidos sem cliente cadastrado → identificar registros inconsistentes
pedidos_sem_cliente = (
    pedidos
    .join(clientes, on="cliente_id", how="left")
    .filter(col("nome").isNull())           # null = não encontrou no lado direito
    .select("pedido_id", "cliente_id", "valor")
)
print("Pedidos sem cliente cadastrado:")
pedidos_sem_cliente.show()

# COMMAND ----------

# MAGIC %md
# ## 3. RIGHT JOIN (RIGHT OUTER) — Todos do lado direito

# COMMAND ----------

# MAGIC %md
# ```
# Todos os clientes aparecem.
# Para c4 e c5 (sem pedidos): campos de pedido = null
# ```

# COMMAND ----------

right = pedidos.join(clientes, on="cliente_id", how="right")
# equivalente: how="right_outer"
print(f"Right join: {right.count()} linhas")
right.orderBy("cliente_id").show()

# COMMAND ----------

# MAGIC %md
# ### Preferência de left sobre right
#
# Na prática, a maioria dos times prefere reescrever um right join como left join
# invertendo a ordem dos DataFrames — mais fácil de ler e raciocinar.

# Equivalente ao right acima:
left_equivalente = clientes.join(pedidos, on="cliente_id", how="left")
print(f"Left equivalente ao right: {left_equivalente.count()} linhas")

# COMMAND ----------

# MAGIC %md
# ## 4. FULL OUTER JOIN — Todos de ambos os lados

# COMMAND ----------

# MAGIC %md
# ```
# Todos os pedidos + todos os clientes.
# Nulos onde não há correspondência em nenhum dos lados.
# ```

# COMMAND ----------

full = pedidos.join(clientes, on="cliente_id", how="full")
# equivalente: how="full_outer" ou how="outer"
print(f"Full outer join: {full.count()} linhas")
full.orderBy(col("cliente_id").asc_nulls_last(), "pedido_id").show()

# COMMAND ----------

# MAGIC %md
# ### Padrão: reconciliação de duas fontes com full outer

# COMMAND ----------

# Identificar o que está em cada fonte e o que falta
full_com_status = (
    pedidos.join(clientes, on="cliente_id", how="full")
    .withColumn("status",
        when(col("pedido_id").isNull(), lit("cliente_sem_pedido"))
        .when(col("nome").isNull(),     lit("pedido_sem_cliente"))
        .otherwise(lit("ok"))
    )
)

print("Reconciliação pedidos × clientes:")
full_com_status.select("pedido_id", "cliente_id", "nome", "status").show()

# COMMAND ----------

# MAGIC %md
# ## 5. LEFT SEMI JOIN — Filtro de existência (com dados do esquerdo)

# COMMAND ----------

# MAGIC %md
# ```
# Semântica: "quais pedidos têm cliente cadastrado?"
#
# Diferença crítica vs INNER JOIN:
# ┌──────────────────────────────────────────────────────────┐
# │ INNER JOIN retorna todas as colunas de ambos os lados    │
# │ e MULTIPLICA linhas se a chave não for única             │
# │                                                          │
# │ LEFT SEMI retorna apenas colunas do lado ESQUERDO        │
# │ e NÃO multiplica — cada linha do esquerdo aparece 1x    │
# │ mesmo que haja múltiplas correspondências no direito     │
# └──────────────────────────────────────────────────────────┘
#
# Uso ideal: filtrar um DataFrame grande usando um DataFrame menor como "lista de válidos"
# ```

# COMMAND ----------

semi = pedidos.join(clientes, on="cliente_id", how="left_semi")
# equivalente: how="semi"
print(f"Left semi: {semi.count()} linhas (apenas pedidos COM cliente)")
semi.show()
# Somente colunas de pedidos — sem nome, estado, tier

# COMMAND ----------

# Demonstração: semi NÃO multiplica, inner multiplica
clientes_dup2 = clientes.union(
    spark.createDataFrame([(1, "Ana_copy", "SP", "Gold")],
                          ["cliente_id", "nome", "estado", "tier"])
)

inner_count = pedidos.join(clientes_dup2, "cliente_id", "inner").count()
semi_count  = pedidos.join(clientes_dup2, "cliente_id", "left_semi").count()

print(f"Inner join com chave duplicada no direito: {inner_count} linhas")
print(f"Semi join  com chave duplicada no direito: {semi_count} linhas (sem multiplicação!)")

# COMMAND ----------

# MAGIC %md
# ### Padrões de uso do Semi Join

# COMMAND ----------

# Padrão 1: filtrar uma tabela fato usando uma dimensão com critério
# "Quais pedidos são de clientes Gold ou Platinum?"
clientes_premium = clientes.filter(col("tier").isin("Gold", "Platinum"))

pedidos_premium = pedidos.join(
    broadcast(clientes_premium),
    on="cliente_id",
    how="left_semi"
)
print("Pedidos de clientes premium:")
pedidos_premium.show()

# COMMAND ----------

# Padrão 2: deduplicação baseada em chave de outra tabela
# "Dos novos pedidos recebidos, quais são realmente novos (não existem na tabela destino)?"
pedidos_existentes = spark.createDataFrame(
    [(101,), (103,)], ["pedido_id"]
)
pedidos_novos = spark.createDataFrame(
    [(101,), (104,), (107,)], ["pedido_id"]
)

# Semi join: quais novos pedidos JÁ existem? (para ignorar)
# Anti join: quais novos pedidos são realmente novos? (para inserir)
ja_existem = pedidos_novos.join(pedidos_existentes, "pedido_id", "left_semi")
print("Pedidos novos que já existem:", ja_existem.collect())

# COMMAND ----------

# MAGIC %md
# ## 6. LEFT ANTI JOIN — Filtro de ausência (órfãos)

# COMMAND ----------

# MAGIC %md
# ```
# Semântica: "quais pedidos NÃO têm cliente cadastrado?"
# Inverso perfeito do semi join.
#
# Equivalente SQL: WHERE NOT EXISTS (...) ou WHERE id NOT IN (...)
# MAS: NOT IN tem comportamento perigoso com NULLs → prefira LEFT ANTI
# ```

# COMMAND ----------

anti = pedidos.join(clientes, on="cliente_id", how="left_anti")
# equivalente: how="anti"
print(f"Left anti: {anti.count()} linhas (pedidos SEM cliente cadastrado)")
anti.show()

# COMMAND ----------

# MAGIC %md
# ### Anti join vs NOT IN — o perigo dos NULLs

# COMMAND ----------

# NOT IN com null na lista de valores → resultado vazio (comportamento surpresa!)
# Isso acontece porque NULL IN (1, 2, NULL) = NULL (não TRUE) → WHERE NULL = FALSE
df_teste = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
lista_com_null = spark.createDataFrame([(1,), (None,)], ["id"])

# LEFT ANTI — correto e seguro com nulls
anti_resultado = df_teste.join(lista_com_null, "id", "left_anti")
print(f"Left anti (seguro): {anti_resultado.count()} linhas")  # 2: ids 2 e 3

# Equivalente SQL NOT IN com null → retorna 0 linhas (bug silencioso)
df_teste.createOrReplaceTempView("teste")
lista_com_null.createOrReplaceTempView("lista_null")
bug_not_in = spark.sql("""
    SELECT * FROM teste
    WHERE id NOT IN (SELECT id FROM lista_null)
""")
print(f"NOT IN com null na lista: {bug_not_in.count()} linhas")  # 0 — bug!
print("→ LEFT ANTI é sempre mais seguro que NOT IN quando pode haver NULLs")

# COMMAND ----------

# MAGIC %md
# ### Padrões de uso do Anti Join

# COMMAND ----------

# Padrão 1: encontrar registros ausentes (auditoria, reconciliação)
# "Quais clientes nunca fizeram um pedido?"
clientes_sem_pedido = clientes.join(pedidos, "cliente_id", "left_anti")
print("Clientes sem nenhum pedido:")
clientes_sem_pedido.show()

# COMMAND ----------

# Padrão 2: implementar lógica de upsert incremental
# "Dos registros novos, quais ainda não existem na tabela destino?"
tabela_destino = spark.createDataFrame(
    [(101, 1, 1500.0), (102, 1, 2000.0), (103, 2, 800.0)],
    ["pedido_id", "cliente_id", "valor"]
)
novos_registros = spark.createDataFrame(
    [(103, 2, 850.0), (107, 3, 1200.0), (108, 4, 600.0)],  # 103 já existe, 107 e 108 são novos
    ["pedido_id", "cliente_id", "valor"]
)

# Anti join: quais novos registros não existem no destino → podem ser inseridos
para_inserir = novos_registros.join(
    tabela_destino.select("pedido_id"),
    "pedido_id",
    "left_anti"
)
print("Registros para INSERT (não existem no destino):")
para_inserir.show()

# COMMAND ----------

# MAGIC %md
# ## 7. CROSS JOIN — Produto Cartesiano

# COMMAND ----------

# MAGIC %md
# ```
# Combina CADA linha do esquerdo com CADA linha do direito.
# N linhas × M linhas = N × M linhas resultado.
#
# Uso legítimo: gerar combinações (cenários, calendários, matrizes de preço)
# Perigo: acidental → join sem condição → cluster pode travar com dados grandes
# ```

# COMMAND ----------

# Cross join explícito e controlado
regioes = spark.createDataFrame([("SP",), ("RJ",), ("MG",)], ["regiao"])
categorias = spark.createDataFrame([("Eletronicos",), ("Moveis",)], ["categoria"])

# Gera todas as combinações regiao × categoria (útil para tabelas de referência)
todas_combinacoes = regioes.crossJoin(categorias)
print(f"Cross join: {todas_combinacoes.count()} linhas ({regioes.count()} × {categorias.count()})")
todas_combinacoes.show()

# COMMAND ----------

# Uso prático: garantir que o relatório final tenha TODAS as combinações (mesmo sem dados)
# Left join de todas_combinacoes com os dados reais → preencherá nulos onde não há dados

resultado_completo = (
    todas_combinacoes
    .join(
        pedidos.join(clientes, "cliente_id")
               .groupBy("estado", "categoria").agg(spark_sum("valor").alias("receita")),
        on=(col("regiao") == col("estado")) & (todas_combinacoes["categoria"] == col("categoria")),
        how="left"
    )
    .select("regiao", todas_combinacoes["categoria"],
            coalesce(col("receita"), lit(0.0)).alias("receita"))
    .orderBy("regiao", "categoria")
)

print("Matriz completa regiao × categoria (zeros onde não houve venda):")
resultado_completo.show()

# COMMAND ----------

# MAGIC %md
# ### Protegendo contra cross join acidental

# COMMAND ----------

# Spark 3.x lança erro para cross join implícito se a config abaixo estiver ativa
print("Cross join implícito protegido:",
      spark.conf.get("spark.sql.crossJoin.enabled", "false"))

# Se false (default): qualquer join sem condição lança AnalysisException
# Se true: permite cross join implícito sem aviso — CUIDADO em produção

# Para habilitar cross join intencional em SQL:
# spark.conf.set("spark.sql.crossJoin.enabled", "true")
# SELECT * FROM a CROSS JOIN b

# COMMAND ----------

# MAGIC %md
# ## 8. Broadcast Join — otimização para small-large

# COMMAND ----------

# MAGIC %md
# O `broadcast()` foi coberto em profundidade em `04_physical_plan_joins.py`.
# Aqui os padrões de uso com cada tipo de join.

# COMMAND ----------

# broadcast() funciona com qualquer tipo de join lógico
pedidos_grande = spark.range(5_000_000).withColumn(
    "cliente_id", (col("id") % 1000).cast(LongType())
)

# Inner com broadcast
inner_bc = pedidos_grande.join(broadcast(clientes), "cliente_id", "inner")

# Left com broadcast
left_bc = pedidos_grande.join(broadcast(clientes), "cliente_id", "left")

# Semi com broadcast — o mais eficiente para filtros de existência em grande escala
semi_bc = pedidos_grande.join(broadcast(clientes), "cliente_id", "left_semi")

# Anti com broadcast — filtro de exclusão em grande escala
clientes_bloqueados = clientes.filter(col("tier") == "Bronze")
anti_bc = pedidos_grande.join(broadcast(clientes_bloqueados), "cliente_id", "left_anti")

for nome, df_join in [
    ("Inner + broadcast", inner_bc),
    ("Semi + broadcast",  semi_bc),
    ("Anti + broadcast",  anti_bc),
]:
    print(f"\n=== {nome} ===")
    df_join.explain(mode="simple")

# COMMAND ----------

# MAGIC %md
# ## 9. Join com múltiplas condições e non-equi join

# COMMAND ----------

# Join por múltiplas colunas (equi-join composto)
tabela_a = spark.createDataFrame(
    [(1, "2024-01", 100.0), (1, "2024-02", 200.0), (2, "2024-01", 300.0)],
    ["id", "mes", "valor"]
)
tabela_b = spark.createDataFrame(
    [(1, "2024-01", "A"), (1, "2024-02", "B"), (3, "2024-01", "C")],
    ["id", "mes", "flag"]
)

# Join por múltiplas colunas: lista de strings
join_multiplo = tabela_a.join(tabela_b, on=["id", "mes"], how="inner")
join_multiplo.show()

# COMMAND ----------

# Non-equi join: condição de range ou desigualdade
# ⚠️ Non-equi join usa Broadcast Nested Loop (O(N×M)) — evite com tabelas grandes

transacoes = spark.createDataFrame(
    [(1, 500.0), (2, 1500.0), (3, 3000.0), (4, 7000.0)],
    ["id", "valor"]
)
faixas = spark.createDataFrame(
    [(0.0, 1000.0, "Bronze"), (1000.0, 5000.0, "Silver"), (5000.0, 999999.0, "Gold")],
    ["min_valor", "max_valor", "categoria"]
)

join_range = transacoes.join(
    broadcast(faixas),  # SEMPRE faça broadcast no non-equi join
    on=(col("valor") >= col("min_valor")) & (col("valor") < col("max_valor")),
    how="inner"
)

print("Categorização por faixa de valor (non-equi join):")
join_range.select("id", "valor", "categoria").show()

# COMMAND ----------

# MAGIC %md
# ## 10. Self Join — tabela joinada com ela mesma

# COMMAND ----------

# Padrão clássico: encontrar pares ou hierarquias dentro da mesma tabela
funcionarios = spark.createDataFrame([
    (1, "Ana",   None,  "SP"),
    (2, "Bruno", 1,     "RJ"),   # Ana é manager de Bruno
    (3, "Carla", 1,     "MG"),   # Ana é manager de Carla
    (4, "Diana", 2,     "SP"),   # Bruno é manager de Diana
], ["id", "nome", "manager_id", "estado"])

# Self join para trazer o nome do manager
func_alias = funcionarios.alias("func")
mgr_alias  = funcionarios.alias("mgr")

hierarquia = (
    func_alias
    .join(
        mgr_alias,
        on=col("func.manager_id") == col("mgr.id"),
        how="left"   # left para incluir Ana (sem manager)
    )
    .select(
        col("func.nome").alias("funcionario"),
        col("mgr.nome").alias("manager"),
        col("func.estado"),
    )
)

hierarquia.show()

# COMMAND ----------

# MAGIC %md
# ## 11. Tabela comparativa: todos os joins

# COMMAND ----------

# MAGIC %md
# ```
# ┌────────────────┬─────────────────────────────────────────────┬────────────────────────────────┐
# │ Tipo           │ Resultado                                   │ Caso de uso típico             │
# ├────────────────┼─────────────────────────────────────────────┼────────────────────────────────┤
# │ inner          │ Apenas correspondências em ambos os lados   │ Enriquecer tabela fato         │
# │                │ Multiplica se chave não for única           │ com dimensão                   │
# ├────────────────┼─────────────────────────────────────────────┼────────────────────────────────┤
# │ left           │ Todo o esquerdo + correspondências do dir.  │ Preservar fato, trazer dims    │
# │ (left outer)   │ Nulos onde não há match no direito          │ opcionais                      │
# ├────────────────┼─────────────────────────────────────────────┼────────────────────────────────┤
# │ right          │ Todo o direito + correspondências do esq.   │ Geralmente reescrito como left │
# │ (right outer)  │ Nulos onde não há match no esquerdo         │ com DataFrames invertidos      │
# ├────────────────┼─────────────────────────────────────────────┼────────────────────────────────┤
# │ full / outer   │ Tudo de ambos os lados                      │ Reconciliação, auditoria entre │
# │ (full outer)   │ Nulos onde não há match em nenhum           │ duas fontes                    │
# ├────────────────┼─────────────────────────────────────────────┼────────────────────────────────┤
# │ left_semi      │ Linhas do esquerdo que TÊM match à direita  │ Filtrar por lista de válidos   │
# │                │ Apenas colunas do esquerdo. Sem duplicação  │ Deduplicação por chave         │
# ├────────────────┼─────────────────────────────────────────────┼────────────────────────────────┤
# │ left_anti      │ Linhas do esquerdo que NÃO têm match        │ Encontrar órfãos, inconsist.   │
# │                │ Apenas colunas do esquerdo                  │ Incremental: registros novos   │
# ├────────────────┼─────────────────────────────────────────────┼────────────────────────────────┤
# │ cross          │ Produto cartesiano: N × M linhas            │ Gerar combinações, cenários    │
# │                │ Sem condição de join                        │ NUNCA use acidentalmente       │
# └────────────────┴─────────────────────────────────────────────┴────────────────────────────────┘
# ```

# COMMAND ----------

# MAGIC %md
# ## ⚠️ Resumo de armadilhas e pontos de prova
#
# | Tema | O que saber |
# |---|---|
# | Inner duplica linhas | Se a chave não for única em ambos os lados → produto cartesiano parcial silencioso |
# | Semi não duplica | Mesmo com chave duplicada no direito → 1 linha por linha do esquerdo |
# | Anti vs NOT IN | NOT IN com null na lista → retorna 0 linhas (bug). Left anti é sempre seguro |
# | Semi/Anti só retorna colunas do esquerdo | Não é possível selecionar colunas do lado direito |
# | Cross join implícito | Com `spark.sql.crossJoin.enabled=false` (default) → AnalysisException |
# | Non-equi join | Usa Broadcast Nested Loop O(N×M) — sempre faça `broadcast()` do lado menor |
# | Self join requer alias | Sem alias → ambiguidade de coluna → AnalysisException |
# | Left preferível a right | Mais fácil de raciocinar. Right pode ser reescrito como left invertido |
# | Broadcast + semi/anti | Combinação poderosa para filtros em escala — sem shuffle do lado esquerdo |
# | Full outer para reconciliação | Padrão clássico para comparar duas fontes de dados |

# COMMAND ----------
