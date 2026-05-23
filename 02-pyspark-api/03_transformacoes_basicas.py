# Databricks notebook source

# MAGIC %md
# # 03 — Transformações Básicas do DataFrame
#
# > **Arquivo:** `02-pyspark-api/03_transformacoes_basicas.py`
# > **Módulo:** 02 — PySpark API
# > **Dependência:** `02_schema_types.py`
#
# ---
#
# ## Analogia
#
# Um DataFrame é uma tabela imutável. Cada transformação não modifica
# a tabela original — ela cria um novo plano de como a próxima tabela
# será construída. É como trabalhar com Lego: você não destrói as peças
# que já tem, você monta uma nova estrutura usando as peças existentes
# como base.
#
# Todas as transformações neste arquivo são **narrow** — não causam shuffle,
# não cruzam fronteiras de partição, não têm custo de rede.
# São as mais baratas que o Spark oferece.
#
# ---
#
# ## Setup — DataFrame de exemplo

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DecimalType,
    TimestampType, BooleanType, DateType, IntegerType, DoubleType
)
from pyspark.sql.functions import (
    col, lit, when, otherwise, coalesce,
    isnull, isnotnull, upper, lower, trim,
    cast, year, current_timestamp, to_date
)
from pyspark.sql import Row
from decimal import Decimal

# DataFrame de exemplo — usado ao longo do arquivo
dados = [
    Row(id=1,  nome="  Ana Silva  ",  uf="SP", salario=3200.0, depto="Engenharia",  ativo=True,  data_adm="2020-03-15"),
    Row(id=2,  nome="Bruno Costa",   uf="RJ", salario=4100.0, depto="Produto",      ativo=True,  data_adm="2019-07-22"),
    Row(id=3,  nome="carla MATOS",   uf="MG", salario=2800.0, depto="Engenharia",  ativo=False, data_adm="2021-11-01"),
    Row(id=4,  nome=None,            uf="SP", salario=5500.0, depto="Gestão",       ativo=True,  data_adm="2018-01-10"),
    Row(id=5,  nome="Eduardo Lima",  uf="RS", salario=None,   depto="Engenharia",  ativo=True,  data_adm=None),
    Row(id=6,  nome="Fernanda Rocha",uf="SP", salario=6200.0, depto="Produto",      ativo=None,  data_adm="2022-05-30"),
    Row(id=7,  nome="Gabriel Dias",  uf="RJ", salario=3800.0, depto="Dados",        ativo=True,  data_adm="2023-02-14"),
    Row(id=8,  nome="Helena Vaz",    uf="MG", salario=4500.0, depto="Dados",        ativo=False, data_adm="2020-08-19"),
]

schema = StructType([
    StructField("id",       LongType(),    False),
    StructField("nome",     StringType(),  True),
    StructField("uf",       StringType(),  True),
    StructField("salario",  DoubleType(),  True),
    StructField("depto",    StringType(),  True),
    StructField("ativo",    BooleanType(), True),
    StructField("data_adm", StringType(),  True),
])

df = spark.createDataFrame(dados, schema=schema)
df.cache()   # vai ser usado muitas vezes neste notebook
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ## 1. select — selecionar e reordenar colunas

# COMMAND ----------

# MAGIC %md
# ### Analogia
# `select` é como escolher quais gavetas de um arquivo você quer ver —
# você não mexe no arquivo, só decide o que trazer para a mesa.

# COMMAND ----------

# ── Formas de referenciar colunas ─────────────────────────────────────────

# Forma 1: string — simples e legível
df.select("id", "nome", "uf").show()

# Forma 2: col() — permite encadear operações
df.select(col("id"), col("nome"), col("uf")).show()

# Forma 3: df["coluna"] — útil para evitar ambiguidade em joins
df.select(df["id"], df["nome"]).show()

# Diferença prática entre string e col():
# → string: só referencia a coluna
# → col():  permite encadear: col("salario") * 1.1, upper(col("nome")), etc.

# COMMAND ----------

# ── select com transformações inline ─────────────────────────────────────

from pyspark.sql.functions import upper, round as spark_round, concat, lit

df.select(
    "id",
    upper(col("nome")).alias("nome_upper"),            # transformação inline
    col("salario").alias("sal"),                       # rename inline
    (col("salario") * 1.1).alias("sal_reajustado"),    # cálculo inline
    concat(col("nome"), lit(" - "), col("uf")).alias("nome_uf"),
).show(truncate=False)

# COMMAND ----------

# ── Selecionar todas as colunas menos algumas ─────────────────────────────

# Todas as colunas
df.select("*").show()

# Todas exceto algumas — pattern útil para remover colunas sensíveis
colunas_para_remover = ["data_adm", "ativo"]
df.select([c for c in df.columns if c not in colunas_para_remover]).show()

# Alternativa com drop (mais legível — ver seção 4)
df.drop("data_adm", "ativo").show()

# COMMAND ----------

# ── selectExpr — SQL expressions dentro do select ────────────────────────

df.selectExpr(
    "id",
    "upper(nome) as nome_upper",
    "salario * 1.1 as sal_reajustado",
    "CASE WHEN uf = 'SP' THEN 'Sudeste' ELSE 'Outro' END as regiao",
    "year(current_timestamp()) as ano_atual",
).show(truncate=False)

# selectExpr é útil para:
# → Expressões SQL complexas que seriam verbosas com a API
# → Migrar queries SQL para PySpark gradualmente
# → Expressões de case/when inline sem importar funções

# COMMAND ----------

# MAGIC %md
# ## 2. filter / where — filtrar linhas

# COMMAND ----------

# MAGIC %md
# ### Analogia
# `filter` é como um coador de café — o líquido (linhas que passam no teste)
# atravessa, o pó (linhas que não passam) fica para trás.
# O DataFrame original não é modificado.

# COMMAND ----------

# ── filter e where são 100% equivalentes ─────────────────────────────────
# Use qual preferir — a maioria das empresas escolhe um padrão

df.filter(col("uf") == "SP").show()
df.where(col("uf") == "SP").show()  # mesmo resultado

# COMMAND ----------

# ── Operadores de comparação ──────────────────────────────────────────────

# Igualdade e desigualdade
df.filter(col("uf") == "SP").show()
df.filter(col("uf") != "SP").show()
df.filter(col("salario") > 4000).show()
df.filter(col("salario") >= 4000).show()
df.filter(col("salario") < 4000).show()
df.filter(col("salario") <= 4000).show()

# COMMAND ----------

# ── Operadores lógicos — & | ~ ────────────────────────────────────────────
#
# ⚠️ ARMADILHA CLÁSSICA: use & e | com parênteses, NÃO and/or Python
# "and" e "or" Python não funcionam com Column objects

# CORRETO
df.filter((col("uf") == "SP") & (col("salario") > 4000)).show()
df.filter((col("uf") == "SP") | (col("uf") == "RJ")).show()
df.filter(~col("ativo")).show()   # NOT ativo

# ERRADO — vai lançar ValueError ou dar resultado inesperado
# df.filter(col("uf") == "SP" and col("salario") > 4000)  # ← NUNCA

# COMMAND ----------

# ── Múltiplos filtros encadeados ─────────────────────────────────────────
# Cada .filter() adiciona uma condição — equivale a AND entre eles
# O Catalyst funde todos em um único predicado no plano físico

df_filtrado = (
    df
    .filter(col("ativo") == True)
    .filter(col("salario") > 3000)
    .filter(col("uf").isin("SP", "RJ"))
    .filter(col("nome").isNotNull())
)
df_filtrado.show()

# COMMAND ----------

# ── isin — filtrar por lista de valores ──────────────────────────────────

ufs_sudeste = ["SP", "RJ", "MG", "ES"]

df.filter(col("uf").isin(ufs_sudeste)).show()
df.filter(~col("uf").isin(ufs_sudeste)).show()   # NOT IN
df.filter(col("uf").isin("SP", "RJ")).show()     # valores diretos

# COMMAND ----------

# ── like e rlike — filtros por padrão de texto ───────────────────────────

from pyspark.sql.functions import col

# like: padrão SQL (% = qualquer sequência, _ = um caractere)
df.filter(col("nome").like("Ana%")).show()       # começa com "Ana"
df.filter(col("nome").like("%Silva")).show()     # termina com "Silva"
df.filter(col("nome").like("%os%")).show()       # contém "os"

# rlike: regex completo
df.filter(col("nome").rlike("^[AB]")).show()     # começa com A ou B
df.filter(col("nome").rlike("\\d")).show()       # contém dígito

# COMMAND ----------

# ── Filtros com nulls ─────────────────────────────────────────────────────

# ⚠️ ARMADILHA: col("campo") == None NÃO funciona para null em Spark
# Spark usa lógica tri-valued: True, False, null
# null == null → null (não True!)

df.filter(col("nome").isNull()).show()           # ✅ correto
df.filter(col("nome").isNotNull()).show()        # ✅ correto
df.filter(isnull(col("nome"))).show()            # ✅ equivalente
df.filter(isnotnull(col("nome"))).show()         # ✅ equivalente

# df.filter(col("nome") == None).show()          # ❌ não funciona como esperado

# COMMAND ----------

# ── filter com SQL string ─────────────────────────────────────────────────

df.filter("uf = 'SP' AND salario > 4000").show()
df.filter("nome IS NOT NULL").show()
df.filter("ativo = true AND depto IN ('Engenharia', 'Dados')").show()

# COMMAND ----------

# MAGIC %md
# ## 3. withColumn — adicionar e modificar colunas

# COMMAND ----------

# MAGIC %md
# ### Analogia
# `withColumn` é como adicionar uma nova coluna numa planilha.
# Se o nome já existe, a coluna é substituída.
# Se o nome é novo, a coluna é adicionada ao final.
# Em ambos os casos, o DataFrame original não é tocado.

# COMMAND ----------

# ── Adicionar nova coluna ─────────────────────────────────────────────────

df2 = df.withColumn("salario_anual", col("salario") * 12)
df2.select("id", "nome", "salario", "salario_anual").show()

# COMMAND ----------

# ── Substituir coluna existente (mesmo nome) ──────────────────────────────

# Normalizar nome: remover espaços e colocar em título
from pyspark.sql.functions import initcap, trim

df3 = df.withColumn("nome", initcap(trim(col("nome"))))
df3.select("nome").show()
# "  Ana Silva  " → "Ana Silva"
# "carla MATOS"  → "Carla Matos"

# COMMAND ----------

# ── withColumn com operações matemáticas ─────────────────────────────────

from pyspark.sql.functions import round as spark_round, abs as spark_abs

df4 = (df
    .withColumn("salario_reajuste_10pct",  col("salario") * 1.10)
    .withColumn("salario_reajuste_arred",  spark_round(col("salario") * 1.10, 2))
    .withColumn("diferenca_media",         col("salario") - 4000)
    .withColumn("diferenca_abs",           spark_abs(col("salario") - 4000))
)
df4.select("nome", "salario", "salario_reajuste_arred", "diferenca_abs").show()

# COMMAND ----------

# ── withColumn com múltiplas colunas — padrão encadeado ──────────────────

# ✅ Encadeamento de withColumn — legível e idiomático
df5 = (df
    .withColumn("nome_clean",    initcap(trim(col("nome"))))
    .withColumn("sal_anual",     col("salario") * 12)
    .withColumn("senioridade",
        when(col("salario") < 3000, "Junior")
        .when(col("salario") < 5000, "Pleno")
        .otherwise("Senior")
    )
    .withColumn("regiao",
        when(col("uf").isin("SP", "RJ", "MG", "ES"), "Sudeste")
        .when(col("uf").isin("RS", "SC", "PR"), "Sul")
        .otherwise("Outro")
    )
    .withColumn("data_adm_dt", to_date(col("data_adm"), "yyyy-MM-dd"))
    .withColumn("ano_adm",     year(col("data_adm_dt")))
)
df5.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ### ⚠️ withColumn em loop — armadilha de performance

# COMMAND ----------

# ❌ Padrão ruim — withColumn em loop cria plano lógico excessivamente profundo
# Para muitas colunas (~50+), pode causar stack overflow no Catalyst

colunas_novas = {"col_a": lit(1), "col_b": lit(2), "col_c": lit(3)}

df_ruim = df
for nome_col, expressao in colunas_novas.items():
    df_ruim = df_ruim.withColumn(nome_col, expressao)
# Aceitável para poucos campos, problemático para 50+ colunas

# ✅ Padrão correto para muitas colunas — usar select
from pyspark.sql.functions import lit

expressoes = [col(c) for c in df.columns] + [
    lit(1).alias("col_a"),
    lit(2).alias("col_b"),
    lit(3).alias("col_c"),
    (col("salario") * 12).alias("sal_anual"),
]
df_bom = df.select(*expressoes)
df_bom.show(5)

# COMMAND ----------

# MAGIC %md
# ## 4. drop — remover colunas

# COMMAND ----------

# ── drop: remover uma ou mais colunas ────────────────────────────────────

# Remover coluna única
df.drop("data_adm").show()

# Remover múltiplas colunas
df.drop("data_adm", "ativo", "depto").show()

# Remover por lista
cols_sensiveis = ["nome", "data_adm"]
df.drop(*cols_sensiveis).show()

# ── drop de coluna inexistente — não gera erro ───────────────────────────
df.drop("coluna_que_nao_existe").show()   # silenciosamente ignorado — cuidado

# COMMAND ----------

# MAGIC %md
# ## 5. alias — renomear colunas

# COMMAND ----------

# ── alias: renomear coluna no select ─────────────────────────────────────

df.select(
    col("id").alias("identificador"),
    col("nome").alias("nome_completo"),
    col("salario").alias("remuneracao"),
    (col("salario") * 12).alias("remuneracao_anual"),
).show()

# COMMAND ----------

# ── withColumnRenamed — renomear sem select ───────────────────────────────

df_renamed = df.withColumnRenamed("nome", "nome_completo")
df_renamed.columns

# ── Renomear múltiplas colunas com withColumnsRenamed (Spark 3.4+) ────────

mapeamento = {
    "nome":    "nome_completo",
    "salario": "remuneracao",
    "uf":      "estado",
}
df_renamed_multi = df.withColumnsRenamed(mapeamento)
df_renamed_multi.columns

# ── toDF — renomear todas as colunas de uma vez ───────────────────────────

# Útil após join quando as colunas têm nomes ambíguos
novos_nomes = ["identificador", "nome_completo", "estado",
               "remuneracao", "departamento", "ativo", "data_admissao"]
df_todo_renamed = df.toDF(*novos_nomes)
df_todo_renamed.show(2)

# COMMAND ----------

# MAGIC %md
# ## 6. cast — conversão de tipos

# COMMAND ----------

# MAGIC %md
# ### Analogia
# `cast` é como um cambista: você chega com reais (string "3200.0")
# e ele te devolve em dólares (DoubleType 3200.0). Se a nota for
# inválida (string "abc"), ele te devolve null — sem erro, sem aviso.

# COMMAND ----------

from pyspark.sql.types import DoubleType, LongType, IntegerType, DateType
from pyspark.sql.functions import col

# ── cast básico ───────────────────────────────────────────────────────────

df_strings = spark.createDataFrame([
    ("1",  "3200.50", "2024-01-15"),
    ("2",  "4100.00", "2024-02-20"),
    ("3",  "abc",     "data-invalida"),   # valores inválidos
], ["id_str", "sal_str", "data_str"])

df_convertido = (df_strings
    .withColumn("id",      col("id_str").cast(LongType()))
    .withColumn("salario", col("sal_str").cast(DoubleType()))
    .withColumn("data",    col("data_str").cast(DateType()))
)
df_convertido.show()
# "abc"          → null  (cast falhou silenciosamente)
# "data-invalida"→ null  (cast falhou silenciosamente)

# COMMAND ----------

# ── Formas alternativas de cast ───────────────────────────────────────────

# Usando string de tipo
df_strings.withColumn("id", col("id_str").cast("long")).show()
df_strings.withColumn("id", col("id_str").cast("bigint")).show()  # bigint = long
df_strings.withColumn("sal", col("sal_str").cast("decimal(18,2)")).show()

# Usando tipo importado
from pyspark.sql.types import DecimalType
df_strings.withColumn("sal", col("sal_str").cast(DecimalType(18, 2))).show()

# COMMAND ----------

# ── ⚠️ cast falha silenciosamente → null ─────────────────────────────────
# Não existe exceção em cast inválido. Null é gerado.
# Isso é perigoso — valide nulos após cast em produção!

from pyspark.sql.functions import count, when, isnull

df_pos_cast = df_strings.withColumn("sal", col("sal_str").cast(DoubleType()))

# Checar quantos nulos surgiram após cast
df_pos_cast.select(
    count(when(isnull("sal"), "sal")).alias("nulos_apos_cast"),
    count("sal").alias("valores_validos"),
).show()

# COMMAND ----------

# MAGIC %md
# ## 7. when / otherwise — lógica condicional

# COMMAND ----------

# MAGIC %md
# ### Analogia
# `when/otherwise` é o CASE WHEN do SQL, que você já conhece.
# No PySpark, é uma função encadeada — cada `.when()` adiciona
# uma condição, e `.otherwise()` é o ELSE.

# COMMAND ----------

from pyspark.sql.functions import when, col

# ── Estrutura básica ──────────────────────────────────────────────────────
#
# when(condicao1, valor1)
# .when(condicao2, valor2)
# .when(condicao3, valor3)
# .otherwise(valor_default)
#
# → Avalia condições em ordem, retorna o valor da PRIMEIRA que for True
# → Se nenhuma condição for True e não há .otherwise(): retorna null
# → .otherwise() é o ELSE — boa prática sempre incluir

# COMMAND ----------

# ── Exemplo 1: classificação de salário ──────────────────────────────────

df_faixa = df.withColumn(
    "faixa_salarial",
    when(col("salario") < 3000,  "Júnior")
    .when(col("salario") < 5000, "Pleno")
    .when(col("salario") < 8000, "Sênior")
    .otherwise("Especialista")
)
df_faixa.select("nome", "salario", "faixa_salarial").show()

# COMMAND ----------

# ── Exemplo 2: múltiplas condições por when ───────────────────────────────

df_bonus = df.withColumn(
    "bonus_percentual",
    when(
        (col("depto") == "Engenharia") & (col("salario") > 4000), 15
    )
    .when(
        (col("depto") == "Dados") & (col("ativo") == True), 12
    )
    .when(col("ativo") == False, 0)
    .otherwise(10)
)
df_bonus.select("nome", "depto", "salario", "ativo", "bonus_percentual").show()

# COMMAND ----------

# ── Exemplo 3: when para tratar nulls ────────────────────────────────────

df_null_handle = df.withColumn(
    "nome_display",
    when(col("nome").isNull(), "Nome não informado")
    .when(col("nome") == "",   "Nome em branco")
    .otherwise(initcap(trim(col("nome"))))
)
df_null_handle.select("nome", "nome_display").show(truncate=False)

# COMMAND ----------

# ── Exemplo 4: when gerando booleano (flag) ───────────────────────────────

df_flags = df.withColumn(
    "is_senior",
    when(col("salario") >= 5000, True).otherwise(False)
).withColumn(
    "is_sp_ativo",
    when(
        (col("uf") == "SP") & (col("ativo") == True), True
    ).otherwise(False)
)
df_flags.select("nome", "salario", "ativo", "uf", "is_senior", "is_sp_ativo").show()

# COMMAND ----------

# ── Exemplo 5: when aninhado (com cuidado) ────────────────────────────────

# Evite aninhar when dentro de when — dificulta leitura
# Prefira múltiplos .when() encadeados ou uma UDF se ficar complexo

# ✅ Preferível: múltiplos .when() encadeados
df.withColumn("categoria",
    when((col("uf") == "SP") & (col("salario") > 5000), "SP-Senior")
    .when((col("uf") == "SP") & (col("salario") <= 5000), "SP-Junior")
    .when(col("uf") == "RJ", "RJ-Qualquer")
    .otherwise("Outro")
).show()

# COMMAND ----------

# ── when sem otherwise — resultado null quando nenhuma condição bate ──────

df_sem_otherwise = df.withColumn(
    "bonus_especial",
    when(col("depto") == "Dados", 5000)
    .when(col("depto") == "Engenharia", 3000)
    # sem .otherwise() → linhas de outros deptos recebem null
)
df_sem_otherwise.select("nome", "depto", "bonus_especial").show()
# Produto → null
# Gestão  → null

# COMMAND ----------

# MAGIC %md
# ## 8. coalesce — primeiro valor não-nulo

# COMMAND ----------

# MAGIC %md
# ### Analogia
# `coalesce` é como uma lista de substitutos numa equipe de plantão.
# Você tenta ligar para o primeiro — se não atender (null), tenta o segundo,
# depois o terceiro, e assim por diante até alguém atender.

# COMMAND ----------

from pyspark.sql.functions import coalesce, lit

# ── Uso básico: fallback para null ────────────────────────────────────────

df_coalesce = df.withColumn(
    "nome_safe",
    coalesce(col("nome"), lit("Nome Desconhecido"))
)
df_coalesce.select("nome", "nome_safe").show()

# COMMAND ----------

# ── Múltiplos fallbacks ───────────────────────────────────────────────────

dados_fallback = [
    (1, None, None, "fallback_c"),
    (2, None, "b_valor", None),
    (3, "a_valor", "b_valor", "fallback_c"),
    (4, None, None, None),
]
df_fb = spark.createDataFrame(dados_fallback, ["id", "col_a", "col_b", "col_c"])

df_fb.withColumn(
    "primeiro_nao_nulo",
    coalesce(col("col_a"), col("col_b"), col("col_c"), lit("TODOS NULOS"))
).show(truncate=False)

# id=1 → "fallback_c"  (col_a e col_b são null)
# id=2 → "b_valor"     (col_a é null, col_b não é)
# id=3 → "a_valor"     (col_a não é null — retorna imediatamente)
# id=4 → "TODOS NULOS" (todos nulos, cai no lit())

# COMMAND ----------

# ── coalesce vs when para null ────────────────────────────────────────────

# Quando usar coalesce:
# → Fallback simples: "se null, use este outro valor"
# → Múltiplos fallbacks em ordem de prioridade

# Quando usar when:
# → Condições mais complexas que apenas "é null?"
# → Diferentes valores para diferentes condições

# Equivalentes para o caso simples:
df.withColumn("nome_safe", coalesce(col("nome"), lit("Desconhecido")))
df.withColumn("nome_safe", when(col("nome").isNull(), "Desconhecido").otherwise(col("nome")))
# coalesce é mais conciso aqui ↑

# COMMAND ----------

# MAGIC %md
# ## 9. Combinando tudo — pipeline de limpeza completo

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, coalesce, trim, initcap,
    upper, to_date, year, current_timestamp,
    regexp_replace
)

df_limpo = (
    df

    # ── Limpeza de strings ──────────────────────────────────────────────
    .withColumn("nome",
        coalesce(
            initcap(trim(col("nome"))),
            lit("Não Informado")
        )
    )
    .withColumn("uf",
        upper(trim(col("uf")))
    )

    # ── Conversão de tipos ───────────────────────────────────────────────
    .withColumn("data_adm",
        to_date(col("data_adm"), "yyyy-MM-dd")
    )
    .withColumn("ano_adm",
        year(col("data_adm"))
    )

    # ── Tratar nulos com regra de negócio ────────────────────────────────
    .withColumn("salario",
        coalesce(col("salario"), lit(0.0))
    )
    .withColumn("ativo",
        coalesce(col("ativo"), lit(False))
    )

    # ── Classificações e flags ────────────────────────────────────────────
    .withColumn("faixa_salarial",
        when(col("salario") == 0,    "Sem Salário")
        .when(col("salario") < 3000, "Júnior")
        .when(col("salario") < 5000, "Pleno")
        .when(col("salario") < 8000, "Sênior")
        .otherwise("Especialista")
    )
    .withColumn("regiao",
        when(col("uf").isin("SP", "RJ", "MG", "ES"), "Sudeste")
        .when(col("uf").isin("RS", "SC", "PR"),       "Sul")
        .when(col("uf").isin("BA", "PE", "CE"),       "Nordeste")
        .otherwise("Outro")
    )
    .withColumn("salario_anual",
        (col("salario") * 12).cast(DecimalType(18, 2))
    )

    # ── Metadados de controle ─────────────────────────────────────────────
    .withColumn("processado_em", current_timestamp())

    # ── Selecionar e reordenar colunas finais ────────────────────────────
    .select(
        "id", "nome", "uf", "regiao",
        "depto", "faixa_salarial",
        "salario", "salario_anual",
        "ativo", "data_adm", "ano_adm",
        "processado_em"
    )
)

df_limpo.printSchema()
df_limpo.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# ## 10. Referência rápida — todas as transformações

# COMMAND ----------

# ── Tabela de referência ──────────────────────────────────────────────────

referencia = [
    ("select(*cols)",          "Seleciona colunas — pode incluir transformações inline"),
    ("selectExpr(*exprs)",     "Select com expressões SQL como string"),
    ("filter(cond) / where",   "Filtra linhas — & para AND, | para OR, ~ para NOT"),
    ("withColumn(nome, expr)", "Adiciona ou substitui coluna"),
    ("withColumnRenamed(a,b)", "Renomeia coluna sem select"),
    ("drop(*cols)",            "Remove colunas — silencioso se não existir"),
    ("col('x').alias('y')",    "Renomeia coluna dentro de select"),
    ("col('x').cast(tipo)",    "Converte tipo — retorna null se inválido"),
    ("when(c,v).otherwise(v)", "Condicional — avalia em ordem, retorna 1º True"),
    ("coalesce(a, b, c)",      "Retorna 1º valor não-nulo da lista"),
    ("isin(*valores)",         "Equivalente ao IN do SQL"),
    ("isNull() / isNotNull()", "Testa nulos — NUNCA use == None"),
    ("like('%padrão%')",       "Padrão SQL — % qualquer seq, _ um char"),
    ("rlike('regex')",         "Padrão regex completo"),
]

print(f"\n{'Transformação':<35} {'Descrição'}")
print("─" * 90)
for func, desc in referencia:
    print(f"  {func:<33} {desc}")

# COMMAND ----------

# MAGIC %md
# ## Resumo — o que fixar deste arquivo
#
# | Conceito | O que saber |
# |----------|-------------|
# | `select` | Pode incluir transformações inline; `selectExpr` para SQL strings |
# | `filter` / `where` | Equivalentes; use `&` e `|` com parênteses — nunca `and`/`or` |
# | Filtrar null | `.isNull()` / `.isNotNull()` — nunca `== None` |
# | `withColumn` | Mesmo nome → substitui; nome novo → adiciona ao final |
# | Loop de `withColumn` | Evitar para 50+ colunas — use `select` com lista |
# | `drop` | Múltiplas colunas em uma chamada; silencioso para coluna inexistente |
# | `cast` | Falha silenciosa → null; validar nulos após cast em produção |
# | `when/otherwise` | Avalia em ordem; sem `.otherwise()` → null para não-matches |
# | `coalesce` | Primeiro não-nulo da lista; mais conciso que `when` para null fallback |
# | Encadeamento | `(df.filter().withColumn().select())` — o padrão idiomático |
#
# ### Conexão com a certificação Associate
# - **Domínio 2:** Transformações são o núcleo do ELT — 29% da prova
# - A prova testa: diferença entre `union` e `unionByName`, `when` sem `otherwise`,
#   `cast` retornando null silenciosamente, filtro com null usando `==` vs `isNull()`
#
# ### Próximo arquivo
# `04_aggregations.py` — groupBy, agg, pivot, cube, rollup —
# transformações **wide** que causam shuffle e separam stages.
