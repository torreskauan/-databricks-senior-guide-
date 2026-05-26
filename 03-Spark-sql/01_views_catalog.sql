-- Databricks notebook source

-- MAGIC %md
-- # 01 — Views e Catálogo — TempView, GlobalTempView e Namespacing
--
-- > **Arquivo:** `03-spark-sql/01_views_catalog.sql`
-- > **Módulo:** 03 — Spark SQL
-- > **Nível:** Fundação do SQL no Spark — entender antes de qualquer DDL
--
-- ---
--
-- ## Analogia
--
-- Pensa no catálogo como a estrutura de uma biblioteca universitária:
-- a biblioteca inteira é o **Metastore** (único por organização/região).
-- Cada **andar** é um **Catalog** (prod, dev, staging).
-- Cada **seção** de um andar é um **Schema** (vendas, financeiro, rh).
-- Cada **livro** é uma **Table** ou **View**.
--
-- Uma TempView é um post-it colado na sua mesa: só você vê,
-- só dura até você sair da sala (sessão).
-- Uma GlobalTempView é um quadro branco no corredor: todos
-- que estão no mesmo andar (cluster) veem, até o prédio fechar.
-- Uma View persistida no catálogo é um livro registrado na biblioteca:
-- qualquer pessoa com acesso pode ler, mesmo semanas depois.
--
-- ---
--
-- ## Estrutura de namespacing no Unity Catalog
--
-- ```
-- Metastore (1 por região/organização)
-- └── Catalog    (prod, dev, staging, sandbox)
--     └── Schema (vendas, financeiro, rh, dados_brutos)
--         └── Table / View / Volume / Function
-- ```
--
-- Referência completa: `catalog.schema.objeto`
-- Referência curta (com USE): `schema.objeto` ou `objeto`

-- COMMAND ----------

-- MAGIC %md
-- ## 1. Navegação no Catálogo

-- COMMAND ----------

-- ── Listar objetos disponíveis ───────────────────────────────────────────

-- Listar todos os catalogs disponíveis
SHOW CATALOGS;

-- Listar todos os schemas de um catalog
SHOW SCHEMAS IN prod;
SHOW DATABASES IN prod;   -- DATABASES é sinônimo de SCHEMAS no Spark SQL

-- Listar todas as tabelas de um schema
SHOW TABLES IN prod.vendas;

-- Listar tabelas com filtro por pattern
SHOW TABLES IN prod.vendas LIKE 'pedidos*';

-- Listar views
SHOW VIEWS IN prod.vendas;

-- Listar funções registradas
SHOW FUNCTIONS IN prod.vendas;

-- COMMAND ----------

-- ── USE — definir contexto padrão ─────────────────────────────────────────
-- Evita precisar qualificar com 3 partes em toda query

-- Sem USE — qualificação completa obrigatória
SELECT * FROM prod.vendas.pedidos WHERE status = 'PAGO';

-- COMMAND ----------

-- Definir catalog padrão
USE CATALOG prod;

-- Agora só precisa de schema.tabela
SELECT * FROM vendas.pedidos WHERE status = 'PAGO';

-- COMMAND ----------

-- Definir schema padrão dentro do catalog atual
USE SCHEMA vendas;
-- ou equivalente:
USE vendas;

-- Agora só precisa do nome da tabela
SELECT * FROM pedidos WHERE status = 'PAGO';

-- COMMAND ----------

-- Verificar contexto atual
SELECT current_catalog();   -- prod
SELECT current_schema();    -- vendas
SELECT current_user();      -- seu email/username

-- COMMAND ----------

-- ── DESCRIBE — inspecionar objetos ───────────────────────────────────────

-- Schema resumido: colunas e tipos
DESCRIBE TABLE prod.vendas.pedidos;

-- Schema detalhado: metadados, location, format, properties, statistics
DESCRIBE TABLE EXTENDED prod.vendas.pedidos;

-- Schema detalhado (formato formatado — mais legível)
DESCRIBE DETAIL prod.vendas.pedidos;
-- Retorna: format, location, numFiles, sizeInBytes, partitionColumns, etc.

-- Inspecionar schema específico
DESCRIBE SCHEMA prod.vendas;
DESCRIBE SCHEMA EXTENDED prod.vendas;

-- COMMAND ----------

-- MAGIC %md
-- ## 2. TempView — visão temporária de sessão

-- COMMAND ----------

-- MAGIC %md
-- ### Características da TempView
--
-- | Característica | Valor |
-- |---|---|
-- | Escopo | Sessão atual (SparkSession) |
-- | Visibilidade | Apenas o notebook/sessão que criou |
-- | Persistência | Destruída quando a sessão termina |
-- | Catálogo | Não registrada — existe só em memória |
-- | Namespace | Sem catalog/schema — só o nome |
-- | Conflito | Substitui se já existir (com OR REPLACE) |

-- COMMAND ----------

-- ── Criando TempViews ────────────────────────────────────────────────────

-- A partir de uma tabela existente
CREATE OR REPLACE TEMP VIEW vw_pedidos_pagos AS
SELECT
    id,
    id_cliente,
    valor,
    status,
    regiao,
    data_pedido,
    YEAR(data_pedido)  AS ano,
    MONTH(data_pedido) AS mes
FROM prod.vendas.pedidos
WHERE status = 'PAGO'
  AND valor > 0;

-- COMMAND ----------

-- A partir de uma transformação complexa (CTE + View)
CREATE OR REPLACE TEMP VIEW vw_resumo_cliente AS
WITH pedidos_base AS (
    SELECT
        id_cliente,
        COUNT(*)        AS qtd_pedidos,
        SUM(valor)      AS total_gasto,
        AVG(valor)      AS ticket_medio,
        MAX(data_pedido) AS ultimo_pedido
    FROM prod.vendas.pedidos
    WHERE status = 'PAGO'
    GROUP BY id_cliente
),
classificacao AS (
    SELECT
        id_cliente,
        qtd_pedidos,
        total_gasto,
        ticket_medio,
        ultimo_pedido,
        CASE
            WHEN total_gasto >= 10000 THEN 'Platinum'
            WHEN total_gasto >= 5000  THEN 'Gold'
            WHEN total_gasto >= 1000  THEN 'Silver'
            ELSE                           'Bronze'
        END AS segmento
    FROM pedidos_base
)
SELECT c.*, cl.nome, cl.email
FROM classificacao c
LEFT JOIN prod.vendas.clientes cl ON c.id_cliente = cl.id;

-- COMMAND ----------

-- Usar a TempView exatamente como uma tabela
SELECT segmento, COUNT(*) AS qtd, AVG(ticket_medio) AS ticket_avg
FROM vw_resumo_cliente
GROUP BY segmento
ORDER BY ticket_avg DESC;

-- COMMAND ----------

-- ── Diferença entre CREATE VIEW e CREATE OR REPLACE TEMP VIEW ─────────────

-- CREATE TEMP VIEW: falha se a view já existir
-- CREATE OR REPLACE TEMP VIEW: recria sem erro — idempotente
-- ✅ Em notebooks e pipelines: sempre usar OR REPLACE

-- COMMAND ----------

-- ── Listar e remover TempViews ────────────────────────────────────────────

-- Listar todas as temp views da sessão atual
SHOW VIEWS;                      -- inclui temp views
SHOW VIEWS LIKE 'vw_*';          -- com filtro de pattern

-- Verificar se existe antes de dropar
-- (não há IF EXISTS para DROP VIEW em todas as versões — use OR REPLACE ao criar)
DROP VIEW IF EXISTS vw_pedidos_pagos;
DROP VIEW IF EXISTS vw_resumo_cliente;

-- COMMAND ----------

-- MAGIC %md
-- ## 3. GlobalTempView — visão temporária de cluster

-- COMMAND ----------

-- MAGIC %md
-- ### Características da GlobalTempView
--
-- | Característica | Valor |
-- |---|---|
-- | Escopo | Cluster inteiro (todos os notebooks ativos) |
-- | Visibilidade | Qualquer notebook no mesmo cluster |
-- | Persistência | Destruída quando o cluster para |
-- | Namespace especial | `global_temp.nome_da_view` |
-- | Uso típico | Compartilhar dataset entre notebooks no mesmo job |

-- COMMAND ----------

-- ── Criando GlobalTempViews ──────────────────────────────────────────────

-- Sintaxe: GLOBAL TEMP VIEW
CREATE OR REPLACE GLOBAL TEMP VIEW gvw_parametros AS
SELECT
    'producao'          AS ambiente,
    '2024-01-01'        AS data_inicio_carga,
    CURRENT_DATE()      AS data_referencia,
    100                 AS batch_size;

-- COMMAND ----------

-- ⚠️ OBRIGATÓRIO: prefixar com global_temp ao acessar
SELECT * FROM global_temp.gvw_parametros;

-- Sem o prefixo → erro: table not found
-- SELECT * FROM gvw_parametros;   -- ← ERRO

-- COMMAND ----------

-- Exemplo de uso: compartilhar parâmetros entre notebooks de um job
-- Notebook 1 (orquestrador): cria a GlobalTempView com configurações
CREATE OR REPLACE GLOBAL TEMP VIEW gvw_config_job AS
SELECT
    CURRENT_DATE() - INTERVAL 1 DAY  AS data_processamento,
    'incremental'                      AS modo_carga,
    1000                               AS limite_rows_debug;

-- COMMAND ----------

-- Notebook 2 (worker): consome a configuração
SELECT data_processamento, modo_carga
FROM global_temp.gvw_config_job;

-- Em PySpark:
-- config = spark.table("global_temp.gvw_config_job").collect()[0]

-- COMMAND ----------

-- ── Listar e remover GlobalTempViews ──────────────────────────────────────

SHOW VIEWS IN global_temp;

DROP VIEW IF EXISTS global_temp.gvw_parametros;
DROP VIEW IF EXISTS global_temp.gvw_config_job;

-- COMMAND ----------

-- MAGIC %md
-- ## 4. Views Persistidas no Catálogo

-- COMMAND ----------

-- MAGIC %md
-- ### Características da View Persistida
--
-- | Característica | Valor |
-- |---|---|
-- | Escopo | Catálogo (persiste entre sessões e clusters) |
-- | Visibilidade | Qualquer usuário com permissão SELECT |
-- | Persistência | Permanente — até ser dropada explicitamente |
-- | Namespace | `catalog.schema.view` — cidadão de primeira classe |
-- | Armazenamento | Apenas a query SQL — não materializa dados |
-- | Uso típico | Camada semântica, mascaramento de dados, simplificar acesso |

-- COMMAND ----------

-- ── Criando View persistida no Unity Catalog ─────────────────────────────

CREATE OR REPLACE VIEW prod.vendas.vw_pedidos_ativos AS
SELECT
    p.id,
    p.id_cliente,
    c.nome       AS nome_cliente,
    c.segmento,
    p.valor,
    p.status,
    p.regiao,
    p.data_pedido,
    YEAR(p.data_pedido)  AS ano,
    MONTH(p.data_pedido) AS mes,
    CURRENT_TIMESTAMP()  AS consultado_em
FROM prod.vendas.pedidos    p
LEFT JOIN prod.vendas.clientes c ON p.id_cliente = c.id
WHERE p.status NOT IN ('CANCELADO', 'RASCUNHO')
  AND p.deleted_at IS NULL;

-- COMMAND ----------

-- ── View como camada de segurança (mascaramento de dados) ─────────────────

-- View que oculta dados sensíveis por grupo de usuário
CREATE OR REPLACE VIEW prod.vendas.vw_clientes_publico AS
SELECT
    id,
    -- Mascarar CPF: mostra só os 3 primeiros e últimos 2 dígitos
    CONCAT(
        SUBSTRING(cpf, 1, 3),
        '.***.***-',
        SUBSTRING(cpf, 10, 2)
    )                       AS cpf_mascarado,
    -- Mascarar email: mostra só domínio
    CONCAT('***@', SPLIT(email, '@')[1])  AS email_mascarado,
    nome,
    segmento,
    uf,
    -- Não expõe: data_nascimento, renda, score_credito
    criado_em
FROM prod.vendas.clientes;

-- COMMAND ----------

-- ── View com lógica de negócio centralizada ───────────────────────────────

CREATE OR REPLACE VIEW prod.gold.vw_kpis_vendas AS
WITH base AS (
    SELECT
        YEAR(data_pedido)          AS ano,
        MONTH(data_pedido)         AS mes,
        regiao,
        COUNT(*)                   AS qtd_pedidos,
        SUM(valor)                 AS receita_bruta,
        SUM(CASE WHEN status = 'PAGO'      THEN valor ELSE 0 END) AS receita_liquida,
        SUM(CASE WHEN status = 'CANCELADO' THEN 1 ELSE 0 END) AS cancelamentos,
        COUNT(DISTINCT id_cliente)  AS clientes_unicos
    FROM prod.vendas.pedidos
    WHERE data_pedido >= '2023-01-01'
    GROUP BY 1, 2, 3
),
com_metricas AS (
    SELECT
        *,
        receita_liquida / NULLIF(qtd_pedidos, 0)   AS ticket_medio,
        cancelamentos   / NULLIF(qtd_pedidos, 0)   AS taxa_cancelamento,
        receita_liquida / NULLIF(receita_bruta, 0) AS taxa_conversao,
        SUM(receita_liquida) OVER (
            PARTITION BY ano, regiao
            ORDER BY mes
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                           AS receita_acumulada_ano
    FROM base
)
SELECT * FROM com_metricas;

-- COMMAND ----------

-- ── Inspecionar uma View ──────────────────────────────────────────────────

-- Ver a query SQL que define a view
SHOW CREATE TABLE prod.vendas.vw_pedidos_ativos;

-- Schema das colunas
DESCRIBE TABLE prod.vendas.vw_pedidos_ativos;

-- Verificar se é view ou tabela
DESCRIBE TABLE EXTENDED prod.vendas.vw_pedidos_ativos;
-- Procure: Type = VIEW

-- COMMAND ----------

-- ── ALTER VIEW — renomear ou mudar a query ────────────────────────────────

-- Renomear
ALTER VIEW prod.vendas.vw_pedidos_ativos
RENAME TO prod.vendas.vw_pedidos_vigentes;

-- Redefinir a query (sem recriar)
ALTER VIEW prod.vendas.vw_pedidos_vigentes AS
SELECT * FROM prod.vendas.pedidos WHERE status = 'ATIVO';

-- COMMAND ----------

-- MAGIC %md
-- ## 5. Tabela de comparação — qual usar em cada situação

-- COMMAND ----------

-- MAGIC %md
-- | Critério | TempView | GlobalTempView | View Persistida |
-- |---|---|---|---|
-- | **Criação** | `CREATE OR REPLACE TEMP VIEW` | `CREATE OR REPLACE GLOBAL TEMP VIEW` | `CREATE OR REPLACE VIEW catalog.schema.nome` |
-- | **Acesso** | `nome_da_view` | `global_temp.nome_da_view` | `catalog.schema.nome` |
-- | **Dura até** | Sessão terminar | Cluster parar | Ser dropada explicitamente |
-- | **Visível para** | Só a sessão atual | Todos no mesmo cluster | Qualquer usuário com permissão |
-- | **Registrada no catálogo** | ❌ | ❌ | ✅ |
-- | **Quando usar** | Queries intermediárias, exploração | Compartilhar entre notebooks no mesmo job | Camada semântica, segurança, reutilização |

-- COMMAND ----------

-- MAGIC %md
-- ## 6. Padrões de namespacing em produção

-- COMMAND ----------

-- ── Convenção de nomes de views ───────────────────────────────────────────

-- Prefixo vw_ diferencia de tabelas ao listar o catálogo
-- Facilita identificar se você está consultando view ou tabela base

-- TempView: prefixo tmp_ ou vw_ + nome descritivo
-- CREATE OR REPLACE TEMP VIEW tmp_pedidos_enriquecidos AS ...
-- CREATE OR REPLACE TEMP VIEW vw_base_analise AS ...

-- GlobalTempView: prefixo gvw_ + contexto
-- CREATE OR REPLACE GLOBAL TEMP VIEW gvw_config_job AS ...

-- View persistida: prefixo vw_ + nome de negócio
-- CREATE OR REPLACE VIEW prod.vendas.vw_pedidos_pagos AS ...
-- CREATE OR REPLACE VIEW prod.gold.vw_kpis_mensais AS ...

-- COMMAND ----------

-- ── Estratégia de USE para simplificar notebooks ──────────────────────────

-- No início de cada notebook que trabalha em um schema específico:
USE CATALOG prod;
USE SCHEMA vendas;

-- A partir daqui, todas as queries usam prod.vendas como padrão
SELECT * FROM pedidos;              -- prod.vendas.pedidos
SELECT * FROM vw_pedidos_pagos;     -- prod.vendas.vw_pedidos_pagos
CREATE TEMP VIEW vw_tmp AS ...;     -- TempView local

-- Para referenciar outro schema explicitamente:
SELECT * FROM financeiro.notas_fiscais;   -- prod.financeiro.notas_fiscais

-- COMMAND ----------

-- MAGIC %md
-- ## 7. Armadilhas e observações

-- COMMAND ----------

-- ── Armadilha 1: TempView esconde tabela com mesmo nome ──────────────────

-- Se existir TempView "pedidos" e tabela "pedidos" no schema atual,
-- a TempView tem precedência — você pode estar lendo o dado errado

CREATE OR REPLACE TEMP VIEW pedidos AS
SELECT * FROM prod.vendas.pedidos WHERE ano = 2023;

-- Agora "SELECT * FROM pedidos" usa a TempView, não a tabela!
-- Para forçar a tabela, qualifique completamente:
SELECT * FROM prod.vendas.pedidos;   -- garante que é a tabela

-- Limpeza
DROP VIEW IF EXISTS pedidos;

-- COMMAND ----------

-- ── Armadilha 2: GlobalTempView exige prefixo global_temp ────────────────

CREATE OR REPLACE GLOBAL TEMP VIEW gvw_teste AS SELECT 1 AS val;

SELECT * FROM gvw_teste;              -- ❌ ERRO: table not found
SELECT * FROM global_temp.gvw_teste;  -- ✅ correto

-- COMMAND ----------

-- ── Armadilha 3: View persistida não persiste dados ──────────────────────

-- Uma View é apenas uma query salva, não uma tabela materializada
-- Se a tabela base for dropada, a view quebra

DROP TABLE prod.vendas.pedidos;
SELECT * FROM prod.vendas.vw_pedidos_ativos;
-- → AnalysisException: Table not found: prod.vendas.pedidos

-- Para materializar: use CREATE TABLE AS SELECT ou CTAS
-- ou use Delta Live Tables para atualização automática

-- COMMAND ----------

-- ── Armadilha 4: DROP VIEW vs DROP TABLE ──────────────────────────────────

-- Views: usar DROP VIEW
DROP VIEW IF EXISTS prod.vendas.vw_pedidos_ativos;

-- Tabelas: usar DROP TABLE
DROP TABLE IF EXISTS prod.vendas.pedidos;

-- DROP TABLE em uma view → erro em algumas versões
-- Sempre identifique o tipo antes de dropar

-- COMMAND ----------

-- MAGIC %md
-- ## Resumo — o que fixar deste arquivo
--
-- | Conceito | O que saber |
-- |---|---|
-- | TempView | `CREATE OR REPLACE TEMP VIEW` — dura a sessão, acesso sem prefixo |
-- | GlobalTempView | `GLOBAL TEMP VIEW` — dura o cluster, acesso via `global_temp.nome` |
-- | View persistida | `CREATE OR REPLACE VIEW catalog.schema.nome` — permanente, no catálogo |
-- | Hierarquia | Metastore → Catalog → Schema → Table/View |
-- | `USE CATALOG` / `USE SCHEMA` | Define contexto padrão — evita qualificação de 3 partes |
-- | Precedência | TempView tem precedência sobre tabela com mesmo nome |
-- | View não materializa | Só salva a query — tabela base deve existir |
-- | `SHOW TABLES` | Lista tabelas e views do schema atual |
-- | `DESCRIBE EXTENDED` | Revela se é TABLE ou VIEW, location, format, properties |
--
-- ### Próximo arquivo
-- `02_ddl_completo.sql` — CREATE TABLE com todas as opções:
-- USING DELTA, PARTITIONED BY, LOCATION, TBLPROPERTIES, GENERATED COLUMNS.
