-- ============================================================================
-- Módulo 03 – Spark SQL (Databricks)
-- Arquivo: 03_dml_patterns.sql
-- Tópico: INSERT, COPY INTO, CTAS (padrões DML)
-- ============================================================================
-- Objetivo: Dominar padrões de carga e transformação de dados com DML no Delta.
-- Público: Nível Sênior Global
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. INSERT: Padrões de Inserção em Delta Lake
-- --------------------------------------------------------------------------

-- 1.1. INSERT INTO – Append tradicional
-- Adiciona registros mantendo o histórico, ideal para ingestão incremental.
INSERT INTO silver.vendas
SELECT
    id_venda,
    id_cliente,
    id_produto,
    data_venda,
    valor
FROM bronze.vendas_json;

-- 1.2. INSERT OVERWRITE – Substituição completa de partição
-- Útil para cargas batch que recarregam uma fatia do dia.
-- A tabela deve estar particionada por data_venda.
INSERT OVERWRITE silver.vendas
PARTITION (data_venda = '2025-01-15')
SELECT
    id_venda,
    id_cliente,
    id_produto,
    valor
FROM staging.vendas_dia_20250115;

-- Nota: INSERT OVERWRITE com Delta substitui apenas a partição especificada,
-- sem afetar outras partições. É atômico e resolve o problema de reprocessamento.

-- 1.3. INSERT com múltiplas tabelas (multiplexação)
-- Direciona registros para tabelas diferentes baseados em condição.
-- Suportado em Databricks Runtime 12.2+ (condicional multi-tabela).
FROM bronze.eventos_brutos
INSERT INTO silver.eventos_tipo_a SELECT * WHERE event_type = 'A'
INSERT INTO silver.eventos_tipo_b SELECT * WHERE event_type = 'B';

-- --------------------------------------------------------------------------
-- 2. COPY INTO: Carga Eficiente de Arquivos Externos
-- --------------------------------------------------------------------------

-- 2.1. Carga de arquivos CSV da camada bronze para uma tabela Delta
-- COPY INTO é idempotente: ignora arquivos já processados (rastreia por nome).
COPY INTO bronze.transacoes_csv
FROM '/mnt/landing/transacoes/'
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'header' = 'true',
    'delimiter' = '|',
    'inferSchema' = 'true',
    'mergeSchema' = 'false'   -- evita alteração de schema durante carga
)
COPY_OPTIONS (
    'force' = 'false',        -- não reprocessa arquivos já ingeridos
    'mergeSchema' = 'false'
)
PATTERN = '*.csv';

-- 2.2. COPY INTO com validação e transformação
-- Se precisar de transformação antes de inserir, use CTAS após COPY INTO,
-- ou faça COPY INTO com SELECT (Databricks 11.3+).
COPY INTO bronze.transacoes_validas (
    id_transacao,
    valor,
    data
)
FROM (
    SELECT
        _c0 AS id_transacao,
        CAST(_c1 AS DECIMAL(10,2)) AS valor,
        TO_DATE(_c2, 'yyyy-MM-dd') AS data
    FROM '/mnt/landing/transacoes/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'false', 'delimiter' = ',');

-- 2.3. Carga de JSON sem esquema fixo (schema auto-merge)
COPY INTO bronze.eventos_json
FROM '/mnt/landing/eventos/'
FILEFORMAT = JSON
FORMAT_OPTIONS ('mergeSchema' = 'true', 'primitivesAsString' = 'true');

-- Boas práticas COPY INTO:
-- - Para pipelines incrementais, use 'force' = 'false' para não reprocessar.
-- - Sempre valide a pasta de origem: arquivos pequenos demais podem gerar overhead.
-- - Considere compactar após carga com OPTIMIZE.

-- --------------------------------------------------------------------------
-- 3. CTAS (Create Table As Select): Padrões de Transformação
-- --------------------------------------------------------------------------

-- 3.1. CTAS para criar tabela silver a partir da bronze com deduplicação
CREATE OR REPLACE TABLE silver.eventos_unicos
USING DELTA
AS
SELECT
    event_id,
    event_type,
    payload,
    ts,
    ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ts DESC) AS rn
FROM bronze.eventos_brutos
QUALIFY rn = 1;   -- QUALIFY filtra resultados de funções de janela (ver arquivo 04)

-- 3.2. CTAS com particionamento e propriedades
CREATE OR REPLACE TABLE silver.vendas_agregadas
USING DELTA
PARTITIONED BY (ano_mes)
LOCATION 'abfss://silver@storageaccount.dfs.core.windows.net/vendas_agregadas'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'owner' = 'analytics'
)
AS
SELECT
    date_format(data_venda, 'yyyyMM') AS ano_mes,
    id_produto,
    sum(valor) AS total_vendido,
    count(*)    AS num_vendas
FROM silver.vendas
GROUP BY 1, 2;

-- 3.3. CTAS com join e enriquecimento
CREATE TABLE gold.fato_vendas_enriquecido
USING DELTA
AS
SELECT
    v.id_venda,
    v.data_venda,
    c.nome AS nome_cliente,
    c.cidade,
    p.nome AS nome_produto,
    p.categoria,
    v.valor
FROM silver.vendas v
JOIN silver.clientes c ON v.id_cliente = c.id_cliente
JOIN gold.dim_produto p ON v.id_produto = p.id_produto;

-- --------------------------------------------------------------------------
-- 4. MERGE (Upsert) – Padrão Completo e Otimizado
-- --------------------------------------------------------------------------

-- 4.1. Merge típico de CDC (Change Data Capture)
MERGE INTO silver.clientes AS target
USING (
    SELECT
        id_cliente,
        nome,
        cpf,
        data_nascimento,
        cidade,
        'update' AS operacao
    FROM staging.clientes_updates
) AS source
ON target.id_cliente = source.id_cliente
WHEN MATCHED AND source.operacao = 'update' THEN
    UPDATE SET
        nome = source.nome,
        cpf = source.cpf,
        data_nascimento = source.data_nascimento,
        cidade = source.cidade
WHEN NOT MATCHED THEN
    INSERT (id_cliente, nome, cpf, data_nascimento, cidade)
    VALUES (source.id_cliente, source.nome, source.cpf, source.data_nascimento, source.cidade);

-- 4.2. Merge com exclusão lógica (soft delete)
-- Adiciona coluna 'ativo' e apenas desativa registros.
ALTER TABLE silver.clientes ADD COLUMNS (ativo BOOLEAN DEFAULT true);

MERGE INTO silver.clientes AS target
USING staging.clientes_deletados AS source
ON target.id_cliente = source.id_cliente
WHEN MATCHED THEN
    UPDATE SET ativo = false;

-- 4.3. Merge otimizado com Deletion Vectors (Databricks 12.2+)
-- Habilite 'delta.enableDeletionVectors' = 'true' na tabela.
-- Isso acelera merges que envolvem muitas atualizações/deleções.
ALTER TABLE silver.clientes SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

-- O mesmo merge agora usa vetores de deleção em vez de reescrever arquivos inteiros.

-- 4.4. Merge com INSERT * e UPDATE * (sintaxe simplificada Spark 3.4+)
-- Databricks Runtime 13.3+ permite WHEN MATCHED THEN UPDATE SET *
MERGE INTO silver.clientes AS target
USING staging.clientes_completos AS source
ON target.id_cliente = source.id_cliente
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Cuidado: UPDATE SET * copia todas as colunas, incluindo colunas técnicas.
-- Use apenas quando as tabelas têm exatamente as mesmas colunas.

-- --------------------------------------------------------------------------
-- 5. BOAS PRÁTICAS E OBSERVAÇÕES DE PERFORMANCE
-- --------------------------------------------------------------------------

-- - Prefira CTAS para recriar tabelas completas em batch, é rápido e atômico.
-- - COPY INTO é ideal para ingestão inicial e incremental de data lakes.
-- - Para pipelines streaming-to-batch, considere Delta Live Tables (DLT).
-- - Em merges pesados, habilite Deletion Vectors e faça OPTIMIZE regularmente.
-- - Evite INSERT OVERWRITE em tabelas não particionadas: substitui tudo.
-- - Se usar MERGE com condição complexa, ajuste o isolationLevel para Serializable.

-- ============================================================================
-- FIM DO ARQUIVO: 03_dml_patterns.sql
-- ============================================================================
