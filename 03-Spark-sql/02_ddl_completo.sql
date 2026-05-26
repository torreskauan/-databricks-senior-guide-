-- ============================================================================
-- Módulo 03 – Spark SQL (Databricks)
-- Arquivo: 02_ddl_completo.sql
-- Tópico: CREATE TABLE, PARTITIONED BY, TBLPROPERTIES
-- ============================================================================
-- Objetivo: Dominar a criação de tabelas em Databricks com Delta Lake,
--           entendendo partições, propriedades e otimizações.
-- Público: Nível Sênior Global
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. SINTAXE BÁSICA DO CREATE TABLE (Databricks SQL / Spark SQL)
-- --------------------------------------------------------------------------

-- 1.1. Tabela gerenciada (managed) simples com Delta Lake
CREATE TABLE IF NOT EXISTS silver.clientes (
    id_cliente       BIGINT GENERATED ALWAYS AS IDENTITY,  -- Surrogate key (Db SQL)
    nome             STRING NOT NULL,
    cpf              STRING,
    data_nascimento  DATE,
    cidade           STRING,
    data_criacao     TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
LOCATION 'abfss://silver@storageaccount.dfs.core.windows.net/clientes'
COMMENT 'Tabela silver de clientes com histórico.';

-- 1.2. Tabela externa (unmanaged) – o metastore só registra o schema
CREATE TABLE bronze.eventos_brutos (
    event_id      STRING,
    event_type    STRING,
    payload       STRING,
    ts            TIMESTAMP
)
USING JSON
LOCATION 'abfss://bronze@storageaccount.dfs.core.windows.net/eventos/'
OPTIONS ('multiline' 'true');

-- 1.3. Create Table As Select (CTAS) – materializa resultado de query
CREATE OR REPLACE TABLE gold.vendas_diarias
USING DELTA
AS
SELECT
    date_trunc('day', data_venda) AS dia,
    id_produto,
    sum(valor)                    AS total_vendido
FROM silver.vendas
GROUP BY 1, 2;

-- --------------------------------------------------------------------------
-- 2. PARTITIONED BY – Estratégias e Performance
-- --------------------------------------------------------------------------

-- 2.1. Partição por coluna de alta cardinalidade (ex.: data)
-- Ideal para leituras por período, evite partições com muitos arquivos pequenos.
CREATE TABLE silver.vendas (
    id_venda       BIGINT,
    id_cliente     BIGINT,
    id_produto     INT,
    data_venda     DATE,
    valor          DECIMAL(18,2)
)
USING DELTA
PARTITIONED BY (data_venda)
LOCATION 'abfss://silver@storageaccount.dfs.core.windows.net/vendas'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);

-- 2.2. Partição composta (ex.: ano, mês) – cuidado com a cardinalidade total
CREATE TABLE silver.log_acessos (
    id_sessao   STRING,
    pagina      STRING,
    duracao     INT,
    ts          TIMESTAMP,
    ano         INT GENERATED ALWAYS AS (year(ts)),  -- coluna gerada para partição
    mes         INT GENERATED ALWAYS AS (month(ts))
)
USING DELTA
PARTITIONED BY (ano, mes)
LOCATION 'abfss://silver@storageaccount.dfs.core.windows.net/log_acessos';

-- 2.3. Erro comum: particionar por coluna de altíssima cardinalidade
-- Exemplo RUIM: PARTITIONED BY (id_cliente) → milhares de pequenas partições,
-- prejudica performance (small file problem). Prefira ZORDER (ver seção 5).

-- 2.4. Partição em tabela externa (CSV, JSON, Parquet)
CREATE TABLE bronze.transacoes_csv (
    id_transacao  STRING,
    valor         DECIMAL(10,2),
    data          DATE
)
USING CSV
PARTITIONED BY (data)
LOCATION 'abfss://bronze@storageaccount.dfs.core.windows.net/transacoes_csv'
OPTIONS ('header' 'true', 'delimiter' '|');

-- --------------------------------------------------------------------------
-- 3. TBLPROPERTIES – Propriedades de Tabela no Delta Lake
-- --------------------------------------------------------------------------

-- O TBLPROPERTIES define metadados e comportamentos da tabela.
-- No Databricks, a maioria das propriedades é específica do Delta Lake.

CREATE TABLE gold.dim_produto (
    id_produto   INT,
    nome         STRING,
    categoria    STRING,
    preco        DECIMAL(10,2),
    atualizado_em TIMESTAMP
)
USING DELTA
LOCATION 'abfss://gold@storageaccount.dfs.core.windows.net/dim_produto'
TBLPROPERTIES (
    -- Configurações de otimização automática
    'delta.autoOptimize.optimizeWrite' = 'true',      -- compacta arquivos na escrita
    'delta.autoOptimize.autoCompact'   = 'true',      -- compacta após merge/delete

    -- Retenção de histórico (time travel) e vacuum
    'delta.logRetentionDuration'  = 'interval 30 days', -- histórico de transações
    'delta.deletedFileRetentionDuration' = 'interval 7 days', -- vacuum seguro

    -- Configuração de isolamento (concorrência)
    'delta.isolationLevel' = 'WriteSerializable',      -- padrão, evita conflitos

    -- Compressão e formato de arquivo (Parquet por padrão)
    'delta.parquet.compression' = 'SNAPPY',            -- compressão padrão

    -- Habilitar column mapping (suporte a renomear colunas e tipos complexos)
    'delta.columnMapping.mode' = 'name',               -- permite renomear colunas sem reescrita

    -- Habilitar Deletion Vectors para operações DML mais rápidas
    'delta.enableDeletionVectors' = 'true',            -- melhora performance de DELETE/UPDATE

    -- Tabela com liquid clustering (Databricks 13.3+)
    'delta.clusteringColumns' = 'categoria',           -- clustering multidimensional

    -- Propriedade customizada para governança/documentação
    'owner' = 'engenharia_dados',
    'data_classification' = 'gold',
    'refresh_frequency' = 'diario'
);

-- Explicação das principais propriedades:
--
-- * autoOptimize.optimizeWrite: mescla pequenos arquivos em arquivos maiores (128MB padrão)
--   durante a escrita, reduzindo o problema de small files.
-- * autoOptimize.autoCompact: executa compactação após operações como MERGE e DELETE.
-- * logRetentionDuration: define por quanto tempo os arquivos de log são mantidos,
--   permitindo time travel (DESCRIBE HISTORY) e restauração (RESTORE TABLE).
-- * deletedFileRetentionDuration: após o VACUUM, arquivos deletados são retidos por
--   este período para evitar leituras inconsistentes. O padrão é 7 dias.
-- * isolationLevel: WriteSerializable é o padrão; Serializable é mais restritivo,
--   necessário para operações com múltiplas cláusulas (ex.: MERGE com condições não
--   determinísticas). Para máxima performance em pipelines de ingestão, use WriteSerializable.
-- * columnMapping.mode: 'name' permite renomear colunas apenas alterando metadados,
--   sem reescrever dados. Essencial em ambientes evolutivos.
-- * enableDeletionVectors: ativa os vetores de deleção, acelerando DELETE e UPDATE
--   ao marcar linhas deletadas em vez de reescrever arquivos inteiros.
-- * clusteringColumns: substitui o particionamento e ZORDER tradicionais (liquid clustering),
--   simplificando a otimização física.

-- --------------------------------------------------------------------------
-- 4. TIPOS DE TABELA E GERENCIAMENTO DE ESQUEMA
-- --------------------------------------------------------------------------

-- 4.1. Tabela gerenciada vs externa
-- - Gerenciada (managed): DROP TABLE remove dados e metadados.
-- - Externa: DROP TABLE remove apenas o registro no metastore, dados permanecem.
--   Use external para tabelas de staging ou compartilhadas entre clusters/workspaces.

-- 4.2. CREATE OR REPLACE (evolução de schema)
-- Substitui a definição da tabela. Dados antigos podem ser perdidos se a localização
-- for a mesma e o esquema for incompatível. Prefira ALTER TABLE para evoluir sem perda.
CREATE OR REPLACE TABLE silver.eventos_limpos
USING DELTA
AS SELECT * FROM bronze.eventos_brutos WHERE event_type IS NOT NULL;

-- 4.3. ALTER TABLE – Adicionar colunas
ALTER TABLE silver.clientes ADD COLUMNS (telefone STRING, email STRING);

-- 4.4. ALTER TABLE – Alterar comentário da coluna
ALTER TABLE silver.clientes ALTER COLUMN nome COMMENT 'Nome completo do cliente';

-- --------------------------------------------------------------------------
-- 5. ESTRATÉGIAS DE OTIMIZAÇÃO FÍSICA (ZORDER, OPTIMIZE, CLUSTERING)
-- --------------------------------------------------------------------------

-- 5.1. ZORDER em colunas de filtro frequente (junto com partições)
-- Deve ser aplicado após grandes cargas, via comando OPTIMIZE.
OPTIMIZE silver.vendas
ZORDER BY (id_cliente, id_produto);

-- 5.2. Compaction manual (reduzir small files)
OPTIMIZE silver.vendas;

-- 5.3. Liquid Clustering (nova recomendação, substitui particionamento + ZORDER)
CREATE TABLE silver.eventos_clusterizados (
    id_evento    BIGINT,
    tipo         STRING,
    ts           TIMESTAMP,
    id_cliente   BIGINT
)
USING DELTA
CLUSTER BY (tipo, id_cliente)   -- Databricks 13.3+
LOCATION 'abfss://silver@storageaccount.dfs.core.windows.net/eventos_clusterizados';

-- 5.4. Configurar tabela existente para Liquid Clustering
ALTER TABLE silver.vendas CLUSTER BY (data_venda, id_cliente);

-- --------------------------------------------------------------------------
-- 6. EXEMPLOS DE CONSULTA DE METADADOS E MANUTENÇÃO
-- --------------------------------------------------------------------------

-- Ver propriedades atuais da tabela
SHOW TBLPROPERTIES silver.vendas;

-- Ver histórico de versões (Time Travel)
DESCRIBE HISTORY silver.vendas;

-- Consultar uma versão anterior
SELECT * FROM silver.vendas VERSION AS OF 5;
SELECT * FROM silver.vendas TIMESTAMP AS OF '2025-01-01T00:00:00';

-- Restaurar tabela para versão anterior
RESTORE TABLE silver.vendas TO VERSION AS OF 4;

-- Limpar arquivos obsoletos (respeitando retenção)
VACUUM silver.vendas RETAIN 168 HOURS;  -- 7 dias, alinhado com deletedFileRetentionDuration

-- Visualizar partições existentes
SHOW PARTITIONS silver.vendas;

-- Obter informações detalhadas da estrutura (formato, localização, particionamento)
DESCRIBE EXTENDED silver.vendas;

-- --------------------------------------------------------------------------
-- 7. BOAS PRÁTICAS PARA CRIAÇÃO DE TABELAS (Sênior Global)
-- --------------------------------------------------------------------------

-- 7.1. Use sempre DELTA como formato padrão.
-- 7.2. Particione apenas quando a coluna:
--     - for frequentemente usada em filtros com alta seletividade,
--     - tiver cardinalidade moderada (evite > 1 milhão de valores distintos),
--     - não causar skew de dados (distribuição uniforme).
-- 7.3. Prefira Liquid Clustering quando disponível (Databricks 13.3+), pois elimina
--     a necessidade de escolher particionamento manual e otimiza o layout de dados.
-- 7.4. Ative autoOptimize e autoCompact em tabelas que recebem escritas frequentes
--     de pequenos lotes (streaming ou micro-batch).
-- 7.5. Defina sempre logRetentionDuration e deletedFileRetentionDuration de acordo
--     com políticas de compliance e recuperação.
-- 7.6. Habilite deletion vectors em tabelas com muitos DELETEs ou UPDATEs (ex.: CDC).
-- 7.7. Documente o propósito da tabela com COMMENT e TBLPROPERTIES customizados.
-- 7.8. Em ambientes multi-tenant, utilize LOCATION em storage externo para desacoplar
--     dados do metastore (facilita compartilhamento e disaster recovery).
-- 7.9. Valide permissões (ACL) no storage e no metastore (Unity Catalog no Databricks).

-- --------------------------------------------------------------------------
-- 8. INTEGRAÇÃO COM UNITY CATALOG (Databricks moderno)
-- --------------------------------------------------------------------------

-- Se estiver usando Unity Catalog, a sintaxe é catalog.schema.table
CREATE CATALOG IF NOT EXISTS corporativo;
CREATE SCHEMA IF NOT EXISTS corporativo.silver;

CREATE TABLE corporativo.silver.clientes (
    id_cliente       BIGINT GENERATED ALWAYS AS IDENTITY,
    nome             STRING NOT NULL,
    data_nascimento  DATE
)
USING DELTA
PARTITIONED BY (year(data_nascimento))
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'owner' = 'time_financeiro'
);

-- Unity Catalog gerencia permissões com GRANT/REVOKE, e tabelas são externas por padrão.

-- ============================================================================
-- FIM DO ARQUIVO: 02_ddl_completo.sql
-- ============================================================================
