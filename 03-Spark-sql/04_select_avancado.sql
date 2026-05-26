-- ============================================================================
-- Módulo 03 – Spark SQL (Databricks)
-- Arquivo: 04_select_avancado.sql
-- Tópico: CTEs aninhadas, QUALIFY, LATERAL VIEW
-- ============================================================================
-- Objetivo: Dominar consultas avançadas com expressões de janela, CTEs,
--           processamento de dados semi-estruturados e lateral view.
-- Público: Nível Sênior Global
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. CTEs ANINHADAS (Common Table Expressions)
-- --------------------------------------------------------------------------

-- 1.1. CTE simples para legibilidade
WITH clientes_sp AS (
    SELECT * FROM silver.clientes WHERE cidade = 'São Paulo'
),
vendas_2025 AS (
    SELECT * FROM silver.vendas WHERE year(data_venda) = 2025
)
SELECT c.nome, sum(v.valor) AS total_gasto
FROM vendas_2025 v
JOIN clientes_sp c ON v.id_cliente = c.id_cliente
GROUP BY c.nome;

-- 1.2. CTE encadeada com lógica de negócio
WITH base_clean AS (
    SELECT
        id_cliente,
        valor,
        data_venda,
        CASE
            WHEN valor > 10000 THEN 'alto'
            WHEN valor > 1000  THEN 'medio'
            ELSE 'baixo'
        END AS ticket
    FROM silver.vendas
    WHERE data_venda >= current_date() - interval 90 days
),
ticket_rank AS (
    SELECT
        id_cliente,
        ticket,
        count(*) AS num_compras,
        sum(valor) AS total_gasto,
        ROW_NUMBER() OVER (PARTITION BY id_cliente ORDER BY count(*) DESC) AS rn
    FROM base_clean
    GROUP BY id_cliente, ticket
)
SELECT id_cliente, ticket, num_compras, total_gasto
FROM ticket_rank
WHERE rn = 1;   -- ticket mais frequente por cliente

-- 1.3. CTE recursiva (compatível com Databricks Runtime 14.2+)
-- Atenção: Spark SQL suporta CTE recursiva apenas em versões recentes.
-- Exemplo: gerar série de datas (calendário).
WITH RECURSIVE date_series(d) AS (
    SELECT date'2025-01-01'
    UNION ALL
    SELECT d + interval 1 day FROM date_series WHERE d < date'2025-01-31'
)
SELECT d FROM date_series;

-- Se RECURSIVE não estiver disponível, use EXPLODE + SEQUENCE:
SELECT explode(sequence(to_date('2025-01-01'), to_date('2025-01-31'), interval 1 day)) AS d;

-- --------------------------------------------------------------------------
-- 2. QUALIFY: Filtragem Pós-Função de Janela
-- --------------------------------------------------------------------------

-- QUALIFY é uma cláusula que permite filtrar os resultados de funções de janela
-- sem subconsulta extra. Suportado a partir do Spark 3.2 / DBR 10.4.

-- 2.1. Exemplo básico: top 1 por grupo
SELECT
    id_produto,
    data_venda,
    valor,
    ROW_NUMBER() OVER (PARTITION BY id_produto ORDER BY data_venda DESC) AS rn
FROM silver.vendas
QUALIFY rn = 1;   -- apenas a venda mais recente de cada produto

-- 2.2. Filtrando outliers com função de janela
-- Encontrar clientes cujo gasto está acima da média + 2 desvios
WITH stats AS (
    SELECT
        id_cliente,
        sum(valor) AS total_gasto,
        AVG(sum(valor)) OVER () AS media_global,
        STDDEV(sum(valor)) OVER () AS desvio_global
    FROM silver.vendas
    GROUP BY id_cliente
)
SELECT id_cliente, total_gasto
FROM stats
QUALIFY total_gasto > media_global + 2 * desvio_global;

-- 2.3. QUALIFY com múltiplas funções de janela
SELECT
    id_cliente,
    data_venda,
    valor,
    SUM(valor) OVER (PARTITION BY id_cliente ORDER BY data_venda
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS gasto_acumulado,
    ROW_NUMBER() OVER (PARTITION BY id_cliente ORDER BY data_venda) AS posicao
FROM silver.vendas
QUALIFY gasto_acumulado > 5000 AND posicao <= 10;

-- Vantagens do QUALIFY:
-- - Evita uma camada extra de subconsulta ou CTE apenas para filtrar window functions.
-- - Melhora legibilidade e facilita manutenção.
-- - O otimizador pode aplicar a filtragem mais cedo, se possível.

-- --------------------------------------------------------------------------
-- 3. LATERAL VIEW: Explodindo Arrays, Maps e Estruturas
-- --------------------------------------------------------------------------

-- 3.1. LATERAL VIEW com EXPLODE de array
-- Tabela de exemplo: cadastro de clientes com vários telefones
CREATE OR REPLACE TEMP VIEW clientes_contatos AS
SELECT 1 AS id_cliente, ARRAY('(11) 99999-0001', '(11) 3333-0002') AS telefones
UNION ALL
SELECT 2 AS id_cliente, ARRAY('(21) 88888-0003') AS telefones;

-- Explodir array de telefones para gerar uma linha por telefone
SELECT
    id_cliente,
    telefone
FROM clientes_contatos
LATERAL VIEW explode(telefones) AS telefone;

-- 3.2. LATERAL VIEW com POSEXPLODE (posição + valor)
-- Útil para manter ordem dos itens
SELECT
    id_cliente,
    posicao,
    telefone
FROM clientes_contatos
LATERAL VIEW posexplode(telefones) AS posicao, telefone;

-- 3.3. LATERAL VIEW em consulta complexa com JSON aninhado
-- Exemplo: logs de eventos com array de ações
CREATE OR REPLACE TEMP VIEW logs_sessao AS
SELECT
    1 AS id_sessao,
    FROM_JSON('{"paginas": ["home","produto","carrinho","checkout"]}',
              'paginas ARRAY<STRING>') AS acoes
UNION ALL
SELECT
    2 AS id_sessao,
    FROM_JSON('{"paginas": ["home","blog","contato"]}',
              'paginas ARRAY<STRING>') AS acoes;

-- Consultar sessões que visitaram a página "carrinho"
SELECT id_sessao, pagina
FROM logs_sessao
LATERAL VIEW explode(acoes.paginas) AS pagina
WHERE pagina = 'carrinho';

-- 3.4. LATERAL VIEW com múltiplas explosões (cuidado com produto cartesiano)
-- Clientes com várias habilidades e vários projetos
CREATE OR REPLACE TEMP VIEW devs AS
SELECT
    1 AS dev_id,
    ARRAY('Scala','Python','SQL') AS skills,
    ARRAY('ProjA','ProjB') AS projetos;

SELECT dev_id, skill, projeto
FROM devs
LATERAL VIEW explode(skills) AS skill
LATERAL VIEW explode(projetos) AS projeto;
-- Gera produto cruzado: 3 skills x 2 projetos = 6 linhas

-- 3.5. LATERAL VIEW OUTER – mantém registros mesmo com array vazio/nulo
-- Similar ao LEFT JOIN com a função de explosão
SELECT id_cliente, telefone
FROM clientes_contatos
LATERAL VIEW OUTER explode(ARRAY() -- array vazio
) AS telefone;
-- Retorna as linhas originais com telefone NULL em vez de excluí-las.

-- 3.6. LATERAL VIEW vs funções de alto desempenho nativas
-- A partir do Spark 3.0+, funções como `exists`, `filter`, `transform` em arrays
-- podem substituir LATERAL VIEW em muitos casos com melhor performance.
-- Exemplo: verificar se array contém elemento.
SELECT id_cliente,
       exists(telefones, t -> t like '(11)%') AS tem_ddd_11
FROM clientes_contatos;

-- Prefira funções nativas para filtros e transformações simples em arrays.
-- Use LATERAL VIEW para normalizar dados em formato relacional.

-- --------------------------------------------------------------------------
-- 4. COMBINAÇÕES AVANÇADAS: CTE + QUALIFY + LATERAL VIEW
-- --------------------------------------------------------------------------

-- Exemplo completo: análise de sessões web com eventos
WITH sessoes AS (
    SELECT
        id_sessao,
        id_usuario,
        ts,
        -- extrai ações de um campo JSON
        from_json(payload, 'array<struct<acao:string, pagina:string>>') AS eventos
    FROM bronze.sessoes_brutas
    WHERE dt = '2025-01-20'
),
acoes_explodidas AS (
    SELECT
        id_sessao,
        id_usuario,
        ts,
        evento.acao,
        evento.pagina
    FROM sessoes
    LATERAL VIEW explode(eventos) AS evento
),
com_ranking AS (
    SELECT
        id_usuario,
        pagina,
        count(*) AS visitas,
        ROW_NUMBER() OVER (PARTITION BY id_usuario ORDER BY count(*) DESC) AS rn_pagina,
        RANK() OVER (ORDER BY count(*) DESC) AS rank_global
    FROM acoes_explodidas
    GROUP BY id_usuario, pagina
)
SELECT id_usuario, pagina, visitas, rank_global
FROM com_ranking
QUALIFY rn_pagina <= 2   -- top 2 páginas mais visitadas por usuário
ORDER BY rank_global;

-- --------------------------------------------------------------------------
-- 5. SUBQUERY CORRELACIONADA E EXISTS
-- --------------------------------------------------------------------------

-- 5.1. EXISTS para semi-join (mais eficiente que IN em muitos casos)
SELECT c.id_cliente, c.nome
FROM silver.clientes c
WHERE EXISTS (
    SELECT 1 FROM silver.vendas v
    WHERE v.id_cliente = c.id_cliente
      AND v.valor > 5000
);

-- 5.2. NOT EXISTS para anti-join
SELECT c.id_cliente, c.nome
FROM silver.clientes c
WHERE NOT EXISTS (
    SELECT 1 FROM silver.vendas v
    WHERE v.id_cliente = c.id_cliente
);

-- 5.3. Subconsulta escalar no SELECT
SELECT
    id_venda,
    valor,
    (SELECT AVG(valor) FROM silver.vendas) AS media_geral,
    valor - (SELECT AVG(valor) FROM silver.vendas) AS diferenca_media
FROM silver.vendas
LIMIT 10;

-- Cuidado: subconsultas escalares podem ser re-executadas para cada linha
-- se não forem otimizadas. Prefira CTE com CROSS JOIN ou window functions.

-- 5.4. Alternativa otimizada com window function
SELECT
    id_venda,
    valor,
    AVG(valor) OVER () AS media_geral,
    valor - AVG(valor) OVER () AS diferenca_media
FROM silver.vendas
LIMIT 10;

-- --------------------------------------------------------------------------
-- 6. DICAS DE PERFORMANCE
-- --------------------------------------------------------------------------

-- - LATERAL VIEW explode gera novas linhas, o que multiplica dados. Sempre filtre o mais cedo possível.
-- - QUALIFY pode ser empurrado para a fase de janela se a condição for sobre funções como ROW_NUMBER,
--   evitando a materialização completa.
-- - CTEs são materializadas ou reavaliadas conforme a decisão do Catalyst; para reuso garantido,
--   use CACHE TABLE ou crie uma tabela temporária.
-- - Em subconsultas correlacionadas, o Spark pode otimizar usando BroadcastNestedLoopJoin ou
--   transformá-las em joins normais; fique atento ao plano de execução.

-- ============================================================================
-- FIM DO ARQUIVO: 04_select_avancado.sql
-- ============================================================================
