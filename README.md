📁 databricks-senior-guide/
│
├── 📁 00-essentials/                     # (Opcional) Para revisão rápida
│   ├── spark-architecture-deep.md        # Driver, executor, slots, memória, planos
│   ├── dataframe-vs-rdd-vs-sql.md        # Quando usar cada API
│   └── spark-session-configs.md          # Configurações críticas (spark.sql.shuffle.partitions, etc.)
│
├── 📁 01-data-ingestion-formats/          # Além de CSV/JSON
│   ├── 01-parquet-optimizations.md       # Row groups, dictionary encoding, predicate pushdown
│   ├── 02-delta-lake-advanced/           # Tudo sobre Delta
│   │   ├── delta-merge-upsert.sql        # MERGE, INSERT, UPDATE, DELETE
│   │   ├── delta-time-travel.md          # VERSION AS OF, TIMESTAMP AS OF
│   │   ├── delta-z-order.md              # Z-order clustering (optimize)
│   │   ├── delta-vacuum-history.md       # VACUUM, history, table properties
│   │   └── delta-generated-columns.md    # Identity, generated columns
│   ├── 03-json-nested.md                 # explode, from_json, schema evolution
│   ├── 04-binary-files.md                # Leitura de PDF, imagens com spark.files
│   ├── 05-jdbc-advanced.md               # Parallel reads, pushdown predicates, connection pooling
│   └── 06-kafka-avro-protobuf.md         # Streaming com schemas complexos
│
├── 📁 02-transformations-advanced/       # Transformações que ferram o Spark se mal feitas
│   ├── 01-joins-optimization/            # Tipos de join (broadcast, sort-merge, shuffle hash)
│   │   ├── broadcast-hint.md             # /*+ BROADCAST(t) */, autoBroadcastJoinThreshold
│   │   ├── skew-joins.md                 # Salting, hot keys, adaptive query execution (AQE)
│   │   └── bucket-joins.md               # Bucketing para eliminar shuffles
│   ├── 02-aggregations-window/           # GroupBy vs rollup vs cube vs pivot
│   │   ├── window-functions-advanced.sql # ROWS vs RANGE, UNBOUNDED PRECEDING, frame clauses
│   │   └── grouping-sets.md
│   ├── 03-udfs-performance/              # UDFs matam performance -> usar pandas UDFs (Vectorized)
│   │   ├── pandas-udfs.py                # Series to Series, Iterator, Grouped Map
│   │   └── udaf.md                       # User Defined Aggregate Functions
│   ├── 04-arrays-maps/                   # Higher-order functions (transform, filter, exists)
│   └── 05-lazy-evaluation-debug.md       # .explain(), .checkpoint(), .localCheckpoint()
│
├── 📁 03-spark-sql-advanced/              # Consultas nível BigQuery
│   ├── 01-cte-recursive.md               # (A partir do Spark 3.x) CTE recursivo
│   ├── 02-lateral-view-join.md           # LATERAL VIEW explode
│   ├── 03-qualify-clause.md              # QUALIFY para filtrar window functions
│   ├── 04-table-valued-functions.md      # explode, posexplode, json_tuple
│   └── 05-sql-optimization-hints.md      # /*+ COALESCE */, /*+ REPARTITION */, /*+ REBALANCE */
│
├── 📁 04-performance-tuning/              # O coração da senioridade
│   ├── 01-partitioning-strategies/       # Particionamento por coluna, dynamic partition pruning
│   │   ├── partition-pruning.md
│   │   ├── bucketing-vs-partitioning.md
│   │   └── file-sizing-best-practices.md # Evitar small files (1GB por partição)
│   ├── 02-aqe-and-dpp.md                 # Adaptive Query Execution + Dynamic Partition Pruning
│   ├── 03-caching-persistence.md         # cache, persist(StorageLevel), checkpoints
│   ├── 04-spark-memory-tuning.md         # spark.memory.fraction, off-heap, py4j overhead
│   ├── 05-shuffle-tuning.md              # spark.sql.shuffle.partitions, spark.sql.adaptive.coalescePartitions
│   ├── 06-skew-handling.md               # Técnicas manuais e automáticas (AQE skew join)
│   ├── 07-spark-ui-deepdive.md           # Interpretar SQL tab, Stages, Tasks, Skew, Garbage Collection
│   └── 08-predictive-optimization.md     # Bloom filters, column stats, histogram
│
├── 📁 05-structured-streaming/            # Streaming de verdade
│   ├── 01-basics-kafka/                  # readStream, writeStream, checkpointing
│   ├── 02-watermark-late-data.md         # Watermark, allowed lateness, append vs update vs complete
│   ├── 03-stream-static-joins.md         # Stream-Static join, Stream-Stream join (inner, left, etc.)
│   ├── 04-output-modes-sinks.md          # foreachBatch, foreach, console, memory, kafka
│   ├── 05-triggers-processing-time.md    # Default, ProcessingTime, Once, Continuous (experimental)
│   ├── 06-state-store-optimization.md    # State store format, min/max watermarks, TTL
│   └── 07-monitoring-streaming.md        # StreamingQueryListener, metrics, rate limiting
│
├── 📁 06-databricks-platform/             # Tudo que é específico da plataforma
│   ├── 01-unity-catalog/                 # Governança unificada
│   │   ├── metastore-catalogs-schemas.md # Hierarquia: metastore -> catalog -> schema -> table
│   │   ├── external-locations.md         # Storage credentials, external tables
│   │   ├── row-filters-masking.md        # Row-level security, column masking
│   │   └── lineage-audit.md              # System tables (access, lineage, billing)
│   ├── 02-delta-live-tables/             # Declarative pipelines
│   │   ├── dlt-qs.md                     # CREATE STREAMING LIVE TABLE, EXPECT clauses
│   │   ├── dlt-expectations.md           # Constraints (expect, expect_or_drop, expect_or_fail)
│   │   └── dlt-autoscaling.md
│   ├── 03-workflows-jobs/                # Orquestração nativa
│   │   ├── job-clusters.md               # Job clusters vs all-purpose
│   │   ├── task-dependencies.md          # Python wheel, notebook, spark jar, dbt
│   │   ├── retry-policies-alerts.md
│   │   └── git-integration-jobs.md       # Deploy notebooks via git tags
│   ├── 04-cluster-management/            # Escolha de runtime, tipos de instância
│   │   ├── photon-vs-spark.md            # Photon engine (acelerador)
│   │   ├── spot-instances-cost.md
│   │   └── autoscaling-customization.md
│   ├── 05-secrets-variables.md           # Databricks secrets, environment variables
│   └── 06-cost-optimization.md           # DBUs, serverless vs pro, idle clusters, workload patterns
│
├── 📁 07-mlflow-feature-store/            # MLOps na Databricks
│   ├── 01-mlflow-tracking.md             # mlflow.start_run(), log params, metrics, models
│   ├── 02-model-registry.md              # Staging, Production, aliases, versioning
│   ├── 03-feature-store.md               # Feature engineering, online/offline serving
│   ├── 04-inference-batch-streaming.md   # Batch inference com Spark + streaming
│   └── 05-model-serving-api.md           # Real-time serving endpoints
│
├── 📁 08-testing-ci-cd/                  # Engenharia de dados moderna
│   ├── 01-unit-testing.md                # Chispa, pytest, assertDataFrameEqual
│   ├── 02-integration-testing.md         # Testes com emuladores (local spark, delta)
│   ├── 03-data-quality-great-expectations/ # Integração com Great Expectations
│   ├── 04-ci-cd-github-actions.md        # Rodar testes no PR, deploy de DLT/jobs
│   └── 05-version-controlled-notebooks.md # Melhores práticas (evitar UI, usar .py importável)
│
├── 📁 09-advanced-analytics/              # Além do SQL (opcional, mas valorizado)
│   ├── 01-graphframes.md                 # Análise de grafos (PageRank, connected components)
│   ├── 02-spatial-data.md                # Magellan, geo joins
│   └── 03-regular-expressions.md         # RegEx sério, regexp_extract, regexp_replace
│
├── 📁 10-case-studies-projects/           # Seu portfólio de sênior
│   ├── project-1-lakehouse-medallion/    # Bronze/Silver/Gold com DLT
│   │   ├── bronze_ingest.py              # Leitura raw (Kafka, cloud storage)
│   │   ├── silver_clean.py               # DLT: expectações, dedup
│   │   ├── gold_aggregations.sql         # Star schema, métricas
│   │   └── README.md                     # Explicação da arquitetura
│   ├── project-2-real-time-anomaly/      # Streaming + ML
│   └── project-3-multi-hop-optimization/ # Exercício de tuning (1TB de dados)
│
├── 📁 11-cert-preparation/                # Spark Developer (Databricks Certified)
│   ├── exam-objectives.md                # Mapeamento oficial
│   ├── practice-questions.md             # 100+ questões comentadas
│   └── hands-on-labs.md
│
├── 📁 scripts/                            # Utilitários
│   ├── generate_test_data.py              # Gerar parquets com volumes grandes
│   ├── benchmark_queries.py               # Medir tempo de execução
│   └── setup_uc_catalogs.sql              # Scripts de inicialização
│
├── 📁 assets/                             # Diagramas (excalidraw, draw.io)
├── 📁 notebooks/                          # Notebooks .ipynb ou .py (com formatação markdown)
│
├── Makefile                               # Comandos: make test, make deploy, make format
├── requirements.txt                       # Chispa, pytest, black, flake8
└── README.md
