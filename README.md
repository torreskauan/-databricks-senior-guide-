## 📁 Estrutura do Repositório

```bash
📁 databricks-senior-guide/
│
├── 📁 00-essentials/                     # (Opcional) Para revisão rápida
│   ├── spark-architecture-deep.md        # Driver, executor, slots, memória, planos
│   ├── dataframe-vs-rdd-vs-sql.md        # Quando usar cada API
│   └── spark-session-configs.md          # Configurações críticas
│
├── 📁 01-data-ingestion-formats/          # Além de CSV/JSON
│   ├── 01-parquet-optimizations.md
│   ├── 02-delta-lake-advanced/           # Tudo sobre Delta
│   │   ├── delta-merge-upsert.sql
│   │   ├── delta-time-travel.md
│   │   ├── delta-z-order.md
│   │   ├── delta-vacuum-history.md
│   │   └── delta-generated-columns.md
│   ├── 03-json-nested.md
│   ├── 04-binary-files.md
│   ├── 05-jdbc-advanced.md
│   └── 06-kafka-avro-protobuf.md
│
├── 📁 02-transformations-advanced/       # Transformações que ferram o Spark se mal feitas
│   ├── 01-joins-optimization/
│   │   ├── broadcast-hint.md
│   │   ├── skew-joins.md
│   │   └── bucket-joins.md
│   ├── 02-aggregations-window/
│   │   ├── window-functions-advanced.sql
│   │   └── grouping-sets.md
│   ├── 03-udfs-performance/
│   │   ├── pandas-udfs.py
│   │   └── udaf.md
│   ├── 04-arrays-maps/
│   └── 05-lazy-evaluation-debug.md
│
├── 📁 03-spark-sql-advanced/              # Consultas nível BigQuery
│   ├── 01-cte-recursive.md
│   ├── 02-lateral-view-join.md
│   ├── 03-qualify-clause.md
│   ├── 04-table-valued-functions.md
│   └── 05-sql-optimization-hints.md
│
├── 📁 04-performance-tuning/              # O coração da senioridade
│   ├── 01-partitioning-strategies/
│   │   ├── partition-pruning.md
│   │   ├── bucketing-vs-partitioning.md
│   │   └── file-sizing-best-practices.md
│   ├── 02-aqe-and-dpp.md
│   ├── 03-caching-persistence.md
│   ├── 04-spark-memory-tuning.md
│   ├── 05-shuffle-tuning.md
│   ├── 06-skew-handling.md
│   ├── 07-spark-ui-deepdive.md
│   └── 08-predictive-optimization.md
│
├── 📁 05-structured-streaming/            # Streaming de verdade
│   ├── 01-basics-kafka/
│   ├── 02-watermark-late-data.md
│   ├── 03-stream-static-joins.md
│   ├── 04-output-modes-sinks.md
│   ├── 05-triggers-processing-time.md
│   ├── 06-state-store-optimization.md
│   └── 07-monitoring-streaming.md
│
├── 📁 06-databricks-platform/             # Tudo específico da plataforma
│   ├── 01-unity-catalog/
│   │   ├── metastore-catalogs-schemas.md
│   │   ├── external-locations.md
│   │   ├── row-filters-masking.md
│   │   └── lineage-audit.md
│   ├── 02-delta-live-tables/
│   │   ├── dlt-qs.md
│   │   ├── dlt-expectations.md
│   │   └── dlt-autoscaling.md
│   ├── 03-workflows-jobs/
│   │   ├── job-clusters.md
│   │   ├── task-dependencies.md
│   │   ├── retry-policies-alerts.md
│   │   └── git-integration-jobs.md
│   ├── 04-cluster-management/
│   │   ├── photon-vs-spark.md
│   │   ├── spot-instances-cost.md
│   │   └── autoscaling-customization.md
│   ├── 05-secrets-variables.md
│   └── 06-cost-optimization.md
│
├── 📁 07-mlflow-feature-store/            # MLOps na Databricks
│   ├── 01-mlflow-tracking.md
│   ├── 02-model-registry.md
│   ├── 03-feature-store.md
│   ├── 04-inference-batch-streaming.md
│   └── 05-model-serving-api.md
│
├── 📁 08-testing-ci-cd/                  # Engenharia de dados moderna
│   ├── 01-unit-testing.md
│   ├── 02-integration-testing.md
│   ├── 03-data-quality-great-expectations/
│   ├── 04-ci-cd-github-actions.md
│   └── 05-version-controlled-notebooks.md
│
├── 📁 09-advanced-analytics/              # Além do SQL
│   ├── 01-graphframes.md
│   ├── 02-spatial-data.md
│   └── 03-regular-expressions.md
│
├── 📁 10-case-studies-projects/           # Portfólio de sênior
│   ├── project-1-lakehouse-medallion/
│   │   ├── bronze_ingest.py
│   │   ├── silver_clean.py
│   │   ├── gold_aggregations.sql
│   │   └── README.md
│   ├── project-2-real-time-anomaly/
│   └── project-3-multi-hop-optimization/
│
├── 📁 11-cert-preparation/                # Certificação Spark Developer
│   ├── exam-objectives.md
│   ├── practice-questions.md
│   └── hands-on-labs.md
│
├── 📁 scripts/                            # Utilitários
│   ├── generate_test_data.py
│   ├── benchmark_queries.py
│   └── setup_uc_catalogs.sql
│
├── 📁 assets/                             # Diagramas, imagens
├── 📁 notebooks/                          # Notebooks .ipynb ou .py
│
├── Makefile                               # Comandos: make test, make deploy
├── requirements.txt                       # Dependências Python
└── README.md                              # Este arquivo
