# 🔥 Spark · PySpark · Spark SQL · Databricks
### Caderno de Estudo e Referência Técnica — Nível Senior Global

> Repositório pessoal de estudo aprofundado sobre a stack de dados moderna com Apache Spark e Databricks.
> Cada arquivo é um notebook documentado, executável e versionado — não só anotação, mas código que roda.

[![Databricks](https://img.shields.io/badge/Databricks-Runtime%2013.x+-FF3621?style=flat&logo=databricks&logoColor=white)](https://databricks.com)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?style=flat&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.x-00ADD8?style=flat)](https://delta.io)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat)](LICENSE)

---

## Por que este repositório existe

A maioria das pessoas estuda Spark lendo documentação ou fazendo cursos e não tem nada para mostrar.
Este repositório é diferente: cada tópico estudado vira um arquivo `.py` ou `.sql` executável,
comentado com analogia + conceito técnico + exemplo real — pronto para ser referenciado no dia a dia
e apresentado em entrevistas.

**Objetivo final:** Databricks Certified Data Engineer Associate → Professional + stack sênior consolidada.

---

## Stack de referência

| Camada | Tecnologia |
|--------|-----------|
| Processamento | Apache Spark 3.5 · PySpark · Spark SQL |
| Armazenamento | Delta Lake 3.x · Parquet · Unity Catalog |
| Plataforma | Databricks Runtime 13.x+ · Workflows · DLT |
| Cloud | AWS S3 / Azure ADLS Gen2 |
| Linguagens | Python 3.10+ · SQL |
| DevOps | Git · GitHub Actions · Databricks Asset Bundles |
| Qualidade | pytest · Great Expectations · DLT Expectations |

---

## Estrutura do repositório

```
spark-databricks-study/
│
├── 📁 00-setup-e-fundamentos-git/     ← ambiente, git workflow, CLI
├── 📁 01-arquitetura-spark/           ← Driver, DAG, Catalyst, Tungsten, Spark UI
├── 📁 02-pyspark-api/                 ← DataFrame API completa, joins, window, UDFs
├── 📁 03-spark-sql/                   ← DDL, DML, CTEs, MERGE, EXPLAIN, funções
├── 📁 04-delta-lake/                  ← ACID, time travel, OPTIMIZE, CDF, liquid clustering
├── 📁 05-unity-catalog/               ← hierarquia, permissões, lineage, Delta Sharing
├── 📁 06-plataforma-databricks/       ← clusters, DLT, Workflows, Autoloader, Secrets
├── 📁 07-performance-tuning/          ← Spark UI, skew, AQE, broadcast, spill, caching
├── 📁 08-streaming/                   ← Structured Streaming, Kafka, watermark, stateful
├── 📁 09-padroes-producao/            ← incremental, SCD, qualidade, testing, cost
├── 📁 10-cicd-devops/                 ← GitHub Actions, Terraform, Asset Bundles
├── 📁 11-cloud-integracao/            ← AWS/Azure/GCP, IAM, PrivateLink, metastore
├── 📁 12-certificacoes/               ← checklists, simulados, recursos
│
├── 📁 templates/                      ← templates reutilizáveis de pipeline e notebook
└── 📄 README.md                       ← este arquivo
```

---

## Roadmap e Progresso

### Módulo 00 — Setup e Git
| Status | Arquivo | Tópico |
|--------|---------|--------|
| ⬜ | `git_workflow.md` | Conventional commits, branching, squash vs merge |
| ⬜ | `databricks_setup.md` | PAT, workspace URL, extensão VS Code, cluster config |
| ⬜ | `environment_config.md` | Python venv, .env, ruff, black, pre-commit hooks |
| ⬜ | `databricks_cli.md` | CLI install, auth, secrets, comandos essenciais |

### Módulo 01 — Arquitetura Spark
| Status | Arquivo | Tópico |
|--------|---------|--------|
| ⬜ | `01_driver_executor.py` | Driver heap, Executors, slots, heartbeat |
| ⬜ | `02_dag_jobs_stages_tasks.py` | DAG, Jobs, Stages, Tasks, narrow vs wide |
| ⬜ | `03_catalyst_optimizer.py` | Analysis → Logical Opt → Physical Plan → Code Gen |
| ⬜ | `04_physical_plan_joins.py` | BHJ, SMJ, SHJ — custo e escolha |
| ⬜ | `05_tungsten_engine.py` | Whole-stage code gen, off-heap, UnsafeRow |
| ⬜ | `06_rdd_dataframe_dataset.py` | Comparação de APIs, interop |
| ⬜ | `07_memoria_executor.py` | Unified Memory Model, execution vs storage pool, spill |
| ⬜ | `08_spark_ui_guide.py` | Leitura de todas as abas do Spark UI |

### Módulo 02 — PySpark API
| Status | Arquivo | Tópico |
|--------|---------|--------|
| ⬜ | `01_sparksession_config.py` | SparkSession builder, configs essenciais |
| ⬜ | `02_schema_types.py` | StructType, todos os tipos, nullable |
| ⬜ | `03_transformacoes_basicas.py` | select, filter, withColumn, when/otherwise |
| ⬜ | `04_aggregations.py` | groupBy, agg, pivot, cube, rollup |
| ⬜ | `05_joins_strategies.py` | Todos os joins, broadcast, semi, anti |
| ⬜ | `06_window_functions.py` | row_number, rank, lag, lead, frames |
| ⬜ | `07_udfs_pandas_udf.py` | UDF vs Pandas UDF (Arrow), performance |
| ⬜ | `08_leitura_escrita.py` | Parquet, Delta, CSV, JSON, JDBC |
| ⬜ | `09_cache_persist.py` | StorageLevel, quando usar, unpersist |
| ⬜ | `10_broadcast_accumulator.py` | Broadcast variables, Accumulators |
| ⬜ | `11_column_functions.py` | String, data, array, struct, JSON, regex |
| ⬜ | `12_streaming_basico.py` | readStream, writeStream, trigger |

### Módulo 03 — Spark SQL
| Status | Arquivo | Tópico |
|--------|---------|--------|
| ⬜ | `01_views_catalog.sql` | TempView, GlobalTempView, namespacing |
| ⬜ | `02_ddl_completo.sql` | CREATE TABLE, PARTITIONED BY, TBLPROPERTIES |
| ⬜ | `03_dml_patterns.sql` | INSERT, COPY INTO, CTAS |
| ⬜ | `04_select_avancado.sql` | CTEs aninhadas, QUALIFY, LATERAL VIEW |
| ⬜ | `05_joins_subqueries.sql` | JOINs, EXISTS, IN vs EXISTS |
| ⬜ | `06_window_functions_sql.sql` | PARTITION BY, ROWS/RANGE BETWEEN |
| ⬜ | `07_merge_into.sql` | MERGE completo, condicional, delete |
| ⬜ | `08_explain_planos.sql` | EXPLAIN FORMATTED, leitura de nós |
| ⬜ | `09_join_hints.sql` | BROADCAST, MERGE, SHUFFLE_HASH hints |
| ⬜ | `10_funcoes_avancadas.sql` | TRANSFORM, FILTER, AGGREGATE (HOF) |
| ⬜ | `11_funcoes_data_string.sql` | date_trunc, regexp_extract, split |
| ⬜ | `12_funcoes_array_struct.sql` | explode, struct, named_struct, schema_of_json |

### Módulo 04 — Delta Lake
| Status | Arquivo | Tópico |
|--------|---------|--------|
| ⬜ | `01_transaction_log.py` | _delta_log, JSON commits, checkpoint |
| ⬜ | `02_acid_snapshot_isolation.py` | OCC, serializability, concurrent writes |
| ⬜ | `03_time_travel.py` | VERSION AS OF, TIMESTAMP AS OF, RESTORE |
| ⬜ | `04_optimize_zorder.sql` | OPTIMIZE, ZORDER, data skipping |
| ⬜ | `05_vacuum.sql` | VACUUM, DRY RUN, retenção |
| ⬜ | `06_schema_evolution.py` | mergeSchema, overwriteSchema, column mapping |
| ⬜ | `07_merge_upsert_patterns.py` | MERGE, SCD Type 1, SCD Type 2 |
| ⬜ | `08_change_data_feed.py` | CDF, readChangeFeed, CDC patterns |
| ⬜ | `09_deletion_vectors.py` | DV vs rewrite, performance |
| ⬜ | `10_liquid_clustering.py` | CLUSTER BY vs ZORDER vs partitionBy |
| ⬜ | `11_table_properties.py` | autoOptimize, autoCompact, targetFileSize |

### Módulo 05 — Unity Catalog
| Status | Arquivo | Tópico |
|--------|---------|--------|
| ⬜ | `01_estrutura_hierarquia.sql` | Metastore, Catalog, Schema, Table, Volume |
| ⬜ | `02_managed_vs_external.sql` | Managed, External, Volumes, storage credentials |
| ⬜ | `03_grants_permissions.sql` | GRANT, REVOKE, herança, service principals |
| ⬜ | `04_row_column_security.sql` | Row Access Policies, Column Masks |
| ⬜ | `05_lineage_tags.py` | Lineage automática, column-level, tags |
| ⬜ | `06_audit_logs.py` | system.access, compliance queries |
| ⬜ | `07_shares_delta_sharing.py` | Delta Sharing, Providers, Recipients |

### Módulo 06 — Plataforma Databricks
| Status | Arquivo | Tópico |
|--------|---------|--------|
| ⬜ | `01_clusters_config.py` | All-Purpose vs Job, policies, autoscaling |
| ⬜ | `02_notebooks_dbutils.py` | %run, dbutils, widgets, secrets |
| ⬜ | `03_workflows_jobs.py` | Tasks, dependências, retry, alertas |
| ⬜ | `04_dlt_basico.py` | @dlt.table, @dlt.view, expect decorators |
| ⬜ | `05_dlt_avancado.py` | Pipeline modes, Unity Catalog, CDC |
| ⬜ | `06_dlt_notebook_workflow.py` | DLT + Notebook + Workflow juntos |
| ⬜ | `07_medallion_architecture.py` | Bronze/Silver/Gold implementação completa |
| ⬜ | `08_autoloader.py` | cloudFiles, schema inference, rescue data |
| ⬜ | `09_lakeflow_jdbc.py` | JDBC, partitionColumn, numPartitions |
| ⬜ | `10_secrets_security.py` | Secret scopes, Key Vault, rotação |
| ⬜ | `11_repos_git_integration.py` | Repos, Git folders, CI/CD |

### Módulo 07 — Performance Tuning
| Status | Arquivo | Tópico |
|--------|---------|--------|
| ⬜ | `01_spark_ui_diagnostico.py` | Método sistemático de diagnóstico |
| ⬜ | `02_data_skew_salting.py` | Detectar skew, salting, AQE skew hints |
| ⬜ | `03_aqe_config.py` | AQE features, coalesce, dynamic join |
| ⬜ | `04_particionamento.py` | Repartition vs Coalesce, sizing |
| ⬜ | `05_broadcast_tuning.py` | Threshold, BHJ vs SMJ, hints |
| ⬜ | `06_spill_gc_tuning.py` | Spill, GC, memory fractions, sizing |
| ⬜ | `07_file_compaction.py` | Small files, OPTIMIZE schedule |
| ⬜ | `08_predicate_pushdown.py` | Filter pushdown, data skipping, statistics |
| ⬜ | `09_caching_strategies.py` | Cache vs persist vs Delta cache |
| ⬜ | `10_configs_referencia.py` | Todas as configs com valores recomendados |

### Módulo 08 — Streaming
| Status | Arquivo | Tópico |
|--------|---------|--------|
| ⬜ | `01_structured_streaming.py` | readStream, writeStream, outputModes |
| ⬜ | `02_triggers_checkpoints.py` | processingTime, once, availableNow |
| ⬜ | `03_watermark_late_data.py` | withWatermark, event time vs processing time |
| ⬜ | `04_stateful_operations.py` | mapGroupsWithState, state cleanup |
| ⬜ | `05_kafka_integration.py` | Kafka source/sink, offsets, schema registry |
| ⬜ | `06_delta_streaming.py` | Delta como source e sink |
| ⬜ | `07_dlt_streaming.py` | DLT com Autoloader, APPLY CHANGES INTO |

### Módulo 09 — Padrões de Produção
| Status | Arquivo | Tópico |
|--------|---------|--------|
| ⬜ | `01_incremental_ingestion.py` | Watermark pattern, idempotência |
| ⬜ | `02_scd_type1_type2.py` | SCD1 com MERGE, SCD2 com is_current |
| ⬜ | `03_data_quality_framework.py` | Great Expectations, DLT, quarantine |
| ⬜ | `04_error_handling.py` | try/except, dead letter tables, retry |
| ⬜ | `05_pipeline_template.py` | Template reutilizável completo |
| ⬜ | `06_expurgo_vacuum.py` | DELETE, VACUUM, GDPR, audit trail |
| ⬜ | `07_testing_pipelines.py` | pytest, chispa, mock SparkSession |
| ⬜ | `08_logging_observability.py` | Structured logging, MLflow, métricas |
| ⬜ | `09_cost_optimization.py` | DBU analysis, rightsizing, spot |

### Módulo 13 — AI Engineering & GenAI
| Status | Arquivo | Tópico |
|--------|---------|--------|
| ⬜ | 01_mlflow_fundamentos.py | Tracking, experiments, registry |
| ⬜ | 02_feature_store.py | Feature engineering e reutilização |
| ⬜ | 03_vector_search.py | Embeddings e busca vetorial |
| ⬜ | 04_rag_basico.py | Retrieval Augmented Generation |
| ⬜ | 05_llm_pipelines.py | Pipelines com LLMs |
| ⬜ | 06_ai_functions.sql | AI Functions do Databricks |
| ⬜ | 07_model_serving.py | Deploy e serving |
| ⬜ | 08_prompt_engineering.md | Prompts para pipelines |
| ⬜ | 09_ai_governance.md | Governança e monitoramento |
| ⬜ | 10_mosaic_ai.md | Mosaic AI completo |

### Módulo 10 — CI/CD e DevOps
| Status | Arquivo | Tópico |
|--------|---------|--------|
| ⬜ | `01_github_actions.yml` | CI: lint → test → deploy |
| ⬜ | `02_terraform_databricks.tf` | Clusters, Jobs, Secrets via IaC |
| ⬜ | `03_bundle_deploy.yml` | Databricks Asset Bundles |
| ⬜ | `04_env_promotion.md` | dev → staging → prod strategy |
| ⬜ | `05_code_quality.md` | ruff, black, mypy, pre-commit |

### Módulo 11 — Cloud e Integração
| Status | Arquivo | Tópico |
|--------|---------|--------|
| ⬜ | `01_aws_s3_iam.py` | S3, IAM roles, instance profiles |
| ⬜ | `02_azure_adls_entra.py` | ADLS Gen2, OAuth, Key Vault |
| ⬜ | `03_gcp_gcs_workload.py` | GCS, Workload Identity |
| ⬜ | `04_network_privatelink.md` | VNet injection, PrivateLink |
| ⬜ | `05_external_metastore.md` | Glue vs Hive vs Unity Catalog |

### Módulo 12 — Certificações
| Status | Arquivo | Tópico |
|--------|---------|--------|
| ⬜ | `associate_checklist.md` | Todos os tópicos da prova Associate |
| ⬜ | `professional_checklist.md` | Tópicos avançados da Professional |
| ⬜ | `simulado_associate.py` | 50 questões comentadas |
| ⬜ | `simulado_professional.py` | 40 questões comentadas |
| ⬜ | `recursos_links.md` | Docs, Academy, blog posts |

---

## Como usar este repositório

### Fluxo de estudo diário

```
1. Assiste a aula (Databricks Academy / curso / doc)
         ↓
2. Abre o arquivo .py ou .sql do tópico no VS Code
         ↓
3. Escreve conceito como comentário Markdown + código executável
         ↓
4. Executa no cluster via extensão Databricks para VS Code
         ↓
5. Ajusta, adiciona observações e exemplos próprios
         ↓
6. Commit e push:
   git add .
   git commit -m "feat(02-pyspark): window functions com LAG e LEAD"
   git push
```

### Formato padrão de cada arquivo

Todo arquivo `.py` segue este padrão — funciona no VS Code e é importável no Databricks:

```python
# Databricks notebook source

# MAGIC %md
# ## Título do Tópico
#
# **Analogia:** [Explicação em linguagem do dia a dia]
#
# **Conceito técnico:** [Definição precisa]
#
# **Quando usar:** [Contexto de aplicação real]

# COMMAND ----------

# imports e setup
from pyspark.sql.functions import col, ...

# COMMAND ----------

# MAGIC %md
# ### Exemplo 1 — [Nome do caso]

# COMMAND ----------

# código comentado linha a linha onde necessário
df = (spark.read
    .format("delta")
    .table("catalog.schema.tabela"))

# COMMAND ----------

# MAGIC %md
# ### ⚠️ Armadilhas e observações
# - Ponto importante 1
# - Diferença que a prova costuma cobrar

# COMMAND ----------
```

### Configuração do ambiente local

```bash
# 1. Clonar o repositório
git clone https://github.com/seu-usuario/spark-databricks-study.git
cd spark-databricks-study

# 2. Criar ambiente virtual Python
python -m venv .venv
source .venv/bin/activate          # Linux/Mac
.venv\Scripts\activate             # Windows

# 3. Instalar dependências de desenvolvimento
pip install pyspark delta-spark ruff black pytest

# 4. Instalar pre-commit hooks
pip install pre-commit
pre-commit install

# 5. Configurar extensão Databricks no VS Code
# Ctrl+Shift+P → "Databricks: Configure"
# URL: https://adb-xxxx.azuredatabricks.net
# Token: gerado em User Settings → Developer → Access Tokens
```

### Conventional commits usados neste repo

```
feat(módulo): adiciona novo tópico ou arquivo
fix(módulo): corrige erro em exemplo de código
docs(módulo): melhora comentários ou explicações
refactor(módulo): reorganiza sem mudar conteúdo
study(módulo): anotações de estudo e revisão
cert(12): progresso nos checklists de certificação
```

---

## Certificações

| Certificação | Status | Meta |
|---|---|---|
| Databricks Certified Data Engineer Associate | ⬜ Não iniciado | Q3 2025 |
| Databricks Certified Data Engineer Professional | ⬜ Não iniciado | Q1 2026 |

**Recursos:**
- [Exam guide Associate (PDF oficial)](https://www.databricks.com/sites/default/files/2023-01/associate-data-engineer-exam-guide.pdf)
- [Databricks Academy](https://www.databricks.com/learn/training)
- [Community Edition — ambiente gratuito para prática](https://community.cloud.databricks.com)
- [`12-certificacoes/associate_checklist.md`](12-certificacoes/associate_checklist.md) — checklist detalhado deste repo

---

## Referências principais

| Recurso | Link |
|---------|------|
| Documentação Apache Spark | https://spark.apache.org/docs/latest |
| Documentação Delta Lake | https://docs.delta.io |
| Documentação Databricks | https://docs.databricks.com |
| PySpark API Reference | https://spark.apache.org/docs/latest/api/python |
| Delta Lake GitHub | https://github.com/delta-io/delta |
| Databricks Engineering Blog | https://www.databricks.com/blog/category/engineering |

---

## Licença

MIT — use à vontade, fork e adapte para o seu próprio caderno.

---

*Spark 3.5 · Delta Lake 3.x · Databricks Runtime 13.x+ · Unity Catalog*
