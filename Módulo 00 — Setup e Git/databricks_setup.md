
# Databricks Setup — Ambiente Local Completo

> Guia passo a passo para conectar VS Code ao Databricks e ter um ambiente
> de desenvolvimento profissional: edita local, executa no cluster, versiona no GitHub.

---

## Visão geral da arquitetura local

```
┌─────────────────────────────────────────────────────────────┐
│  Sua máquina local                                          │
│                                                             │
│  ┌──────────────┐    ┌──────────────────────────────────┐  │
│  │   VS Code    │───▶│  Extensão Databricks             │  │
│  │              │    │  (sincroniza arquivos via API)    │  │
│  │  .py  .sql   │    └──────────────┬───────────────────┘  │
│  │  .md  .yml   │                   │ HTTPS + PAT           │
│  └──────┬───────┘                   │                       │
│         │ git push                  ▼                       │
│  ┌──────▼───────┐    ┌──────────────────────────────────┐  │
│  │    GitHub    │    │  Databricks Workspace            │  │
│  │  repositório │    │  ┌──────────┐  ┌──────────────┐ │  │
│  └──────────────┘    │  │ Notebook │  │   Cluster    │ │  │
│                      │  │  (sync)  │  │  (execução)  │ │  │
│                      │  └──────────┘  └──────────────┘ │  │
│                      └──────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## 1. Personal Access Token (PAT)

O PAT é a senha que o VS Code usa para se comunicar com o Databricks via API.
Funciona como credencial de autenticação para todas as operações: executar código,
sincronizar arquivos, acessar clusters, chamar a API REST.

### Como gerar

```
1. Acesse seu Databricks Workspace no navegador
2. Clique no seu avatar (canto superior direito)
3. Vá em: Settings → Developer → Access Tokens
4. Clique em "Generate new token"
5. Preencha:
   - Comment: "VS Code local dev" (ou nome descritivo)
   - Lifetime: 90 days (recomendado — renove periodicamente)
6. Clique em "Generate"
7. COPIE O TOKEN IMEDIATAMENTE — ele não será exibido novamente
```

### ⚠️ Segurança do token

```bash
# NUNCA faça isso — token no código
spark = SparkSession.builder \
    .config("token", "dapi1234abcd...")  # ← NUNCA

# NUNCA commite arquivos com token
echo "DATABRICKS_TOKEN=dapi1234..." >> .env
git add .env  # ← NUNCA — adicione .env ao .gitignore

# CORRETO — usar variável de ambiente ou Databricks Secrets
import os
token = os.environ.get("DATABRICKS_TOKEN")

# CORRETO — usar dbutils.secrets dentro do Databricks
token = dbutils.secrets.get(scope="meu-scope", key="meu-token")
```

Adicione ao `.gitignore` imediatamente:

```gitignore
.env
.env.*
.databrickscfg
*.token
```

### Onde o token fica armazenado localmente

A extensão do Databricks para VS Code armazena o token no arquivo
`~/.databrickscfg` (home do usuário):

```ini
# ~/.databrickscfg — gerado automaticamente pela extensão ou CLI
[DEFAULT]
host  = https://adb-1234567890.12.azuredatabricks.net
token = dapi1234abcd5678efgh...

# Múltiplos workspaces (perfis)
[producao]
host  = https://adb-prod.azuredatabricks.net
token = dapiPROD...

[dev]
host  = https://adb-dev.azuredatabricks.net
token = dapiDEV...
```

---

## 2. Workspace URL

A URL do workspace identifica qual instância do Databricks você está acessando.

### Formatos por cloud

```
# Azure
https://adb-1234567890123456.12.azuredatabricks.net

# AWS
https://dbc-a1b2c3d4-e5f6.cloud.databricks.com

# GCP
https://1234567890.12.gcp.databricks.com

# Databricks Community Edition (gratuito — para estudo)
https://community.cloud.databricks.com
```

### Como encontrar a URL

```
Opção 1: Barra de endereço do navegador quando estiver no workspace
Opção 2: Azure Portal → seu recurso Databricks → Overview → URL
Opção 3: AWS → Databricks console → seu workspace → URL
```

### ⚠️ URLs que NÃO funcionam

```bash
# Errado — com path
https://adb-1234.azuredatabricks.net/#notebook/123

# Errado — sem https
adb-1234.azuredatabricks.net

# Correto — só o host
https://adb-1234.azuredatabricks.net
```

---

## 3. Extensão Databricks para VS Code

A extensão oficial da Databricks transforma o VS Code em um IDE completo
para desenvolvimento em Databricks: executa células, sincroniza arquivos,
acessa clusters e secrets sem sair do editor.

### Instalação

```
1. Abra o VS Code
2. Acesse Extensions (Ctrl+Shift+X)
3. Pesquise: "Databricks"
4. Instale: "Databricks" (publisher: Databricks)
   — verificar que é a extensão oficial com o logo vermelho
5. Recarregue o VS Code se solicitado
```

### Configuração inicial

```
Opção A — Via paleta de comandos (recomendado):
  1. Ctrl+Shift+P
  2. Digite: "Databricks: Configure Databricks"
  3. Selecione a opção
  4. Cole a URL do workspace: https://adb-xxxx.azuredatabricks.net
  5. Cole o PAT quando solicitado
  6. A extensão valida a conexão automaticamente

Opção B — Via arquivo de configuração:
  1. Ctrl+Shift+P → "Databricks: Open Configuration"
  2. Edite o JSON gerado (ver seção de configuração abaixo)
```

### O que a extensão faz

```
✅ Executa células .py no cluster diretamente do VS Code
✅ Sincroniza arquivos locais → Databricks Workspace (Repos)
✅ Lista e conecta clusters disponíveis
✅ Exibe output de células inline no editor
✅ Acessa o Databricks file system (DBFS)
✅ Gerencia secrets scopes
✅ Integra com Git e Databricks Repos
```

### Arquivo de configuração da extensão

A extensão gera um `.databricks/project.json` na raiz do repositório.
Adicione ao `.gitignore` — contém configurações específicas da sua máquina:

```json
{
  "host": "https://adb-1234567890.12.azuredatabricks.net",
  "clusterId": "1234-567890-abc123",
  "workspacePath": "/Repos/seu-email@empresa.com/spark-databricks-study",
  "mode": "development"
}
```

```gitignore
# Adicionar ao .gitignore
.databricks/
```

### Executando código

Com a extensão configurada e um cluster selecionado:

```python
# Databricks notebook source

# COMMAND ----------
# Qualquer célula pode ser executada com:
# Shift+Enter → executa célula atual e avança
# Ctrl+Enter  → executa célula atual e permanece

df = spark.read.table("samples.nyctaxi.trips")
df.show(5)

# COMMAND ----------
# O output aparece inline abaixo da célula
print(spark.version)
```

---

## 4. Cluster Config

O cluster é a infraestrutura que executa seu código. Configurá-lo corretamente
evita custos desnecessários e garante performance adequada para estudo.

### Tipos de cluster relevantes para estudo

| Tipo | Uso | Custo | Config recomendada |
|------|-----|-------|--------------------|
| Single Node | Estudo local, datasets pequenos | Baixo | 1 nó, 14-30 GB RAM |
| Multi-node small | Testar paralelismo e particionamento | Médio | 1 driver + 2 workers |
| SQL Warehouse | Queries SQL interativas | Variável | Serverless se disponível |

### Configuração recomendada para estudo

```
# Cluster de desenvolvimento pessoal — configurações ideais:

Nome:           dev-estudo-spark
Cluster mode:   Single Node (para estudo solo)
                OU Standard (para testar paralelismo)

Databricks Runtime: 14.x LTS ML
  → LTS = Long Term Support (mais estável)
  → ML = inclui MLflow, sklearn (não pesa muito e pode ser útil)

Node type (AWS):    m5.xlarge   (4 cores, 16 GB — suficiente para estudo)
Node type (Azure):  Standard_DS3_v2 (4 cores, 14 GB)

Auto termination:   30 minutos  ← CRÍTICO — evita cobranças esquecidas
Autoscaling:        Desativado  (para estudo, fixo é mais previsível)
Spot instances:     Ativado     (reduz custo em até 70% — OK para estudo)
```

### Spark configs recomendadas para estudo

Adicione em `Advanced Options → Spark Config` na UI do cluster:

```properties
# Configurações úteis para desenvolvimento e aprendizado

# AQE ativado (já é padrão, mas bom deixar explícito)
spark.sql.adaptive.enabled true
spark.sql.adaptive.coalescePartitions.enabled true
spark.sql.adaptive.skewJoin.enabled true

# Partições de shuffle — reduzir para datasets pequenos em estudo
# (padrão 200 é alto para datasets de desenvolvimento)
spark.sql.shuffle.partitions 8

# Forçar BroadcastHashJoin para joins menores (útil para ver o plano)
spark.sql.autoBroadcastJoinThreshold 10485760

# Habilitar ANSI SQL (mais rigoroso — bom para aprender)
spark.sql.ansi.enabled true

# Log level — reduzir verbosidade desnecessária
spark.databricks.driver.dbutils.fs.implicit.cache.size 10g
```

### Auto Termination — por que é crítico

```
Databricks cobra por DBU (Databricks Unit) por hora de cluster ligado.
Um cluster esquecido ligado overnight pode gerar custos significativos.

Auto Termination de 30 minutos:
→ Cluster desliga sozinho após 30 min sem atividade
→ Você reinicia quando precisar (leva ~2 min)
→ Salva potencialmente horas de custo por descuido
```

### Acessar o cluster ID

O Cluster ID é necessário para configurar a extensão VS Code:

```
UI: Compute → seu cluster → Advanced Options → Tags
    O ID aparece como: 1234-567890-abc123def456

URL: quando você abre o cluster na UI, a URL contém o ID:
    https://adb-xxxx.net/o/123/#setting/clusters/1234-567890-abc123/configuration
                                                   ↑ este é o cluster ID

Via CLI:
    databricks clusters list
```

### Cluster libraries — instalar dependências

```python
# Via UI: Cluster → Libraries → Install New → PyPI
# Útil para: great_expectations, chispa (testing), delta-spark fora do Databricks

# No notebook (instalação temporária — só dura enquanto o cluster está ligado)
%pip install great-expectations chispa

# Para persistir entre restarts, instale via cluster library na UI
# ou via init script
```

---

## 5. Databricks Community Edition (estudo gratuito)

Se você não tem acesso a um workspace pago, o Community Edition é suficiente
para estudar a maioria dos tópicos.

### Limitações do Community Edition

```
✅ Apache Spark completo
✅ Delta Lake
✅ Notebooks Python, SQL, Scala, R
✅ DBFS

❌ Unity Catalog (não disponível)
❌ Databricks Workflows / Jobs
❌ Delta Live Tables
❌ MLflow (versão limitada)
❌ Múltiplos clusters (apenas 1 cluster por vez)
❌ Cluster persiste por no máximo 2h (termina automaticamente)
```

### Acesso

```
URL:    https://community.cloud.databricks.com
Signup: https://www.databricks.com/try-databricks
        → Selecione "Community Edition" (sem cartão de crédito)
```

---

## 6. Verificação da conexão

Após configurar tudo, verifique se está funcionando:

### Via VS Code

```
1. Abra a paleta: Ctrl+Shift+P → "Databricks: Connect"
2. Selecione seu cluster na lista lateral
3. O ícone na status bar deve mostrar o cluster conectado
4. Abra qualquer .py do repositório
5. Pressione Shift+Enter em uma célula
6. Output deve aparecer inline em ~5 segundos
```

### Via notebook de validação

Crie `00-setup-e-fundamentos-git/validate_setup.py`:

```python
# Databricks notebook source
# Rode este arquivo para validar que o ambiente está correto

# COMMAND ----------

# MAGIC %md
# ## Validação do ambiente
# Execute célula por célula. Todas devem rodar sem erro.

# COMMAND ----------

# 1. Versão do Spark
print(f"Spark version: {spark.version}")
print(f"Python version: {sc.pythonVer}")

# COMMAND ----------

# 2. Delta Lake disponível
from delta.tables import DeltaTable
print("✅ Delta Lake OK")

# COMMAND ----------

# 3. SparkSession ativa
print(f"App name: {spark.sparkContext.appName}")
print(f"Master: {spark.sparkContext.master}")
print(f"Executors: {sc.defaultParallelism}")

# COMMAND ----------

# 4. Criar e ler tabela Delta temporária
import tempfile, os
from pyspark.sql import Row

tmp_path = "/tmp/validate_setup_delta"

dados = [Row(id=1, nome="teste"), Row(id=2, nome="validacao")]
df = spark.createDataFrame(dados)
df.write.format("delta").mode("overwrite").save(tmp_path)

df_lido = spark.read.format("delta").load(tmp_path)
df_lido.show()
print("✅ Delta read/write OK")

# COMMAND ----------

# 5. dbutils disponível
dbutils.fs.ls("/")
print("✅ dbutils OK")

# COMMAND ----------

# 6. Unity Catalog (se disponível)
try:
    spark.sql("SHOW CATALOGS").show()
    print("✅ Unity Catalog OK")
except Exception as e:
    print(f"⚠️  Unity Catalog não disponível: {e}")
    print("   (Normal no Community Edition)")

# COMMAND ----------

print("\n✅ Ambiente validado com sucesso!")
print(f"   Spark:  {spark.version}")
print(f"   Cluster: {spark.sparkContext.appName}")
```

---

## 7. Extensões VS Code recomendadas

```
Obrigatórias:
  - Databricks            (publisher: Databricks)
  - Python                (publisher: Microsoft)
  - Pylance               (publisher: Microsoft) — type checking

Recomendadas:
  - GitLens               — histórico Git inline, blame, comparação de branches
  - Git Graph             — visualização do grafo de commits
  - Error Lens            — erros inline no editor
  - Even Better TOML      — para pyproject.toml e configs
  - Rainbow CSV           — visualização de arquivos CSV
  - indent-rainbow        — identação colorida (útil em Python)

Para SQL:
  - SQLTools              — syntax highlight e formatação SQL
  - SQLTools Databricks   — driver do Databricks para SQLTools
```

### Settings recomendados do VS Code

Adicione em `.vscode/settings.json` (não commitar — já está no `.gitignore`):

```json
{
  "editor.formatOnSave": true,
  "editor.rulers": [88],
  "python.defaultInterpreterPath": "${workspaceFolder}/.venv/bin/python",
  "python.formatting.provider": "black",
  "[python]": {
    "editor.defaultFormatter": "ms-python.black-formatter"
  },
  "files.trimTrailingWhitespace": true,
  "files.insertFinalNewline": true,
  "editor.tabSize": 4,
  "editor.insertSpaces": true,
  "databricks.clusters.onlyShowAccessibleClusters": true
}
```

---

## 8. Referência rápida — Checklist de setup

```
PRIMEIRA VEZ — fazer uma vez só:

[ ] Conta Databricks criada (Community ou workspace da empresa)
[ ] PAT gerado em Settings → Developer → Access Tokens
[ ] URL do workspace anotada (https://adb-xxxx.net)
[ ] VS Code instalado
[ ] Extensão Databricks instalada (publisher: Databricks)
[ ] Extensão Python + Pylance instaladas
[ ] Extensão configurada: Ctrl+Shift+P → "Databricks: Configure"
[ ] Cluster criado com Auto Termination 30 min
[ ] Cluster selecionado na extensão
[ ] validate_setup.py executado sem erros
[ ] .gitignore inclui: .env, .databricks/, .databrickscfg


TODA VEZ QUE FOR ESTUDAR:

[ ] Abrir VS Code na pasta do repositório
[ ] Verificar se cluster está rodando (ou iniciar — ~2 min)
[ ] git pull origin main (pegar últimas mudanças)
[ ] git checkout -b feat/modulo-topico (nova branch)
[ ] Estudar, escrever, executar no cluster
[ ] git add, commit, push
[ ] PR no GitHub → Squash and Merge
[ ] git checkout main && git pull
```

---

*Referências: [Databricks VS Code Extension](https://docs.databricks.com/dev-tools/vscode-ext.html) ·
[Personal Access Tokens](https://docs.databricks.com/dev-tools/api/latest/authentication.html) ·
[Cluster Configuration](https://docs.databricks.com/clusters/configure.html)*
