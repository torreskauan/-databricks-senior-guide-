# Environment Config — Ambiente Python Profissional

> Configuração completa do ambiente local de desenvolvimento:
> isolamento com venv, variáveis de ambiente, dependências versionadas,
> linting, formatação automática e hooks de qualidade no commit.
>
> Objetivo: reproduzir em qualquer máquina, em qualquer momento,
> com um único comando — sem "funciona na minha máquina".

---

## Por que configurar o ambiente corretamente?

A maioria dos problemas de "funcionava ontem e hoje não funciona" vem de
três causas: dependências sem versão fixada, segredos no código, e código
sem padrão de formatação. Este guia resolve os três.

```
Sem configuração adequada:          Com este guia:
─────────────────────────           ──────────────────────────────
pip install pyspark                 requirements.txt versionado
token = "dapi123..." no código      .env no .gitignore + python-dotenv
código formatado diferente          black + ruff padronizam tudo
commitar com erro de sintaxe        pre-commit bloqueia o commit
"funciona na minha máquina"         venv reproduzível em qualquer lugar
```

---

## 1. Python Virtual Environment (venv)

### Analogia

Um venv é como um quarto separado dentro da sua casa. Cada projeto tem seu
próprio quarto com suas próprias coisas — instalar algo no quarto do
projeto A não bagunça o quarto do projeto B, e nada do sistema operacional
entra sem permissão.

### Por que usar venv e não instalar globalmente

```
Sem venv (global):
  projeto-spark    → precisa de pyspark 3.4
  projeto-legado   → precisa de pyspark 2.4
  → CONFLITO — só uma versão pode estar instalada globalmente

Com venv (isolado):
  .venv-spark/     → pyspark 3.4, delta-spark 3.0
  .venv-legado/    → pyspark 2.4
  → ZERO CONFLITO — cada projeto tem o seu
```

### Criando e ativando o venv

```bash
# Criar venv na raiz do repositório
python -m venv .venv

# Ativar — Linux / Mac
source .venv/bin/activate

# Ativar — Windows (PowerShell)
.venv\Scripts\Activate.ps1

# Ativar — Windows (CMD)
.venv\Scripts\activate.bat

# Confirmar que está ativo — deve mostrar o path do .venv
which python          # Linux/Mac
where python          # Windows
# Saída esperada: /caminho/para/repo/.venv/bin/python

# Desativar quando terminar a sessão
deactivate
```

### .venv no .gitignore

O `.venv` nunca vai para o repositório — ele é pesado, específico de SO
e deve ser recriado a partir do `requirements.txt`:

```gitignore
# já deve estar no seu .gitignore
.venv/
venv/
env/
```

### Recriar o venv em outra máquina

```bash
# Clonar o repositório
git clone https://github.com/seu-usuario/spark-databricks-study.git
cd spark-databricks-study

# Criar e ativar venv
python -m venv .venv
source .venv/bin/activate

# Instalar tudo de uma vez
pip install -r requirements-dev.txt

# Pronto — ambiente idêntico ao original
```

---

## 2. Arquivo requirements.txt

### Dois arquivos, dois propósitos

```
requirements.txt         ← dependências de runtime (o que o código precisa)
requirements-dev.txt     ← dependências de desenvolvimento (linting, testes, etc.)
```

Separá-los evita instalar pytest e ruff em ambientes de produção.

### requirements.txt — dependências de runtime

```txt
# ─────────────────────────────────────────────────────────────
# requirements.txt
# Dependências de runtime do projeto
# Atualizar versões conforme necessário — fixar com ==
# ─────────────────────────────────────────────────────────────

# Spark e Delta (para rodar fora do Databricks, ex: testes locais)
pyspark==3.5.1
delta-spark==3.1.0

# Utilitários de dados
pandas==2.2.1
pyarrow==15.0.2          # necessário para Pandas UDF (Arrow)

# Leitura de configuração
python-dotenv==1.0.1     # carrega variáveis do .env

# JDBC drivers (se necessário para testes locais)
# jaydebeapi==1.2.3      # descomente se usar JDBC local
```

### requirements-dev.txt — dependências de desenvolvimento

```txt
# ─────────────────────────────────────────────────────────────
# requirements-dev.txt
# Dependências de desenvolvimento, linting e testes
# ─────────────────────────────────────────────────────────────

# Inclui tudo do runtime
-r requirements.txt

# Linting e formatação
ruff==0.4.4              # linter + isort ultra-rápido
black==24.4.2            # formatador de código
mypy==1.10.0             # type checking estático

# Testes
pytest==8.2.0
pytest-cov==5.0.0        # cobertura de testes
chispa==0.9.2            # testes unitários para PySpark

# Qualidade e automação
pre-commit==3.7.1        # hooks de pre-commit
ipykernel==6.29.4        # Jupyter kernel (para VS Code notebooks locais)

# Documentação (opcional)
mkdocs==1.6.0
mkdocs-material==9.5.20
```

### Instalar dependências

```bash
# Ativar venv primeiro
source .venv/bin/activate

# Instalar dependências de desenvolvimento (inclui runtime)
pip install -r requirements-dev.txt

# Atualizar requirements com versões instaladas atualmente
pip freeze > requirements-freeze.txt   # versões exatas de tudo
# OBS: não use pip freeze direto no requirements.txt —
# ele inclui sub-dependências e fica difícil de manter

# Atualizar uma dependência específica
pip install --upgrade pyspark==3.5.2
# Depois atualize manualmente o número no requirements.txt
```

### pyproject.toml — alternativa moderna ao requirements.txt

Projetos modernos usam `pyproject.toml` para centralizar tudo:
dependências, configs de linting, formatação e testes em um só arquivo.

```toml
# pyproject.toml — raiz do repositório

[project]
name = "spark-databricks-study"
version = "0.1.0"
description = "Caderno de estudo Spark, PySpark e Databricks"
requires-python = ">=3.10"
dependencies = [
    "pyspark>=3.5.0",
    "delta-spark>=3.1.0",
    "pandas>=2.2.0",
    "pyarrow>=15.0.0",
    "python-dotenv>=1.0.0",
]

[project.optional-dependencies]
dev = [
    "ruff>=0.4.0",
    "black>=24.4.0",
    "mypy>=1.10.0",
    "pytest>=8.2.0",
    "pytest-cov>=5.0.0",
    "chispa>=0.9.0",
    "pre-commit>=3.7.0",
]

# ─── Ruff (linter) ───────────────────────────────────────────
[tool.ruff]
line-length = 88
target-version = "py310"

[tool.ruff.lint]
select = [
    "E",   # pycodestyle errors
    "W",   # pycodestyle warnings
    "F",   # pyflakes
    "I",   # isort (ordenação de imports)
    "B",   # flake8-bugbear
    "C4",  # flake8-comprehensions
    "UP",  # pyupgrade
]
ignore = [
    "E501",  # line too long — black cuida disso
    "B008",  # function calls in default args
]

[tool.ruff.lint.per-file-ignores]
"**/templates/*.py" = ["F401"]  # imports não usados ok em templates

# ─── Black (formatador) ──────────────────────────────────────
[tool.black]
line-length = 88
target-version = ["py310"]
include = '\.pyi?$'
exclude = '''
/(
    \.venv
  | \.git
  | __pycache__
)/
'''

# ─── Mypy (type checking) ────────────────────────────────────
[tool.mypy]
python_version = "3.10"
warn_return_any = true
warn_unused_configs = true
ignore_missing_imports = true   # PySpark não tem stubs completos

# ─── Pytest ──────────────────────────────────────────────────
[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_functions = ["test_*"]
addopts = "--cov=. --cov-report=term-missing --cov-fail-under=70"
```

---

## 3. Variáveis de Ambiente com .env

### Analogia

O `.env` é como um cofre de chaves na sua casa. As chaves (senhas, tokens,
URLs) ficam no cofre — nunca penduradas na parede (no código).
O `python-dotenv` é quem abre o cofre e entrega as chaves para o programa.

### O arquivo .env

Crie na raiz do repositório:

```bash
# .env — NUNCA commitar este arquivo
# Copie de .env.example e preencha com seus valores reais

# ── Databricks ──────────────────────────────────────────────
DATABRICKS_HOST=https://adb-1234567890.12.azuredatabricks.net
DATABRICKS_TOKEN=dapi1234abcdef5678ghijkl

# ── Banco de dados (JDBC source) ────────────────────────────
JDBC_URL=jdbc:sqlserver://host:1433;database=mydb
JDBC_USER=meu_usuario
JDBC_PASSWORD=minha_senha_segura

# ── Cloud Storage ────────────────────────────────────────────
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=abc123...
STORAGE_ACCOUNT_NAME=meuazurestorage
STORAGE_ACCOUNT_KEY=chave_base64...

# ── Ambiente ─────────────────────────────────────────────────
ENVIRONMENT=development     # development | staging | production
LOG_LEVEL=INFO
```

### O arquivo .env.example

Este SIM vai para o repositório — é o template sem valores reais:

```bash
# .env.example — commitar este arquivo
# Copie para .env e preencha com seus valores reais
# NUNCA preencha com valores reais neste arquivo

DATABRICKS_HOST=https://seu-workspace.azuredatabricks.net
DATABRICKS_TOKEN=seu-token-aqui

JDBC_URL=jdbc:sqlserver://host:1433;database=db
JDBC_USER=usuario
JDBC_PASSWORD=senha

AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
STORAGE_ACCOUNT_NAME=
STORAGE_ACCOUNT_KEY=

ENVIRONMENT=development
LOG_LEVEL=INFO
```

### Usando python-dotenv no código

```python
# Em qualquer script Python local (fora do Databricks)
from dotenv import load_dotenv
import os

# Carrega variáveis do .env para os.environ
load_dotenv()

databricks_host  = os.environ.get("DATABRICKS_HOST")
databricks_token = os.environ.get("DATABRICKS_TOKEN")
jdbc_url         = os.environ.get("JDBC_URL")
jdbc_user        = os.environ.get("JDBC_USER")
jdbc_password    = os.environ.get("JDBC_PASSWORD")
environment      = os.environ.get("ENVIRONMENT", "development")
```

### Dentro do Databricks — usar Secrets, não .env

```python
# No Databricks, NUNCA use .env ou variáveis de ambiente diretas
# Use sempre dbutils.secrets

jdbc_user     = dbutils.secrets.get(scope="jdbc-scope",     key="user")
jdbc_password = dbutils.secrets.get(scope="jdbc-scope",     key="password")
jdbc_url      = dbutils.secrets.get(scope="jdbc-scope",     key="url")
storage_key   = dbutils.secrets.get(scope="storage-scope",  key="access-key")

# O valor não aparece em output de célula — aparece como [REDACTED]
print(jdbc_password)  # → [REDACTED]
```

### Configuração no .gitignore

```gitignore
# Variáveis de ambiente — NUNCA commitar
.env
.env.local
.env.production
.env.staging
.env.*
!.env.example    # exceção — o template vai para o repo
```

---

## 4. Ruff — Linter Ultrarrápido

### O que é um linter

Um linter é um revisor automático de código que aponta problemas antes
de você rodar: imports não usados, variáveis não definidas, comparações
com `==` onde deveria ser `is`, código que nunca executa, etc.

### Por que Ruff e não Flake8 ou Pylint

```
Flake8:   linter Python clássico — lento em projetos grandes
Pylint:   muito verboso, configuração complexa
Ruff:     escrito em Rust — 10-100x mais rápido que Flake8
          substitui Flake8 + isort + várias outras ferramentas
          mesmas regras, muito menos overhead
```

### Instalação e uso básico

```bash
pip install ruff

# Verificar o projeto inteiro
ruff check .

# Corrigir automaticamente o que for possível
ruff check . --fix

# Verificar arquivo específico
ruff check 02-pyspark-api/06_window_functions.py

# Verificar e mostrar código-fonte do problema
ruff check . --show-source
```

### Saída típica do ruff

```
02-pyspark-api/06_window_functions.py:3:1: F401 [*] `pyspark.sql.functions.lit`
  imported but unused
02-pyspark-api/06_window_functions.py:15:5: E711 Comparison to `None`
  (use `is None`)
03-spark-sql/07_merge_into.sql: — (arquivos .sql são ignorados pelo ruff)

Found 2 errors.
[*] 1 fixable with the `--fix` option.
```

### Regras mais úteis habilitadas

```
E/W  — PEP8: indentação, espaços, comprimento de linha
F    — PyFlakes: imports não usados, variáveis não definidas
I    — isort: ordenação de imports
B    — Bugbear: bugs comuns e anti-padrões
C4   — Comprehensions: list comprehension onde cabe
UP   — Pyupgrade: sintaxe moderna (f-strings, type hints)
```

### Ignorar uma linha específica

```python
# Quando você sabe que está certo e o linter está errado:
import pyspark  # noqa: F401       ← ignora F401 nesta linha
x = 1           # noqa              ← ignora tudo nesta linha

# Para arquivos gerados automaticamente, ignorar via config:
# [tool.ruff.lint.per-file-ignores]
# "templates/*.py" = ["F401", "E501"]
```

---

## 5. Black — Formatador de Código

### O que é um formatador

Um formatador não aponta problemas — ele corrige a formatação do código
automaticamente. Black é opinativo: há um único jeito de formatar e ele
não aceita discussão. Isso elimina debates de estilo no time.

### Por que Black

```
Sem formatador:
  dev A: f(x,y)         → sem espaço
  dev B: f( x, y )      → com espaço extra
  dev C: f(x,            → quebra de linha pessoal
             y)
  → diff de PR cheio de mudanças de formatação, não de lógica

Com Black:
  todos →  f(x, y)      → um único estilo, sempre
  → diff do PR mostra APENAS mudanças de lógica
```

### Instalação e uso básico

```bash
pip install black

# Formatar arquivo
black 02-pyspark-api/06_window_functions.py

# Formatar o projeto inteiro
black .

# Ver o que seria formatado SEM alterar (dry run)
black --check .

# Ver o diff das mudanças que seriam feitas
black --diff .
```

### O que Black muda

```python
# ANTES — código mal formatado
df=spark.read.format('delta').option('mergeSchema','true').option('versionAsOf',5).load('/mnt/silver/pedidos')
x={'a':1,'b':2,'c':3}
result=df.filter(col('status')=='ATIVO').withColumn('ano',year(col('data'))).groupBy('regiao').agg(sum('valor').alias('total'))

# DEPOIS — formatado pelo Black
df = (
    spark.read.format("delta")
    .option("mergeSchema", "true")
    .option("versionAsOf", 5)
    .load("/mnt/silver/pedidos")
)
x = {"a": 1, "b": 2, "c": 3}
result = (
    df.filter(col("status") == "ATIVO")
    .withColumn("ano", year(col("data")))
    .groupBy("regiao")
    .agg(sum("valor").alias("total"))
)
```

### Integração com VS Code

```json
// .vscode/settings.json
{
  "editor.formatOnSave": true,
  "[python]": {
    "editor.defaultFormatter": "ms-python.black-formatter"
  }
}
```

Com isso, o arquivo é formatado automaticamente toda vez que você salva.

---

## 6. Mypy — Type Checking Estático

### O que é type checking

Type checking verifica se você está usando os tipos corretos antes de
rodar o código. Pega erros como passar uma `str` onde espera um `int`
ou chamar um método que não existe no objeto.

```python
# Sem type hints — mypy não consegue ajudar muito
def processar(df, coluna):
    return df.filter(coluna == "ATIVO")

# Com type hints — mypy valida em tempo de desenvolvimento
from pyspark.sql import DataFrame
from pyspark.sql.column import Column

def processar(df: DataFrame, coluna: Column) -> DataFrame:
    return df.filter(coluna == "ATIVO")
```

### Uso básico

```bash
pip install mypy

# Verificar arquivo
mypy 02-pyspark-api/06_window_functions.py

# Verificar projeto inteiro
mypy .

# Ignorar erros de imports sem stubs (comum com PySpark)
mypy . --ignore-missing-imports
```

### Saída típica

```
09-padroes-producao/02_scd_type1_type2.py:45: error: Argument 1 to
  "filter" has incompatible type "str"; expected "Column"
09-padroes-producao/02_scd_type1_type2.py:67: error: Item "None" of
  "Optional[str]" has no attribute "split"
Found 2 errors in 1 file (checked 15 source files)
```

---

## 7. Pre-commit Hooks

### Analogia

Pre-commit hooks são como uma inspeção de qualidade na porta da fábrica:
antes de qualquer produto (commit) sair, ele passa por uma série de
verificações automáticas. Se algo estiver errado, o commit é bloqueado
e você conserta antes de sair.

### O que acontece sem hooks

```
Sem hooks:                          Com hooks:
git commit -m "feat: novo tópico"   git commit -m "feat: novo tópico"
→ commita com imports não usados    → ruff detecta imports não usados
→ commita com formatação quebrada   → black reformata automaticamente
→ commita com merge conflict        → check-merge-conflict bloqueia
→ descobre na review ou em prod     → descobre ANTES do commit
```

### Instalação e configuração

```bash
pip install pre-commit

# Instalar os hooks no repositório
pre-commit install

# Saída:
# pre-commit installed at .git/hooks/pre-commit

# Rodar manualmente em todos os arquivos (primeira vez)
pre-commit run --all-files
```

### Arquivo .pre-commit-config.yaml completo

```yaml
# .pre-commit-config.yaml — raiz do repositório
# Hooks executados automaticamente em cada git commit

repos:
  # ── Ruff — linter ultrarrápido ──────────────────────────────
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.4.4
    hooks:
      - id: ruff
        args: [--fix]           # corrige automaticamente o que puder
        types_or: [python, pyi]

  # ── Black — formatador ──────────────────────────────────────
  - repo: https://github.com/psf/black
    rev: 24.4.2
    hooks:
      - id: black
        language_version: python3.10

  # ── Checks gerais de qualidade ──────────────────────────────
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.6.0
    hooks:
      - id: trailing-whitespace        # remove espaços no fim da linha
      - id: end-of-file-fixer          # garante newline no fim do arquivo
      - id: check-yaml                 # valida sintaxe de arquivos YAML
      - id: check-json                 # valida sintaxe de arquivos JSON
      - id: check-toml                 # valida sintaxe de arquivos TOML
      - id: check-merge-conflict       # bloqueia se tiver marcador de conflito
      - id: check-added-large-files    # bloqueia arquivos > 500kb
        args: [--maxkb=500]
      - id: detect-private-key         # bloqueia se detectar chave privada
      - id: check-case-conflict        # evita conflito de case em filenames
      - id: mixed-line-ending          # padroniza LF vs CRLF
        args: [--fix=lf]

  # ── Segurança — detecta secrets no código ───────────────────
  - repo: https://github.com/Yelp/detect-secrets
    rev: v1.5.0
    hooks:
      - id: detect-secrets
        args: [--baseline, .secrets.baseline]
        # Crie o baseline com: detect-secrets scan > .secrets.baseline
```

### O que acontece em cada commit

```bash
git add 02-pyspark-api/06_window_functions.py
git commit -m "feat(02-pyspark): window functions completo"

# Pre-commit executa automaticamente:
ruff......................................................Passed    ✅
black.....................................................Passed    ✅
trailing whitespace.......................................Passed    ✅
end of file fixer.........................................Passed    ✅
check yaml................................................Passed    ✅
check for merge conflicts.................................Passed    ✅
detect private key........................................Passed    ✅
detect-secrets............................................Passed    ✅

# Se algo falhar:
ruff......................................................Failed    ❌
- hook id: ruff
- exit code: 1
- files were modified by this hook

02-pyspark-api/06_window_functions.py:3:1: F401 [*] `pyspark.sql.functions.lit`
  imported but unused

# O commit é bloqueado. Você corrige e tenta novamente.
```

### Pular hooks em casos específicos (use com moderação)

```bash
# Pular todos os hooks (emergência)
git commit -m "fix: correção urgente" --no-verify

# Pular hook específico
SKIP=ruff git commit -m "wip: trabalho em progresso"
```

### Atualizar hooks para versões mais recentes

```bash
# Atualizar todos os hooks para a versão mais recente
pre-commit autoupdate

# Limpar cache de hooks (se algo estiver quebrado)
pre-commit clean
```

---

## 8. Estrutura final do ambiente

Depois de configurar tudo, a raiz do repositório fica assim:

```
spark-databricks-study/
│
├── .env                        ← NÃO commitado — seus secrets reais
├── .env.example                ← commitado — template sem valores
├── .gitignore                  ← commitado
├── .pre-commit-config.yaml     ← commitado
├── pyproject.toml              ← commitado — configs centralizadas
├── requirements.txt            ← commitado — runtime
├── requirements-dev.txt        ← commitado — desenvolvimento
│
├── .venv/                      ← NÃO commitado — ambiente virtual
├── .databricks/                ← NÃO commitado — config da extensão
├── .secrets.baseline           ← commitado — baseline do detect-secrets
│
├── .github/
│   ├── workflows/
│   │   └── ci.yml              ← commitado — CI automático
│   └── pull_request_template.md ← commitado
│
└── [módulos de estudo]/
```

---

## 9. Checklist de setup do ambiente

```
PRIMEIRA VEZ:

[ ] python --version          → confirmar 3.10+
[ ] python -m venv .venv      → criar venv
[ ] source .venv/bin/activate → ativar
[ ] pip install -r requirements-dev.txt → instalar tudo
[ ] pre-commit install        → instalar hooks
[ ] pre-commit run --all-files → rodar nos arquivos existentes
[ ] cp .env.example .env      → criar .env local
[ ] preencher .env com valores reais
[ ] python -c "from dotenv import load_dotenv; load_dotenv(); print('OK')"

VERIFICAR SE ESTÁ TUDO FUNCIONANDO:

[ ] ruff check .              → zero erros
[ ] black --check .           → zero erros de formatação
[ ] mypy . --ignore-missing-imports → zero erros de tipo
[ ] pre-commit run --all-files → todos os hooks passam
[ ] pytest                    → testes passam (se existirem)


TODA VEZ QUE ABRIR O PROJETO:

[ ] source .venv/bin/activate (ou .venv\Scripts\activate no Windows)
[ ] confirmar venv ativo: which python deve apontar para .venv
```

---

## 10. Referência rápida de comandos

```bash
# ── VENV ────────────────────────────────────────────────────
python -m venv .venv                          # criar
source .venv/bin/activate                     # ativar (Linux/Mac)
.venv\Scripts\activate                        # ativar (Windows)
deactivate                                    # desativar
pip install -r requirements-dev.txt           # instalar deps

# ── RUFF ────────────────────────────────────────────────────
ruff check .                                  # verificar
ruff check . --fix                            # corrigir automaticamente
ruff check . --show-source                    # mostrar contexto do erro

# ── BLACK ────────────────────────────────────────────────────
black .                                       # formatar tudo
black --check .                               # verificar sem alterar
black --diff .                                # ver mudanças

# ── MYPY ─────────────────────────────────────────────────────
mypy . --ignore-missing-imports               # type check

# ── PRE-COMMIT ───────────────────────────────────────────────
pre-commit install                            # instalar hooks
pre-commit run --all-files                    # rodar em tudo
pre-commit run ruff                           # rodar hook específico
pre-commit autoupdate                         # atualizar versões
pre-commit clean                              # limpar cache

# ── PYTEST ───────────────────────────────────────────────────
pytest                                        # rodar todos os testes
pytest -v                                     # verbose
pytest tests/test_scd.py                      # arquivo específico
pytest -k "test_merge"                        # testes por nome
pytest --cov=. --cov-report=html              # cobertura em HTML
```

---

*Referências: [Ruff docs](https://docs.astral.sh/ruff) ·
[Black docs](https://black.readthedocs.io) ·
[pre-commit docs](https://pre-commit.com) ·
[python-dotenv](https://pypi.org/project/python-dotenv)*
