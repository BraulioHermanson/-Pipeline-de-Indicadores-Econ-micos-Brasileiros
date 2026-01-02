# 🌾 Pipeline de Indicadores Econômicos Brasileiros

Pipeline de dados para extração, armazenamento e análise de indicadores econômicos e agrícolas brasileiros, utilizando arquitetura Lakehouse com Apache Iceberg.
<img width="2816" height="1536" alt="resumo-readmeimagem" src="https://github.com/user-attachments/assets/14305023-e28b-4130-96c7-074e0f1e2117" />

## 📋 Índice

- [Visão Geral](#visão-geral)
- [Arquitetura](#arquitetura)
- [1. Extração de Dados - APIs Python](#1-extração-de-dados---apis-python)
  - [1.1 CEPEA - Indicadores Agrícolas](#11-cepea---indicadores-agrícolas)
  - [1.2 BCB - Banco Central do Brasil](#12-bcb---banco-central-do-brasil)
  - [1.3 IPEA - Instituto de Pesquisa Econômica](#13-ipea---instituto-de-pesquisa-econômica)
  - [1.4 CONAB - Hortifruti (Prohort)](#14-conab---hortifruti-prohort)
- [2. Lakehouse com Apache Iceberg](#2-lakehouse-com-apache-iceberg)
- [3. Orquestração com Airflow](#3-orquestração-com-airflow)
- [4. Visualização com DBeaver](#4-visualização-com-dbeaver)
- [Aprendizados e Lições](#aprendizados-e-lições)
- [Como Executar](#como-executar)

---

## Visão Geral

Este projeto implementa um pipeline completo de dados para coletar indicadores econômicos de diversas fontes brasileiras e armazená-los em um Lakehouse moderno.

**Fontes de Dados:**
| Fonte | Tipo | Dados |
|-------|------|-------|
| CEPEA/ESALQ | Web Scraping | Preços agrícolas (boi, soja, milho, café, etc.) |
| BCB | API REST | Indicadores econômicos (dólar, SELIC, IPCA) |
| IPEA | API REST | Séries históricas econômicas |
| CONAB | Download Direto | Preços de hortifruti das CEASAs |

**Stack Tecnológico:**
- **Extração:** Python (requests, pandas)
- **Armazenamento:** Apache Iceberg + MinIO (S3)
- **Processamento:** Apache Spark
- **Orquestração:** Apache Airflow
- **Visualização:** DBeaver

---

## Arquitetura

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           FONTES DE DADOS                                │
├─────────────┬─────────────┬─────────────┬─────────────────────────────────┤
│   CEPEA     │    BCB      │    IPEA     │           CONAB                │
│ (scraping)  │  (API)      │   (API)     │      (download TXT)            │
└──────┬──────┴──────┬──────┴──────┬──────┴────────────┬────────────────────┘
       │             │             │                   │
       ▼             ▼             ▼                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        EXTRACTORS (Python)                              │
│  cepea_scraper.py │ bcb_client.py │ ipea_client.py │ conab_client.py   │
└─────────────────────────────────────────────────────────────────────────┘
       │             │             │                   │
       ▼             ▼             ▼                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         PARQUET FILES                                    │
│                        /opt/spark/data/                                  │
└─────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    SPARK + ICEBERG (Bronze Layer)                        │
│                     load_bronze_iceberg.py                               │
│                        (MERGE/Upsert)                                    │
└─────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           MinIO (S3)                                     │
│                    s3://warehouse/bronze/                                │
│  ┌─────────────┬─────────────┬─────────────┬─────────────┐              │
│  │   cepea_    │    bcb_     │   ipea_     │   conab_    │              │
│  │ indicadores │ indicadores │ indicadores │ indicadores │              │
│  └─────────────┴─────────────┴─────────────┴─────────────┘              │
└─────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     CONSUMO / VISUALIZAÇÃO                               │
│              DBeaver (SQL) │ Power BI │ Jupyter                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 1. Extração de Dados - APIs Python

### 1.1 CEPEA - Indicadores Agrícolas

**Fonte:** Centro de Estudos Avançados em Economia Aplicada (ESALQ/USP)

**Método:** Web Scraping

**URL Base:** `https://cepea.esalq.usp.br/br/indicador/`

**Indicadores disponíveis:**
- Boi Gordo, Bezerro
- Soja, Milho, Trigo
- Café Arábica, Café Robusta
- Açúcar, Etanol
- Algodão, Arroz, Feijão
- Frango, Suíno
- Leite, Ovos
- Mandioca

**Estrutura do código:**
```python
class CepeaScraper:
    def __init__(self):
        self.base_url = "https://cepea.esalq.usp.br/br/indicador/"
        self.indicators = {...}  # Mapeamento de indicadores
    
    def extract_indicator(self, indicator: str) -> pd.DataFrame:
        # Faz request HTTP e parseia HTML
        # Retorna DataFrame com: date, value_brl, value_usd, var_day, var_month
```

**Aprendizados CEPEA:**
- ⚠️ Site não tem API oficial - depende de scraping
- ⚠️ Estrutura HTML pode mudar sem aviso
- ⚠️ Múltiplas tabelas por indicador (Tabela 1, 2, 3) - importante incluir `indicator_name` na chave do MERGE
- ✅ Dados diários atualizados
- ✅ Preços em BRL e USD

---

### 1.2 BCB - Banco Central do Brasil

**Fonte:** Sistema Gerenciador de Séries Temporais (SGS)

**Método:** API REST oficial

**URL Base:** `https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados`

**Séries utilizadas:**
| Código | Descrição |
|--------|-----------|
| 1 | Dólar comercial (venda) |
| 433 | IPCA - Variação mensal |
| 4390 | Taxa SELIC |
| 7326 | PIB mensal |
| 24363 | Dívida líquida do setor público |

**Estrutura do código:**
```python
class BCBClient:
    BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados"
    
    def get_series(self, series_code: int, start_date: str = None) -> pd.DataFrame:
        url = self.BASE_URL.format(series_code)
        params = {"formato": "json"}
        if start_date:
            params["dataInicial"] = start_date
        
        response = requests.get(url, params=params)
        return pd.DataFrame(response.json())
```

**Aprendizados BCB:**
- ✅ API oficial, estável e bem documentada
- ✅ Formato JSON limpo
- ✅ Não requer autenticação
- ✅ Permite filtro por data inicial/final
- ⚠️ Rate limiting em requisições muito frequentes
- ⚠️ Algumas séries têm frequência diferente (diária, mensal, anual)

**Documentação:** https://dadosabertos.bcb.gov.br/

---

### 1.3 IPEA - Instituto de Pesquisa Econômica

**Fonte:** IPEA Data

**Método:** API REST

**URL Base:** `http://www.ipeadata.gov.br/api/odata4/`

**Séries utilizadas:**
| Código | Descrição |
|--------|-----------|
| GM366_ERC366 | Taxa de câmbio comercial |
| PRECOS366_IGPDI366 | IGP-DI |
| BM366_PIB366 | PIB nominal |

**Estrutura do código:**
```python
class IPEAClient:
    BASE_URL = "http://www.ipeadata.gov.br/api/odata4/"
    
    def get_series(self, series_code: str) -> pd.DataFrame:
        # Primeiro busca metadados da série
        metadata_url = f"{self.BASE_URL}Metadados('{series_code}')"
        
        # Depois busca os valores
        values_url = f"{self.BASE_URL}Metadados('{series_code}')/Valores"
        
        response = requests.get(values_url)
        data = response.json()
        return pd.DataFrame(data['value'])
```

**Aprendizados IPEA:**
- ✅ API OData4 bem estruturada
- ✅ Metadados ricos (descrição, fonte, unidade)
- ⚠️ Códigos de séries não são intuitivos
- ⚠️ Algumas séries descontinuadas
- ⚠️ Performance pode variar

**Documentação:** http://www.ipeadata.gov.br/api/

---

### 1.4 CONAB - Hortifruti (Prohort)

**Fonte:** Companhia Nacional de Abastecimento - Programa Prohort

**Método:** Download direto de arquivo TXT

**URL:** `https://portaldeinformacoes.conab.gov.br/downloads/arquivos/ProhortDiario.txt`

**Dados disponíveis:**
- Preços diários de hortifruti
- 48 produtos (frutas e hortaliças)
- 43 CEASAs de todo Brasil
- Histórico desde 2022

**Estrutura do arquivo:**
```
municipio_ceasa;cod_ibge_municipio;uf_ceasa;dsc_ceasa;dsc_produto;sig_unidade_medida;data_preco;preco_diario
ARAÇATUBA-SP;3502804;SP;CEAGESP - ARACATUBA;ABACATE;KG;2022/07/08 00:00:00.000;5.8
```

**Estrutura do código:**
```python
class ConabProhortClient:
    ENDPOINTS = {
        "diario": "https://portaldeinformacoes.conab.gov.br/downloads/arquivos/ProhortDiario.txt",
        "mensal": "https://portaldeinformacoes.conab.gov.br/downloads/arquivos/ProhortMensal.txt"
    }
    
    def extract_diario(self, produtos=None, estados=None) -> pd.DataFrame:
        # Download do arquivo completo
        # Leitura com encoding latin-1
        # Limpeza e padronização
        # Filtros opcionais
```

**Aprendizados CONAB:**
- ✅ Dados oficiais do governo
- ✅ Download simples (HTTP GET)
- ✅ ~900.000 registros disponíveis
- ✅ Funciona em qualquer ambiente (não precisa Selenium!)
- ⚠️ Arquivo grande (~150MB) - demora para baixar
- ⚠️ Encoding `latin-1` (não UTF-8)
- ⚠️ Dados podem ter duplicatas - necessário deduplicar
- ⚠️ Timestamps em formato `NANOS` causam erro no Spark - converter para `microseconds`

**Importante - Por que não usamos Selenium:**

Inicialmente consideramos usar Selenium para extrair dados do site HF Brasil (CEPEA Hortifruti), mas identificamos problemas:

| Aspecto | Selenium | CONAB (Download) |
|---------|----------|------------------|
| Complexidade | Alta (Chrome, WebDriver) | Baixa (HTTP GET) |
| Manutenção | Alta (site pode mudar) | Baixa |
| Performance | Lenta | Rápida |
| EMR/Produção | Difícil | Fácil |
| Confiabilidade | Baixa | Alta |

A CONAB oferece os **mesmos dados** em formato muito mais acessível.

---

## 2. Lakehouse com Apache Iceberg

### Por que migrar para Iceberg?

| Problema com Parquet puro | Solução com Iceberg |
|---------------------------|---------------------|
| Sem controle de transações | ACID transactions |
| Difícil fazer UPDATE/DELETE | MERGE (upsert) nativo |
| Sem versionamento | Time travel |
| Schema rígido | Schema evolution |
| Difícil particionar | Particionamento transparente |

### Arquitetura Docker

```yaml
services:
  minio:        # Storage S3-compatible
  iceberg-rest: # Catálogo Iceberg
  spark-master: # Processamento
  spark-worker: # Workers
  spark-thrift: # SQL interface (JDBC)
```

### Estrutura das Tabelas Bronze

**iceberg.bronze.cepea_indicadores:**
```sql
CREATE TABLE iceberg.bronze.cepea_indicadores (
    date DATE,
    value_brl DOUBLE,
    var_day_pct DOUBLE,
    var_month_pct DOUBLE,
    value_usd DOUBLE,
    region STRING,
    indicator STRING,
    indicator_name STRING,  -- Importante para diferenciar tabelas!
    unit STRING,
    source STRING,
    extracted_at TIMESTAMP,
    _loaded_at TIMESTAMP
)
```

**iceberg.bronze.conab_indicadores:**
```sql
CREATE TABLE iceberg.bronze.conab_indicadores (
    date DATE,
    product STRING,
    price DOUBLE,
    unit STRING,
    ceasa_name STRING,
    municipality STRING,
    state STRING,
    ibge_code STRING,
    source STRING,
    data_type STRING,
    extracted_at TIMESTAMP,
    _loaded_at TIMESTAMP
)
```

### Processo de Carga (MERGE)

O script `load_bronze_iceberg.py` implementa:

1. **Validação de Schema:** Verifica se o Parquet é compatível com Spark
2. **Detecção de Duplicatas:** Identifica chaves duplicadas antes do MERGE
3. **Deduplicação Automática:** Remove duplicatas se encontradas
4. **MERGE (Upsert):** Atualiza existentes, insere novos

```python
# Configuração das chaves de MERGE
tables = [
    {
        "file": "cepea_indicadores.parquet",
        "table": "cepea_indicadores",
        "merge_keys": ["date", "indicator", "indicator_name"]  # Chave composta!
    },
    {
        "file": "conab_indicadores.parquet",
        "table": "conab_indicadores",
        "merge_keys": ["date", "product", "ceasa_name"]
    },
    # ...
]
```

### Erros Comuns e Soluções

| Erro | Causa | Solução |
|------|-------|---------|
| `MERGE_CARDINALITY_VIOLATION` | Duplicatas na chave | Adicionar mais colunas à chave ou deduplicar |
| `Illegal Parquet type: TIMESTAMP(NANOS)` | Pandas salva timestamp em nanos | Usar `coerce_timestamps='us'` no to_parquet() |
| `SCHEMA_NOT_FOUND: default` | Thrift não encontra namespace | Criar `iceberg.default` ou conectar sem database |

---

## 3. Orquestração com Airflow

### Estrutura da DAG

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│check_containers │ --> │   extractions   │ --> │    validate     │
│                 │     │ (extract_conab) │     │   extractions   │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                        │
                                                        ▼
                        ┌─────────────────┐     ┌─────────────────┐
                        │   verify_load   │ <-- │  load_bronze    │
                        │                 │     │    iceberg      │
                        └─────────────────┘     └─────────────────┘
```

### Tasks

| Task | Descrição | Comando |
|------|-----------|---------|
| `check_containers` | Verifica se Docker está rodando | `docker ps \| grep spark-master` |
| `extract_conab` | Extrai dados da CONAB | `docker exec spark-master python conab_prohort_client.py` |
| `validate_extractions` | Valida arquivos gerados | `ls -lh *.parquet` |
| `load_bronze_iceberg` | Carrega no Iceberg com MERGE | `spark-submit load_bronze_iceberg.py` |
| `verify_load` | Conta registros finais | `spark-sql -e "SELECT COUNT(*)..."` |

### Configuração

```python
with DAG(
    dag_id='pipeline_indicadores_economicos',
    schedule_interval='0 6 * * *',  # Diariamente às 6h
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['iceberg', 'bronze', 'indicadores'],
) as dag:
```

### Aprendizados Airflow

- ⚠️ **Jinja Templates:** Evitar `{{ }}` em comandos bash (conflita com Jinja do Airflow)
- ⚠️ **Docker Socket:** Precisa de permissão (`chmod 666 /var/run/docker.sock`)
- ⚠️ **Volumes:** Montar `/var/run/docker.sock` e pastas do projeto no container Airflow
- ✅ **BashOperator:** Simples e eficiente para executar comandos Docker
- ✅ **TaskGroup:** Organiza tasks relacionadas visualmente

---

## 4. Visualização com DBeaver

### Conexão

| Campo | Valor |
|-------|-------|
| Tipo | Apache Spark / Apache Hive |
| Host | localhost |
| Port | 10000 |
| Database | *(deixar vazio)* |
| Authentication | No Authentication |

### Queries Úteis

```sql
-- Listar tabelas
SHOW TABLES IN iceberg.bronze;

-- Contagem por tabela
SELECT 'conab' as fonte, COUNT(*) as registros FROM iceberg.bronze.conab_indicadores
UNION ALL
SELECT 'bcb', COUNT(*) FROM iceberg.bronze.bcb_indicadores
UNION ALL
SELECT 'ipea', COUNT(*) FROM iceberg.bronze.ipea_indicadores
UNION ALL
SELECT 'cepea', COUNT(*) FROM iceberg.bronze.cepea_indicadores;

-- Top produtos CONAB
SELECT product, COUNT(*) as registros, ROUND(AVG(price), 2) as preco_medio
FROM iceberg.bronze.conab_indicadores
GROUP BY product
ORDER BY registros DESC
LIMIT 20;

-- Evolução do preço da banana
SELECT date, AVG(price) as preco_medio
FROM iceberg.bronze.conab_indicadores
WHERE product = 'BANANA'
GROUP BY date
ORDER BY date;
```

---

## Aprendizados e Lições

### Sobre APIs de Dados Brasileiras

1. **Prefira APIs oficiais** quando disponíveis (BCB, IPEA)
2. **Evite Selenium** - sempre procure alternativas (CONAB vs HF Brasil)
3. **Encoding:** Dados brasileiros frequentemente usam `latin-1`, não `UTF-8`
4. **Documentação:** APIs governamentais nem sempre são bem documentadas

### Sobre Apache Iceberg

1. **MERGE keys:** Escolha cuidadosamente - erros de cardinalidade são comuns
2. **Timestamps:** Pandas usa nanosegundos, Spark espera microsegundos
3. **Schema evolution:** Iceberg facilita, mas planejar schema inicial bem ajuda

### Sobre Arquitetura

1. **Separar responsabilidades:** Airflow orquestra, Spark processa, Iceberg armazena
2. **Validar antes de carregar:** Evita erros em produção
3. **Logs detalhados:** Essenciais para debug

---

## Como Executar

### Pré-requisitos

- Docker e Docker Compose
- WSL2 (Windows) ou Linux
- ~8GB RAM disponível

### 1. Subir o Lakehouse

```bash
cd ~/Jornada/iceberg-dbt-project
docker compose up -d
docker ps  # Verificar containers
```

### 2. Executar extração manual

```bash
# CONAB
docker exec spark-master python /opt/spark/scripts/conab_prohort_client.py \
    --output /opt/spark/data/conab_indicadores.parquet

# Carregar no Iceberg
docker exec spark-master spark-submit /opt/spark/scripts/load_bronze_iceberg.py
```

### 3. Subir o Airflow

```bash
cd ~/Jornada/airflow_b
docker compose up -d

# Verificar DAG
docker compose exec airflow-webserver airflow dags list | grep indicadores
```

### 4. Acessar interfaces

| Serviço | URL | Credenciais |
|---------|-----|-------------|
| Airflow | - | - |
| MinIO | - | - |
| Spark UI | - | - |

### 5. Conectar DBeaver

- Host: `localhost`
- Port: `10000`
- Tipo: Apache Spark

---

## Estrutura de Arquivos

```
~/Jornada/
├── iceberg-dbt-project/
│   ├── docker-compose.yml      # Stack Iceberg + Spark + MinIO
│   ├── scripts/
│   │   ├── conab_prohort_client.py   # Extrator CONAB
│   │   └── load_bronze_iceberg.py    # Loader com MERGE
│   ├── data/                   # Arquivos Parquet
│   └── spark/
│       └── Dockerfile          # Imagem Spark customizada
│
└── airflow_b/
    ├── docker-compose.yaml     # Stack Airflow
    └── dags/
        └── pipeline_indicadores_dag.py  # DAG de orquestração
```

---

## Próximos Passos

- [ ] Adicionar camada Silver (transformações dbt)
- [ ] Adicionar camada Gold (agregações para BI)
- [ ] Conectar Power BI via ODBC
- [ ] Implementar alertas de falha no Airflow
- [ ] Adicionar mais fontes de dados (B3, CVM)

---

## Autor

**Braulio Campos**  
Senior Data Engineer

---

*Última atualização: Janeiro/2026*
