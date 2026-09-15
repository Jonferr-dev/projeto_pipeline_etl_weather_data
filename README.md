🌦️ Projeto Pipeline Weather


Pipeline de ETL (Extract, Transform, Load) para dados climáticos de São Paulo, orquestrado com **Apache Airflow** e totalmente containerizado com **Docker Compose**. Os dados são extraídos da API do OpenWeatherMap, transformados com Pandas e carregados simultaneamente em um banco de dados relacional na nuvem (**AWS RDS**) e em um bucket de armazenamento de objetos (**AWS S3**).


---


## 📌 Visão Geral


O pipeline está **apto para execução automatizada periódica** (ex: `schedule='0 * * * *'`), mas mantido atualmente com acionamento manual por se tratar de um projeto de teste/demonstração. Ele realiza:


1. **Extract** — busca dados climáticos atuais de São Paulo via API do OpenWeatherMap

2. **Transform** — normaliza o JSON aninhado da API em um DataFrame tabular, renomeia colunas, remove campos desnecessários e trata campos condicionais (chuva/neve)

3. **Load** — persiste os dados transformados em dois destinos, em paralelo:

- **AWS RDS (PostgreSQL)** — para consultas analíticas estruturadas

- **AWS S3** — para armazenamento bruto em formato `.parquet`


---


## 🏗️ Arquitetura


```

extract() → transform() → ┬→ load_rds() (AWS RDS - PostgreSQL)

└→ load_s3() (AWS S3)

```


- **Orquestração:** Apache Airflow 3.1.7 (scheduler, dag-processor, apiserver, worker, triggerer)

- **Metastore do Airflow:** PostgreSQL 16 (container local)

- **Broker/Cache:** Redis 7.2

- **Armazenamento de dados do projeto:** AWS RDS (PostgreSQL) + AWS S3

- **Ambiente de desenvolvimento:** Ubuntu (VM via VirtualBox) + VS Code + Docker Compose


---


## 🛠️ Stack Técnica


| Camada | Tecnologia |

|---|---|

| Orquestração | Apache Airflow (TaskFlow API — `@dag` / `@task`) |

| Linguagem | Python 3.14 |

| Manipulação de dados | Pandas |

| Requisições HTTP | Requests |

| Banco de dados analítico | AWS RDS (PostgreSQL) via `psycopg2` / SQLAlchemy |

| Armazenamento de objetos | AWS S3 via `boto3` |

| Gerenciamento de dependências | `uv` |

| Containerização | Docker / Docker Compose |

| Variáveis de ambiente | `python-dotenv` |


---


## 📂 Estrutura do Projeto


```

projeto_pipeline_weather/

├── config/

│ └── .env # Credenciais (API_KEY, AWS, RDS) — não versionado

├── dags/

│ └── weather_dag.py # Definição da DAG e orquestração das tasks

├── src/

│ ├── extract_data.py # Extração via API OpenWeatherMap

│ ├── transform_data.py # Normalização e limpeza dos dados

│ └── load_data.py # Carga para RDS e S3

├── data/ # Armazenamento temporário local (parquet)

├── notebooks/ # Notebooks de exploração/análise

├── docker-compose.yaml

└── pyproject.toml

```


---


## ⚙️ Configuração e Execução


### Pré-requisitos

- Docker e Docker Compose instalados

- Conta AWS com uma instância RDS (PostgreSQL) e um bucket S3 criados

- Chave de API do [OpenWeatherMap](https://openweathermap.org/api)


### Variáveis de ambiente (`config/.env`)

```

API_KEY=<sua_chave_openweathermap>

AWS_ACCESS_KEY_ID=<sua_access_key>

AWS_SECRET_ACCESS_KEY=<sua_secret_key>

RDS_HOST=<endpoint_do_seu_rds>

RDS_PORT=5432

RDS_DB=postgres

RDS_USER=postgres

RDS_PASSWORD=<sua_senha>

S3_BUCKET=<nome_do_bucket>

```


### Subindo o ambiente

```bash

docker compose up -d

```


Acesse a interface do Airflow em `http://localhost:8080` e ative a DAG `projeto_pipeline_weather`.


---


## ☁️ Configuração AWS (S3 e RDS)


Esta segunda versão do projeto migrou o armazenamento de local para a nuvem:


- **RDS (PostgreSQL):** instância criada na região `us-east-2`, com **acesso público habilitado** e regra de entrada no Security Group liberando a porta `5432` para o IP de desenvolvimento. Os dados climáticos são inseridos na tabela `sp_weather`.

- **S3:** os mesmos dados (em `.parquet`) são enviados como backup bruto/histórico, útil para reprocessamento futuro sem depender de nova consulta à API.

- As credenciais AWS e do banco são carregadas via variáveis de ambiente (`.env`), nunca hardcoded no código-fonte — passo importante de segurança implementado nesta atualização.


---


## 🧩 Desafios Enfrentados e Soluções


Durante a migração para S3/RDS e os ajustes gerais do pipeline, alguns problemas foram identificados e corrigidos:


- **VS Code fechando ao rodar notebooks dentro da VM** — causado por renderização gráfica (GPU virtual) do Electron dentro do VirtualBox, não por erro de código.

- **Disco cheio ao subir o Airflow (`no space left on device`)** — resolvido com `docker system prune -a --volumes`, liberando imagens/containers não utilizados.

- **DAG sumindo da listagem do Airflow** — não era erro de import; a DAG estava sendo carregada normalmente, apenas "escondida" entre as dezenas de DAGs de exemplo do Airflow (resolvido filtrando por nome ou desativando `AIRFLOW__CORE__LOAD_EXAMPLES`).

- **Falha de conexão com o RDS (`OperationalError`)** — a instância estava com **"Publicly accessible" = No**. Corrigido habilitando acesso público e ajustando o Security Group para liberar a porta 5432.

- **Erro de coluna inexistente (`rain.1h`, `snow.1h`, etc.)** — campos aninhados da API que só aparecem em dias de chuva/neve quebravam o insert no Postgres. Resolvido padronizando os nomes de colunas e tratando a remoção de colunas condicionais com `errors='ignore'`.

- **Dependências de tasks duplicadas na DAG** — a task `transform()` estava sendo instanciada duas vezes, gerando conflito de `task_id`. Corrigido instanciando cada task uma única vez e definindo a carga em paralelo para RDS e S3:

```python

extract_task >> transform_task >> [load_rds_task, load_s3_task]

```


---


## 👤 Autor


**Jonathan Ferreira Ribeiro**

GitHub: [@Jonferr-dev](https://github.com/Jonferr-dev)

LinkedIn: [jonferr-dev](https://www.linkedin.com/in/jonferr-dev) 
