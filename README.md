# Databricks Sales Pipeline

Projeto de engenharia de dados utilizando Databricks, Apache Spark e Delta Lake.

O objetivo é construir um pipeline completo de vendas seguindo a arquitetura Medallion:

- Bronze: ingestão dos dados brutos, validação inicial e quarentena
- Silver: tratamento, padronização e enriquecimento dos dados
- Gold: modelo dimensional para análise


## Arquitetura

CSV Files
    |
    |
Auto Loader
    |
    |
Bronze Delta Tables
    |
    |
Data Quality Rules
    |
    |
Silver Delta Tables
    |
    |
Gold Layer
    |
    |
Analytics


## Tecnologias

- Databricks
- Apache Spark
- PySpark
- Delta Lake
- Unity Catalog
- SQL
- Python


## Dataset

O projeto utiliza uma base simulada de vendas contendo:

- vendas
- produtos
- lojas
- clientes


## Implementações

### Bronze

Características:

- Streaming ingestion utilizando Databricks Auto Loader
- Processamento incremental
- Controle de checkpoint
- Armazenamento em Delta Tables
- Tratamento de dados inválidos através de camada de quarentena


### Data Quality e Quarantine Pattern

A camada Bronze possui dois tipos de validação:


#### Quarentena Técnica

Responsável por capturar problemas relacionados à estrutura dos dados.

Exemplos:

- Alterações inesperadas de schema
- Campos não reconhecidos
- Dados capturados no campo `_rescued_data`


#### Quarentena de Negócio

Responsável por capturar registros que possuem estrutura válida, porém violam regras do domínio.

Exemplos:

- Produto inexistente
- Loja inválida
- Cliente inexistente
- Valores negativos
- Campos obrigatórios vazios


Os registros inconsistentes são isolados sem interromper o pipeline, permitindo rastreabilidade e posterior análise.


### Silver

Implementações:

- Tratamento de tipos
- Padronização dos dados
- Deduplicação
- Aplicação de regras de negócio
- Enriquecimento dos dados
- Otimização utilizando comando `OPTIMIZE`

#### Performance

As tabelas da camada Silver são periodicamente otimizadas utilizando o comando `OPTIMIZE`, reduzindo a quantidade de arquivos pequenos e melhorando a performance de leitura para as camadas analíticas.


### Gold

Modelo dimensional:

Dimensões:

- dim_produto
- dim_loja
- dim_cliente
- dim_status_venda

Fato:

- fato_venda

#### Estratégia de Consumo

A camada Gold foi implementada utilizando Materialized Views, permitindo:

- Atualização incremental dos dados
- Redução do custo computacional das consultas
- Melhor desempenho para análises e dashboards
- Processamento apenas das alterações identificadas nas camadas inferiores

As Materialized Views são atualizadas automaticamente conforme novos dados são disponibilizados nas camadas Bronze e Silver.

## Data Quality

Foram implementadas validações:

| Regra | Ação |
|-|-|
| Valor negativo | Quarentena |
| Campo obrigatório vazio | Quarentena |
| Fora de estrutura recuperados pelo campo `_rescued_data` | Quarentena |


## Como executar

1. Importar notebooks no Databricks
2. Importar os arquivos de dados
3. Configurar catálogo e schema no Unity Catalog
4. Executar pipeline

## Principais conceitos aplicados

- Medallion Architecture
- Delta Lake
- Incremental Data Processing
- Data Quality Framework
- Quarantine Pattern
- Dimensional Modeling
- OPTIMIZE nas tabelas Silver
- Materialized Views


## Autor

Felipe Porfirio

Senior Data Engineer