# Sales_pipe

## Visao Geral

`Sales_pipe` e um projeto de Analytics Engineering voltado para transformar um datalake relacional em uma camada analitica pronta para banco de dados e BI.

O projeto usa as tabelas do dataset Olist como base e organiza o fluxo em tres camadas:

- `bronze`: dados brutos da origem
- `silver`: dados tratados e padronizados
- `gold`: tabelas analiticas para consumo no BI

A ideia central e sair da exploracao em notebook e evoluir para uma pipeline simples, relacional e reproduzivel, com persistencia em MySQL e consumo final no Metabase.

## Objetivo

O objetivo da `Sales_pipe` e consolidar um fluxo em que os dados:

1. entram por arquivos CSV relacionados entre si
2. sao organizados em tabelas consistentes
3. passam por tratamento e enriquecimento
4. sao modelados para analise
5. podem ser persistidos em MySQL
6. ficam prontos para visualizacao no Metabase

O notebook continua como camada de descoberta. A `pipe` e onde as regras deixam de ser exploratorias e passam a ser reutilizaveis.

## Arquitetura Atual

Hoje o projeto trabalha com um desenho enxuto de ELT:

- `Bronze`: replica das tabelas brutas da origem
- `Silver`: consolidacao e tratamento dos dados operacionais
- `Gold`: dimensoes, fatos e agregados para analise

As transformacoes principais sao feitas em SQL em memoria, o que reduz a quantidade de `merge` manuais em pandas e deixa a logica mais proxima de um fluxo relacional real.

O SQL do projeto fica externalizado em arquivos:

- `sql/transform.sql` para a transformacao Gold
- `sql/ddl/bronze.sql`, `sql/ddl/silver.sql` e `sql/ddl/gold.sql` para o schema MySQL

## Estrutura do Projeto

```text
Sales_pipe/
|-- core/
|   |-- database.py
|   |-- getters.py
|   |-- logging_config.py
|   |-- mysql_schema.py
|   |-- validators.py
|   `-- paths.py
|-- pipe/
|   `-- sales.py
|-- sql/
|   |-- transform.sql
|   `-- ddl/
|       |-- bronze.sql
|       |-- silver.sql
|       `-- gold.sql
|-- data/
|   |-- entry/
|   `-- exit/
|-- conftest.py
|-- docker-compose.yml
|-- main.py
|-- requirements.txt
|-- test_gold.py
|-- test_silver.py
|-- test_validators.py
|-- Analise.ipynb
|-- Anotacoes.md
|-- .env
`-- README.md
```

## Fonte de Dados

As tabelas de origem atuais sao:

- `customers`
- `geolocation`
- `orders`
- `order_items`
- `order_payments`
- `order_reviews`
- `products`
- `sellers`
- `translation`

Relacoes principais:

- `orders.customer_id -> customers.customer_id`
- `order_items.order_id -> orders.order_id`
- `order_items.product_id -> products.product_id`
- `order_items.seller_id -> sellers.seller_id`
- `order_payments.order_id -> orders.order_id`
- `order_reviews.order_id -> orders.order_id`
- `products.product_category_name -> translation.product_category_name`
- `customers.customer_zip_code_prefix -> geolocation.geolocation_zip_code_prefix`
- `sellers.seller_zip_code_prefix -> geolocation.geolocation_zip_code_prefix`

## Camadas da Pipe

### Bronze

Replica os dados brutos da origem em tabelas com prefixo `bronze_`:

- `bronze_customers`
- `bronze_geolocation`
- `bronze_orders`
- `bronze_order_items`
- `bronze_order_payments`
- `bronze_order_reviews`
- `bronze_products`
- `bronze_sellers`
- `bronze_translation`

### Silver

Padroniza e trata os dados operacionais em tabelas com prefixo `silver_`:

- `silver_geolocations`
- `silver_product_categories`
- `silver_customers`
- `silver_sellers`
- `silver_products`
- `silver_orders`
- `silver_order_items`
- `silver_order_payments`
- `silver_order_reviews`

Regras relevantes da camada Silver:

- consolidacao de geolocalizacao por CEP
- media para `latitude` e `longitude`
- moda para `cidade` e `estado`
- ajuste de tipagem e arredondamento de valores monetarios

### Gold

Entrega a camada analitica com prefixo `gold_`:

- `gold_dim_customers`
- `gold_dim_products`
- `gold_dim_sellers`
- `gold_fct_orders`
- `gold_fct_order_items`
- `gold_agg_sales_monthly`

Essa e a camada pensada para MySQL e consumo no Metabase.

## O Que a Pipe Entrega Hoje

Quando voce roda `principal()`, a saida padrao e a camada Gold em formato de DataFrame:

- `gold_dim_customers`
- `gold_dim_products`
- `gold_dim_sellers`
- `gold_fct_orders`
- `gold_fct_order_items`
- `gold_agg_sales_monthly`

Quando voce roda a carga para MySQL, a pipeline monta e persiste as tres camadas.

## Principais Metricas Disponiveis

A camada Gold ja entrega campos uteis para KPI como:

- faturamento total
- ticket medio
- frete medio
- tempo medio de entrega
- atraso de entrega
- OTD (`otd_rate`)
- review medio
- comportamento de pagamento

## Integracao com MySQL

O projeto ja possui:

- criacao automatica do database, se ele nao existir
- criacao das tabelas Bronze, Silver e Gold
- tipagem definida no schema
- carga full refresh controlada para evitar duplicacao em novas execucoes
- DDL separado por camada em arquivos SQL

As variaveis ficam em [`.env`](/C:/Users/ramon/OneDrive/Desktop/estudos/Data_analysis/Sales_pipe/.env):

```env
MYSQL_HOST=
MYSQL_PORT=3306
MYSQL_DATABASE=
MYSQL_USER=
MYSQL_PASSWORD=
MYSQL_DRIVER=pymysql
MYSQL_ECHO=false
```

## Como Usar

Retornar a camada Gold para analise:

```python
from pipe.sales import principal

tables = principal()
orders = tables["gold_fct_orders"]
```

Persistir Bronze, Silver e Gold no MySQL:

```python
from pipe.sales import load_mysql_elt_tables

load_mysql_elt_tables()
```

Ou usar a mesma `principal()` e, ao mesmo tempo, gravar no banco:

```python
from pipe.sales import principal

gold_tables = principal(persist_mysql=True)
```

Executar a pipeline via CLI:

```bash
python main.py
python main.py --persist-mysql
python main.py --log-level DEBUG
```

Subir MySQL e Metabase localmente:

```bash
docker-compose up -d
```

## Qualidade e Operacao

O projeto agora tambem possui:

- validacoes leves entre as camadas em `core/validators.py`
- configuracao central de logging em `core/logging_config.py`
- testes unitarios para Silver, Gold e validadores
- entrypoint de linha de comando em `main.py`
- stack local com `docker-compose` para MySQL e Metabase

Os testes principais podem ser executados com:

```bash
pytest -q test_silver.py test_gold.py test_validators.py
```

## Pontos Fortes

- Mantem o projeto orientado a Analytics Engineering, sem estrutura pesada demais.
- Usa notebook para descoberta e pipeline para consolidacao.
- Trabalha com dados relacionais proximos de um cenario real.
- Ja separa dados em `bronze`, `silver` e `gold`.
- Ja esta preparado para persistencia em MySQL.
- Ja produz camada final adequada para BI.
- Ja possui validacoes basicas e testes automatizados.
- Ja possui SQL e DDL externalizados, o que facilita manutencao.

## Pontos de Melhoria

- Refinar estrategia de carga incremental quando o projeto evoluir.
- Documentar melhor cada tabela final para uso no Metabase.
- Ampliar as validacoes para integridade referencial e regras de negocio.
- Parametrizar melhor o `docker-compose` para ambientes diferentes.

## Direcao do Projeto

O projeto esta caminhando para uma stack local e simples de Analytics Engineering:

1. datalake em CSV
2. transformacao em camadas Bronze, Silver e Gold
3. persistencia em MySQL
4. visualizacao no Metabase

O foco nao e virar um projeto de engenharia de software pesado. O foco e construir uma pipeline analitica clara, util e pronta para responder perguntas de negocio.
