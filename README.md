# Sales_pipe

## Visão Geral

A `Sales_pipe` é um projeto de Engenharia e Análise de Dados que usa um conjunto de tabelas de e-commerce como base para estudar ingestão, organização de dados, modelagem analítica e construção de indicadores de negócio.

O projeto parte de um cenário próximo do mundo real: os dados chegam fragmentados em múltiplos arquivos CSV relacionados entre si, funcionando como um pequeno datalake relacional. A proposta da `Sales_pipe` é transformar esse conjunto de tabelas em uma base confiável para exploração, consultas SQL, limpeza, modelagem e construção de KPIs.

Hoje o foco principal está em três frentes:

- organizar a entrada dos dados
- entender os relacionamentos entre as tabelas
- criar uma base sólida para análises de vendas, entrega, pagamentos e comportamento operacional

## Objetivo do Projeto

O objetivo da `Sales_pipe` não é apenas carregar arquivos, mas evoluir para uma pipeline analítica com visão de produto de dados. Isso significa sair de arquivos soltos e caminhar para um fluxo mais estruturado, onde seja possível:

- carregar tabelas de forma padronizada
- preservar os relacionamentos por chaves de negócio
- trabalhar com essas tabelas em SQL
- testar hipóteses no notebook antes de consolidar regras no código
- transformar análises exploratórias em funções reutilizáveis

Em outras palavras, o notebook é o espaço de descoberta, enquanto a `pipe` tende a se tornar o espaço de padronização e produção.

## Estado Atual

No estágio atual, o projeto já possui:

- uma camada de entrada de dados em `data/entry`
- uma `pipe` que carrega as tabelas do datalake e expõe seus relacionamentos
- um notebook de análise para exploração inicial, limpeza e definição de métricas
- utilitários para leitura de múltiplos formatos e descoberta de arquivos

Atualmente a direção do projeto está mais orientada para **tabelas relacionais e consultas SQL** do que para um único dataframe final achatado. Essa escolha deixa o projeto mais próximo de cenários reais de engenharia, onde diferentes entidades precisam ser preservadas antes de qualquer consolidação analítica.

## Estrutura do Projeto

```text
Sales_pipe/
├── core/
│   ├── getters.py
│   ├── input_resolver.py
│   └── paths.py
├── pipe/
│   └── sales.py
├── data/
│   ├── entry/
│   └── exit/
├── Analise.ipynb
└── Anotações.md
```

### Papel de cada parte

- `core/getters.py`: centraliza a leitura de arquivos e o suporte a diferentes formatos.
- `core/input_resolver.py`: ajuda a localizar arquivos automaticamente em diretórios.
- `core/paths.py`: concentra os caminhos principais do projeto.
- `pipe/sales.py`: carrega as tabelas da camada de entrada e expõe os relacionamentos entre elas.
- `Analise.ipynb`: ambiente de exploração, prototipagem de tratamentos, testes de joins e definição de KPIs.

## Tabelas Trabalhadas

O projeto está usando um conjunto de tabelas relacionadas do domínio de vendas, incluindo:

- pedidos
- itens de pedido
- pagamentos
- clientes
- produtos
- vendedores
- reviews
- geolocalização
- tradução de categoria de produto

Essas tabelas se relacionam principalmente por chaves como:

- `order_id`
- `customer_id`
- `product_id`
- `seller_id`
- `product_category_name`
- prefixos de CEP

Esse desenho torna o projeto especialmente útil para estudar modelagem relacional, joins, agregações e construção de camadas analíticas.

## Exemplos de Análises e KPIs Esperados

Pelo material já explorado no notebook, a `Sales_pipe` caminha para gerar indicadores como:

- ticket médio
- valor médio de frete
- faturamento histórico
- tendência temporal de vendas
- tempo médio de entrega
- atraso de entrega
- OTD (On Time Delivery)
- comportamento de pagamento

Esses indicadores podem ser evoluídos tanto por consultas SQL quanto por funções da própria pipeline.

## Pontos Fortes

- Usa um caso próximo do mundo real, com múltiplas tabelas e relacionamentos reais.
- Já nasce com separação entre entrada de dados, lógica da pipeline e ambiente de análise.
- Permite explorar o dado no notebook antes de transformar regras em código reutilizável.
- Mantém o projeto aberto para evoluir de CSVs para um fluxo relacional mais maduro.
- Tem potencial claro para servir como portfólio de Engenharia de Dados, ETL e Analytics Engineering.

## Pontos de Melhoria

- Formalizar um banco relacional local, como SQLite, para consolidar a etapa SQL.
- Criar uma camada de transformação com regras versionadas e reutilizáveis.
- Padronizar melhor nomes de colunas, encoding e convenções de nomenclatura.
- Adicionar validações automáticas para integridade das chaves entre tabelas.
- Criar testes para garantir que mudanças na pipeline não quebrem relações importantes.
- Persistir saídas analíticas intermediárias em vez de depender apenas do notebook.
- Organizar melhor o fluxo entre exploração, tratamento e produção.

## Para Onde o Projeto Está Indo

A tendência natural da `Sales_pipe` é evoluir de um projeto exploratório para uma pequena plataforma analítica local. O caminho mais coerente, olhando o estado atual do notebook e da `pipe`, é:

1. consolidar as tabelas em ambiente SQL
2. validar joins, métricas e regras no notebook
3. transformar essas regras em funções estáveis na pipeline
4. gerar tabelas analíticas ou marts voltados para KPI
5. preparar o projeto para consultas, dashboards ou automações futuras

Esse direcionamento faz com que a `Sales_pipe` deixe de ser apenas um estudo com CSVs e passe a ser um projeto de estruturação analítica com foco em clareza, reprodutibilidade e evolução incremental.

## Exemplo de Uso Atual

Hoje a `pipe` pode ser usada para carregar as tabelas brutas do datalake:

```python
from pipe.sales import principal, get_relationships

tables = principal()
relationships = get_relationships()

orders = tables["orders"]
order_items = tables["order_items"]
```

Esse fluxo reforça a ideia central do projeto neste momento: primeiro entender e organizar as entidades relacionais, depois consolidar as análises.
