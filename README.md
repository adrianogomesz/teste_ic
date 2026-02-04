**Teste Técnico – Pipeline de Dados ANS**

Autor: Adriano Gomes

Stack: Python, Pandas, Requests, BeautifulSoup

Objetivo: Construir um pipeline completo de ingestão, tratamento, enriquecimento e agregação de dados públicos da ANS, com foco em robustez, clareza técnica e decisões justificadas.


**Visão Geral do Projeto**

Este projeto implementa um pipeline ETL (Extract, Transform, Load) a partir de dados públicos da ANS (Agência Nacional de Saúde Suplementar), cobrindo:

- Download automatizado dos dados trimestrais de despesas

- Tratamento de dados inconsistentes (padrão comum em dados governamentais)

- Enriquecimento com dados cadastrais das operadoras (CADOP)

- Agregação final por operadora, UF, ano e trimestre

- Geração de arquivos CSV finais e compactação para entrega

Os dados não são versionados no repositório. Todo o processo é reprodutível via execução do pipeline.


Estrutura do Projeto

main.py         # Orquestração do pipeline
scraper.py      # Navegação e descoberta de URLs
downloader.py   # Download e extração de arquivos
http_client.py  # Comunicação HTTP isolada
transformer.py  # Limpeza, enriquecimento e agregações
utils.py        # Funções utilitárias (ex: formatação BRL)

data/
raw/            # Dados brutos (ignorado no Git)
processed/      # Dados processados (ignorado no Git)



**Arquitetura do Pipeline (ETL)**

Extract:

- Navegação automática no repositório FTP da ANS

- Localização da pasta Demonstrações Contábeis

- Identificação dos 3 trimestres mais recentes

- Download e extração de arquivos .zip

Transform:

- Leitura defensiva dos CSVs trimestrais

- Normalização de nomes de colunas

- Conversão segura de valores monetários

- Enriquecimento com dados cadastrais (CADOP)

- Inclusão de UF e Modalidade

- Agregações analíticas

Load:

- Exportação de CSVs finais

- Codificação compatível com Excel

- Compactação dos arquivos de entrega


**Tradeoffs Técnicos**

**Scraping vs API REST**

Apesar do PDF mencionar “API REST”, os dados da ANS são disponibilizados via repositório FTP público, sem endpoints REST clássicos.

Para contornar isso, utilizei requests + BeautifulSoup para navegar e descobrir recursos dinamicamente.

Justificativa:

- Não há documentação de endpoints REST formais

- Estrutura FTP é estável e pública

- Evita hardcode de URLs

**Separação de Responsabilidades (Arquitetura)**

O código foi dividido em módulos com responsabilidades claras:

- scraper.py → Descoberta de URLs

- downloader.py → Download e extração

- transformer.py → Regras de negócio e dados

- utils.py → Funções puras reutilizáveis

- main.py → Orquestração do fluxo

Justificativa:

- Código mais legível

- Facilita testes unitários

- Facilita manutenção e evolução


**Leitura Defensiva de CSVs Governamentais**

Dados públicos frequentemente apresentam encoding inconsistentes, colunas com nomes variados, valores numéricos como strings, dados faltates ou inválidos. 
Para contornar isso, utilizei encoding explicito na leitura (latin1), normalizei nomes de colunas, utilizei validações explicitas de schema, erros tratados por arquivo sem quebrar o pipeline inteiro.


**Formatação Monetária (BRL)**

Formatar valores como string vs manter valores numéricos

Os valores foram mantidos como float durante todo o processamento, a formatação para BRL foi aplicada apenas na etapa final de exportação.

Justificativa:

- Evita erros em cálculos

- Facilita uso futuro em banco de dados ou APIs

- Mantém precisão durante agregações


**Estratégia de Agregação**

Agregação realizada por:

- CNPJ

- RazaoSocial

- UF

- Ano

- Trimestre

Métricas calculadas:

- DespesaTotal

- DespesaMediaTrimestre

- Resultados ordenados por DespesaTotal (decrescente).

Essa estrutura atende tanto análises exploratórias quanto uso posterior em banco ou API.


**Dados Não Versionados**

Os diretórios data/raw e data/processed são ignorados no Git.

Justificativa:

- Evita versionamento de dados grandes

- Repositório mais limpo

- Pipeline totalmente reprodutível

Para gerar os dados:

python -m api_ans.main


**Arquivos Gerados**

- despesas_consolidadas.csv

- despesas_agregadas.csv

- Teste_Adriano_Gomes.zip

Todos gerados automaticamente pelo pipeline.


**Como reproduzir?**

Crie um ambiente virtual:

windows: 
1. python -m venv venv

2. .\venv\Scripts\activate

linux/mac:
1. python3 -m venv venv

2. source venv/bin/activate


Instale dependências:

1. pip install -r requirements.txt


Execute:

1. python -m api_ans.main


**Considerações Finais:**

Este projeto foi desenvolvido com foco em:

- Robustez frente a dados reais

- Clareza arquitetural

- Justificativa consciente de trade-offs

- Boas práticas de versionamento e organização


**O pipeline foi pensado para ser facilmente extensível para:**

- Persistência em banco de dados

- Exposição via API

- Integração com ferramentas analíticas