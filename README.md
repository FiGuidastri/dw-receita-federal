# 🚀 Projeto CNPJ Analytics - Lista Fria de Clientes

Este projeto oferece uma solução completa de engenharia de dados e um aplicativo Streamlit interativo para processar, analisar e extrair informações valiosas dos dados abertos do CNPJ da Receita Federal. O objetivo principal é facilitar a criação de "listas frias" de potenciais clientes para prospecção, segmentadas por diversos critérios como CNAE, localização e porte da empresa.

## ✨ Funcionalidades Principais

### 1\. **Pipeline de Engenharia de Dados (Python)**

* **Download Automatizado**: Script para baixar todos os arquivos `.zip` da página da Receita Federal.

* **Extração e Unificação**: Descompacta os arquivos e unifica os dados de diferentes arquivos `.csv` que possuem o mesmo formato (ex: todos os arquivos de `Empresas`).

* **Conversão para Parquet**: Converte os dados unificados para o formato Parquet, otimizado para análise de grandes volumes de dados, garantindo alta performance e compressão eficiente.

* **Estruturação de Dados**: Organiza os dados em "tabelas" Parquet (ex: `empresas.parquet`, `estabelecimentos.parquet`, `socios.parquet`), seguindo o dicionário de dados fornecido pela Receita Federal.

### 2\. **Aplicativo Interativo (Streamlit)**

Um dashboard intuitivo para explorar os dados do CNPJ, com as seguintes seções:

* **📊 Dashboard Analítico**:

  * Visão geral com métricas chave (total de empresas, estabelecimentos ativos).

  * Gráficos de distribuição de empresas por UF e CNAEs mais comuns.

  * Análise temporal da abertura de empresas.

* **🔍 Pesquisa Avançada de Empresas**:

  * Ferramenta de busca flexível por Razão Social, Nome Fantasia, CNAE, UF, Situação Cadastral, Porte e Capital Social Mínimo.

  * Exibição dos resultados em tabela paginada.

  * Opções de exportação dos resultados para CSV e Excel.

* **📋 Gerador de Lista Fria de Clientes**:

  * Interface dedicada para construir listas de prospecção altamente segmentadas.

  * Filtros combináveis por CNAEs, UF, Porte da Empresa, Situação Cadastral (apenas ativas), e disponibilidade de E-mail/Telefone.

  * Métricas rápidas sobre a lista gerada (total de empresas, com e-mail, com telefone).

  * Preview da lista e exportação completa para CSV e Excel.

  * Análise visual da lista gerada (distribuição por estado e porte).

## ⚙️ Tecnologias Utilizadas

* **Python**: Linguagem de programação principal.

* **Pandas**: Manipulação e análise de dados.

* **PyArrow**: Leitura e escrita de arquivos Parquet.

* **DuckDB**: Banco de dados analítico embutido, utilizado para consultas SQL de alta performance diretamente nos arquivos Parquet.

* **Streamlit**: Framework para construção do aplicativo web interativo.

* **Plotly Express**: Geração de gráficos interativos para o dashboard.

* **Requests**: Para download de arquivos da web.

* **BeautifulSoup**: Para parsing de HTML (no script de download).

* **tqdm**: Barras de progresso para visualização do processamento.

## 🚀 Como Usar

### Pré-requisitos

Certifique-se de ter o Python 3.8+ instalado.

### 1\. Configuração do Ambiente

Clone o repositório (ou crie os arquivos `pipeline.py` e `app.py`):

```bash
git clone <URL_DO_SEU_REPOSITORIO>
cd cnpj-analytics
```

Instale as dependências necessárias:

```bash
pip install -r requirements.txt
```

Conteúdo do `requirements.txt`:

```
requests
beautifulsoup4
tqdm
pandas
pyarrow
duckdb
streamlit
plotly
openpyxl
```

### 2\. Download e Processamento dos Dados

Primeiro, execute o script de download para obter os arquivos `.zip` da Receita Federal. Eles serão salvos na pasta `dados_cnpj`.

```bash
python pipeline.py
```

Este script irá:

1. Baixar os arquivos `.zip` para a pasta `dados_cnpj`.

2. Extrair os conteúdos para uma pasta temporária `temp_extraidos`.

3. Unificar os `.csv` de mesmo tipo e convertê-los para `.parquet` na pasta `cnpj_parquet`.

**Atenção**: Os arquivos são muito grandes (vários GB). Certifique-se de ter espaço em disco suficiente e uma boa conexão com a internet.

### 3\. Executar o Aplicativo Streamlit

Após o processamento dos dados, inicie o aplicativo Streamlit:

```bash
streamlit run app.py
```

O aplicativo será aberto automaticamente no seu navegador padrão (geralmente em `http://localhost:8501`).

## 📂 Estrutura do Projeto

```
cnpj-analytics/
├── 01_download.py             # Script para download dos dados
├── 02_transform.py            # Script para processamento dos dados
├── app.py                  # Aplicativo Streamlit
├── requirements.txt        # Dependências do projeto
├── dados_cnpj/             # Pasta para os arquivos .zip baixados
│   ├── F.K03200UF.D10117.EMPRECSV.zip
│   ├── F.K03200UF.D10117.ESTABC.zip
│   └── ...
├── temp_extraidos/         # Pasta temporária para arquivos extraídos (será criada e limpa)
│   ├── K3241.K03200UF.D10117.EMPRECSV
│   └── ...
├── cnpj_parquet/           # Pasta para os arquivos .parquet processados
│   ├── empresas.parquet
│   ├── estabelecimentos.parquet
│   ├── socios.parquet
│   ├── simples.parquet
│   ├── cnaes.parquet
│   ├── municipios.parquet
│   └── ...
└── README.md               # Este arquivo
```

## 🤝 Contribuição

Contribuições são bem-vindas! Sinta-se à vontade para abrir issues, sugerir melhorias ou enviar pull requests.

## 📄 Licença

Este projeto está licenciado sob a licença MIT. Veja o arquivo `LICENSE` para mais detalhes. (Se aplicável)

---

**Desenvolvido por \[FG Data\]**