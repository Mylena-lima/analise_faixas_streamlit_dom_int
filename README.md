# Análise de Faixas Streamlit

Este projeto analisa dados de movimentos e passageiros de aeroportos por faixas.

## Pré-requisitos

- Python 3.11+
- Docker (para devcontainer) ou ambiente Python local

## Instalação

### Opção 1: Usando DevContainer (Recomendado)

1. Abra o projeto no VS Code
2. Instale a extensão "Dev Containers"
3. Pressione `Ctrl+Shift+P` e selecione "Dev Containers: Reopen in Container"
4. O ambiente será configurado automaticamente

### Opção 2: Ambiente Local

1. Instale as dependências:
```bash
pip install -r requirements.txt
```

2. Execute o projeto:
```bash
streamlit run streamlit.py
```

## Executando o Projeto

Após a instalação das dependências, execute:

```bash
streamlit run streamlit.py
```

O aplicativo estará disponível em: http://localhost:8501

## Dependências

- streamlit
- polars
- plotly
- pandas
- pyarrow

## Estrutura do Projeto

- `streamlit.py` - Aplicação principal
- `requirements.txt` - Dependências Python
- `faixas_aeroportos.parquet` - Dados de faixas de aeroportos
- `voos_por_aeronave_aeroporto_mes3.parquet` - Dados de voos
- `.devcontainer/` - Configuração do ambiente de desenvolvimento
