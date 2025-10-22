#!/bin/bash

# Script para instalar dependências do projeto
echo "Instalando dependências do projeto..."

# Instalar dependências do requirements.txt
pip3 install --user -r requirements.txt

# Instalar streamlit se não estiver instalado
pip3 install --user streamlit

echo "✅ Dependências instaladas com sucesso!"
echo "Para executar o projeto, use: streamlit run streamlit.py"
