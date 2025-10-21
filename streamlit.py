import streamlit as st
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
import hashlib
import locale
import pandas as pd


# ----------------------------------------------------------

# Função para formatar números com separador de milhares usando ponto
def formatar_numero(numero, casas_decimais=0):
    """
    Formata um número com separador de milhares usando ponto (.) em vez de vírgula (,)
    
    Args:
        numero (int, float): Número a ser formatado
        casas_decimais (int): Número de casas decimais (padrão: 0)
    
    Returns:
        str: Número formatado com separador de milhares usando ponto
    """
    if numero is None or (isinstance(numero, float) and numero != numero):  # Verifica NaN
        return "0"
    
    # Converter para float se necessário
    numero_float = float(numero)
    
    # Formatar com separador de milhares usando ponto
    if casas_decimais == 0:
        # Para números inteiros
        return f"{numero_float:,.0f}".replace(",", ".")
    else:
        # Para números com casas decimais
        return f"{numero_float:,.{casas_decimais}f}".replace(",", ".")


# Configuração da página
st.set_page_config(
    page_title="Análise de Faixas de Aeroportos PAN atualizada",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- LOGIN ---
def login():
    st.title("Análise de Faixas de Aeroportos PAN atualizada - Login")
    with st.form("login_form"):
        username = st.text_input("Usuário")
        password = st.text_input("Senha", type="password")
        submitted = st.form_submit_button("Entrar")

        if submitted:
            correct_username = st.secrets["credentials"]["username"]
            correct_password = st.secrets["credentials"]["password"]
            
            if username == correct_username and password == correct_password:
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.error("Usuário ou senha inválidos")

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    login()
    st.stop()

# Função para gerar cores consistentes para aeronaves
@st.cache_data
def gerar_paleta_cores_aeronaves():
    """Gera uma paleta de cores consistente para aeronaves"""
    # Paleta de cores vibrantes e distintas
    cores_base = [
    '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
    '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf',
    '#aec7e8', '#ffbb78', '#98df8a', '#ff9896', '#c5b0d5',
    '#c49c94', '#f7b6d3', '#FF0000', '#dbdb8d', '#9edae5', # Vermelho Vivo
    '#393b79', '#5254a3', '#6b6ecf', '#9c9ede', '#637939',
    '#8ca252', '#b5cf6b', '#cedb9c', '#8c6d31', '#bd9e39',
    '#FF8C00', '#e7cb94', '#843c39', '#ad494a', '#d6616b', # Laranja Escuro
    '#e7969c', '#7b4173', '#a55194', '#ce6dbd', '#de9ed6',
    '#40E0D0', '#FFD700', '#6A5ACD', '#FA8072', '#008B8B',
    'B8860B', '#FF00FF', '#32CD32', '#8B0000', '#66CDAA',
    '#D2691E', '#000080', '#F0E68C', '#DDA0DD'
    ]
    return cores_base

def obter_cor_aeronave(aeronave, todas_aeronaves):
    """Obtém cor consistente para uma aeronave específica"""
    cores_paleta = gerar_paleta_cores_aeronaves()
    
    # Criar lista ordenada de aeronaves para garantir uma atribuição consistente
    aeronaves_ordenadas = sorted(list(set(todas_aeronaves)))
    
    try:
        # Usar o índice da aeronave na lista ordenada para garantir cores distintas
        indice_aeronave = aeronaves_ordenadas.index(aeronave)
        indice_cor = indice_aeronave % len(cores_paleta)
        return cores_paleta[indice_cor]
    except ValueError:
        # Fallback (não deve acontecer se a lista de aeronaves estiver completa)
        hash_aeronave = int(hashlib.md5(aeronave.encode()).hexdigest(), 16)
        indice_cor = hash_aeronave % len(cores_paleta)
        return cores_paleta[indice_cor]

@st.cache_data
def carregar_dados():
    aeroporto_pax = pl.read_parquet("faixas_aeroportos.parquet").with_columns(
        pl.col("ano").cast(pl.Int64)
    )

    # Carregar dados de voos (arquivo já contém coluna 'mes')
    voos_aeroporto_aeronave = pl.read_parquet("voos_por_aeronave_aeroporto_mes3.parquet").with_columns([
        pl.col("ano").cast(pl.Int64),
        pl.col("mes").cast(pl.Int64)
    ])

    # Calcular o total de passageiros (pax) do DW por aeroporto e ano
    pax_dw = (voos_aeroporto_aeronave
              .group_by(["aeroporto", "ano"])
              .agg(pl.sum("pax").alias("passageiros_dw")))

    # Juntar os dados de pax do DW com o DataFrame principal de aeroportos
    aeroporto_pax = aeroporto_pax.join(pax_dw, on=["aeroporto", "ano"], how="left")

    # Atualizar a coluna de passageiros:
    # - Usar 'passageiros_dw' para anos < 2025. Se for nulo (sem voos), será 0.
    # - Manter 'passageiros_projetado' para 2025
    aeroporto_pax = aeroporto_pax.with_columns(
        pl.when(pl.col("ano") < 2025)
        .then(pl.col("passageiros_dw"))
        .otherwise(pl.col("passageiros_projetado"))
        .alias("passageiros_atualizado")
    ).drop("passageiros_projetado", "passageiros_dw").rename({"passageiros_atualizado": "passageiros_projetado"}).with_columns(
        pl.col("passageiros_projetado").fill_null(0) # Preencher nulos com 0
    )

    faixas_padrao = {
    'bins': [0, 2000, 30000, 50000, 200000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, float('inf')],
    'labels': ['Faixa_AvG', 'Faixa_1', 'Faixa_2', 'Faixa_3', 'Faixa_4', 'Faixa_5', 'Faixa_6', 'Faixa_7', 'Faixa_8', 'Faixa_9', 'Faixa_10']
}
    return aeroporto_pax, voos_aeroporto_aeronave, faixas_padrao

# Carregar dados e mostrar informações de debug
aeroporto_pax, voos_aeroporto_aeronave, faixas_padrao = carregar_dados()


# Filtrar dados para remover período 2025-T4
# Aplicar filtro apenas ao DataFrame que possui coluna "mês"


# aeroporto_pax não possui coluna "mês", então não precisa deste filtro específico

 #--- SIDEBAR (BARRA LATERAL) PARA FILTROS ---
st.sidebar.header("📊 Informações dos Dados")
st.sidebar.markdown("*Dados disponíveis para análise:*")

# Informações sobre os dados
total_aeroportos_disponiveis = aeroporto_pax["aeroporto"].n_unique()
anos_disponiveis = sorted(aeroporto_pax["ano"].unique().to_list())
periodo_dados = f"{min(anos_disponiveis)} - {max(anos_disponiveis)}"

st.sidebar.metric("Total de Aeroportos", formatar_numero(total_aeroportos_disponiveis))
st.sidebar.metric("Período dos Dados", periodo_dados)
st.sidebar.info(f"📅 **Anos Disponíveis:**\n{', '.join(map(str, anos_disponiveis))}")

# Usar todos os aeroportos disponíveis (sem filtros)
# Filtro por aeroportos com seleção de anos
if 'aeroportos_excluidos' not in st.session_state:
    st.session_state['aeroportos_excluidos'] = []
if 'anos_exclusao' not in st.session_state:
    st.session_state['anos_exclusao'] = {}

st.sidebar.markdown("### 🚫 **Exclusão de Aeroportos**")

# Definir a lista de aeroportos padrão para exclusão
aeroportos_padrao_exclusao = [
    'SBGV', 'SBIL', 'SBJV', 'SBKG', 'SBME', 'SBML', 'SBPB', 'SBPP', 'SBRJ', 
    'SBRP', 'SBSM', 'SBSP', 'SBSR', 'SBTC', 'SBTE', 'SBUA', 'SBUG', 'SBUR', 
    'SBUY', 'SIRI', 'SISO', 'SNCL', 'SNRJ', 'SNTI', 'SSCT', 'SSUV', 'SWCA', 
    'SBJR', 'SBMI', 'SBCO', 'SBSJ', 'SBIP'
]

# Botão para selecionar os padrões
if st.sidebar.button("Selecionar Exclusões Padrão"):
    st.session_state['aeroportos_excluidos'] = aeroportos_padrao_exclusao

# Seletor de aeroportos para exclusão
aeroportos_excluidos = st.sidebar.multiselect(
    "Selecione o Aeroporto a ser excluído:",
    options=aeroporto_pax["aeroporto"].unique().sort(),
    key='aeroportos_excluidos',
    help="Selecione os aeroportos que deseja excluir da análise ou clique no botão para selecionar o padrão"
)

# Seletor de anos para exclusão (apenas se houver aeroportos selecionados)
if aeroportos_excluidos:
    st.sidebar.markdown("#### 📅 **Anos de Exclusão**")
    st.sidebar.markdown("*Selecione os anos em que cada aeroporto deve ser excluído:*")
    
    # Obter anos disponíveis
    anos_disponiveis_exclusao = sorted(aeroporto_pax["ano"].unique().to_list())
    
    # Criar controles para cada aeroporto selecionado
    for aeroporto in aeroportos_excluidos:
        # Inicializar anos de exclusão para este aeroporto se não existir
        if aeroporto not in st.session_state['anos_exclusao']:
            st.session_state['anos_exclusao'][aeroporto] = anos_disponiveis_exclusao.copy()
        
        # Seletor de anos para este aeroporto
        anos_selecionados = st.sidebar.multiselect(
            f"**{aeroporto}** - Anos para excluir:",
            options=anos_disponiveis_exclusao,
            default=st.session_state['anos_exclusao'][aeroporto],
            key=f'anos_exclusao_{aeroporto}',
            help=f"Selecione os anos em que {aeroporto} deve ser excluído"
        )
        
        # Atualizar session state
        st.session_state['anos_exclusao'][aeroporto] = anos_selecionados
    
    # Mostrar resumo das exclusões
    st.sidebar.markdown("#### 📋 **Resumo das Exclusões**")
    for aeroporto in aeroportos_excluidos:
        anos_exclusao = st.session_state['anos_exclusao'].get(aeroporto, [])
        if anos_exclusao:
            anos_texto = ', '.join(map(str, sorted(anos_exclusao)))
            st.sidebar.info(f"**{aeroporto}**: Excluído em {anos_texto}")
        else:
            st.sidebar.success(f"**{aeroporto}**: Nenhum ano selecionado (não será excluído)")

# Aplicar filtros de exclusão
# Para df_filtrado1 (voos_aeroporto_aeronave) - tem coluna ano
condicoes_exclusao1 = []
for aeroporto in aeroportos_excluidos:
    anos_exclusao = st.session_state['anos_exclusao'].get(aeroporto, [])
    if anos_exclusao:
        # Excluir aeroporto nos anos selecionados
        condicao = ~((pl.col("aeroporto") == aeroporto) & (pl.col("ano").is_in(anos_exclusao)))
        condicoes_exclusao1.append(condicao)

# Adicionar filtro para excluir aeronave E110
condicoes_exclusao1.append(pl.col("aeronave") != "E110")

if condicoes_exclusao1:
    # Combinar todas as condições com AND
    condicao_final1 = condicoes_exclusao1[0]
    for condicao in condicoes_exclusao1[1:]:
        condicao_final1 = condicao_final1 & condicao
    df_filtrado1 = voos_aeroporto_aeronave.filter(condicao_final1)
else:
    df_filtrado1 = voos_aeroporto_aeronave

# Para df_filtrado2 (aeroporto_pax) - tem coluna ano
condicoes_exclusao2 = []
for aeroporto in aeroportos_excluidos:
    anos_exclusao = st.session_state['anos_exclusao'].get(aeroporto, [])
    if anos_exclusao:
        # Excluir aeroporto nos anos selecionados
        condicao = ~((pl.col("aeroporto") == aeroporto) & (pl.col("ano").is_in(anos_exclusao)))
        condicoes_exclusao2.append(condicao)

if condicoes_exclusao2:
    # Combinar todas as condições com AND
    condicao_final2 = condicoes_exclusao2[0]
    for condicao in condicoes_exclusao2[1:]:
        condicao_final2 = condicao_final2 & condicao
    df_filtrado2 = aeroporto_pax.filter(condicao_final2)
else:
    df_filtrado2 = aeroporto_pax


# Título principal
st.title("⚙️ Configurador de Faixas de Aeroportos atualizado - PAN")
st.markdown("### Defina intervalos personalizados para classificação de aeroportos por passageiros (E + D)")

# Adicionar informações sobre a aplicação
with st.expander("ℹ️ **Sobre esta Aplicação**", expanded=False):
    st.markdown("""
    **📋 Funcionalidades Principais:**

    **Aba "Análise de Faixas":**
    - 🎯 **Configuração de Faixas Personalizadas**: Defina intervalos customizados para classificação de aeroportos usando sliders ou inserindo o número exato.
    - 📊 **Análise de Distribuição**: Visualize como os aeroportos se distribuem pelas faixas configuradas anualmente ou em todo o período.
    - 📈 **Evolução de Movimentos**: Acompanhe a evolução de movimentos (P + D) por aeronave para aeroportos em uma determinada faixa.
    - ✈️ **Utilização de Aeronaves**: Analise o percentual de aeroportos em uma faixa que utilizam cada tipo de aeronave.
    - 🔍 **Exploração Detalhada**: Explore a lista de aeroportos, movimentos e passageiros por faixa, ano, e aeronave.

    **Aba "Análise por Categoria":**
    - 📈 **Análise Cumulativa de Frota**: Visualize a participação de voos de cada categoria de aeronave à medida que o total de passageiros se acumula em um determinado ano.
    - 📋 **Dados Agregados**: Acesse uma tabela com o total de passageiros e movimentos para cada categoria de aeronave no ano selecionado.
    - 🔢 **Análise Multi-Ano**: Selecione múltiplos anos para análise agregada de categorias de aeronave.
    - 📊 **Composição de Movimentos**: Visualize movimentos (P + D) e passageiros (E + D) por categoria com percentuais detalhados.
    - 📋 **Tabelas Detalhadas**: Acesse dados detalhados de percentual de movimentos e passageiros por categoria.

    **Aba "Tabela de Presença de Movimentos":**
    - 📋 **Presença de Movimentos**: Visualize tabela com presença (Sim/Não) de movimentos por aeroporto, aeronave e período (mês-ano).
    - 🔍 **Filtros Avançados**: Filtre por aeroportos e aeronaves específicos com botões "Selecionar Todos".
    - 📅 **Análise de Meses Consecutivos**: Calcule máximo, mínimo e médio de meses consecutivos com movimentação.
    - 🔢 **Filtros de Meses Consecutivos**: Filtre por operadores (maior que, menor que, igual a, etc.) com valores personalizados.
    - 📊 **Gráfico de Presença**: Visualize presença de movimentos (0/1) ao longo do tempo para cada combinação aeroporto-aeronave.
    - 🎯 **Exclusão Automática**: Remove automaticamente combinações sem movimento para análise mais focada.
    
    **💡 Dicas de Uso:**
    - Navegue entre as três abas para diferentes perspectivas de análise.
    - Na configuração de faixas, use os sliders para ajustes rápidos e os campos numéricos para valores precisos.
    - Utilize os filtros na barra lateral para excluir aeroportos em anos específicos.
    - Na "Tabela de Presença", use os botões "Selecionar Todos" para resetar filtros rapidamente.
    - Expanda as seções para análises mais detalhadas e visualização de tabelas de dados.
    - Use o gráfico de presença para identificar padrões temporais de movimentação.
    """)

st.markdown("---")


# --- ABAS DE NAVEGAÇÃO ---
tab1, tab2, tab3 = st.tabs(["📊 Análise de Faixas", "✈️ Análise por Categoria", "📋 Presença de Movimentos"])

with tab1:
    # Seção de Configuração de Faixas Personalizadas - DESTAQUE PRINCIPAL
    st.header("🎯 **Configuração de Faixas Personalizadas**")

    # Toggle para usar faixas personalizadas - em destaque
    usar_faixas_personalizadas = st.checkbox("🔧 **Ativar Configuração Personalizada de Faixas**", value=True, help="Ative para definir seus próprios intervalos de faixas")

    if usar_faixas_personalizadas:
        # Seletor de quantidade de faixas
        st.markdown("#### ⚙️ **Configuração Inicial**")
        col_config1, col_config2 = st.columns(2)
        
        with col_config1:
            num_faixas = st.selectbox(
                "📊 **Quantidade de Faixas:**",
                options=[3, 4, 5, 6, 7, 8, 9, 10, 11],
                index=7,  # Default para 10 faixas (índice 7)
                help="Escolha quantas faixas deseja configurar (além da Faixa AvG)"
            )
        
        with col_config2:
            st.info(f"**Configuração Atual:**\n- Faixa AvG (sempre presente)\n- {num_faixas} faixas numeradas\n- **Total: {num_faixas + 1} faixas**")
        
        st.markdown("---")

        
        # Container em destaque para os sliders
        with st.container():
            st.markdown("#### 🎚️ **Ajuste os Intervalos das Faixas**")
            st.markdown("*Use os sliders abaixo para definir os limites de cada faixa de passageiros (E + D):*")
            
            # Valores padrão baseados nas faixas_padrao (ajustados dinamicamente)
            valores_padrao_base = [2000, 30000, 50000, 200000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 25000000]
            
            # Cores e ícones para as faixas
            cores_icones = ["🟢", "🟡", "🟠", "🔴", "🟣", "🔵", "⚫", "⚪", "🟤", "🔶", "🔷"]
            
            # Criar sliders dinamicamente baseado na quantidade selecionada
            faixas_personalizadas = []
            
            # Callbacks para sincronizar slider e number_input
            def slider_callback(idx):
                st.session_state[f'num_faixa_{idx}'] = st.session_state[f'slider_faixa_{idx}']

            def num_input_callback(idx):
                st.session_state[f'slider_faixa_{idx}'] = st.session_state[f'num_faixa_{idx}']
            
            # Calcular quantas colunas usar baseado no número de faixas
            num_colunas = 2 if num_faixas <= 6 else 3
            
            # Organizar sliders em grupos
            sliders_por_linha = 2 if num_colunas == 2 else 3
            num_linhas = (num_faixas + sliders_por_linha - 1) // sliders_por_linha
            
            for linha in range(num_linhas):
                # Determinar o título da seção baseado na linha
                if linha == 0:
                    st.markdown("**🔹 Faixas Iniciais**")
                elif linha == num_linhas - 1:
                    st.markdown("**🔹 Faixas Superiores**")
                else:
                    st.markdown(f"**🔹 Faixas Intermediárias**")
                
                # Criar colunas para esta linha
                cols = st.columns(num_colunas)
                
                # Adicionar sliders nesta linha
                for col_idx in range(sliders_por_linha):
                    faixa_idx = linha * sliders_por_linha + col_idx
                    
                    if faixa_idx < num_faixas:
                        with cols[col_idx % num_colunas]:
                            # Determinar valores para este slider
                            if faixa_idx == 0:
                                # Primeira faixa (AvG → 1)
                                min_val = 100
                                max_val = 50000
                                default_val = valores_padrao_base[faixa_idx]
                                step = 100
                                label = f"{cores_icones[faixa_idx]} Limite Faixa AvG → Faixa 1"
                                help_text = "Pax (E + D) até este valor serão classificados como Faixa AvG"
                            else:
                                # Faixas subsequentes
                                if faixa_idx == 4:
                                    incremento = 300_00
                                elif faixa_idx == 5:
                                    incremento = 500_00
                                elif faixa_idx == 6:
                                    incremento = 1_000_00
                                elif faixa_idx == 7:
                                    incremento = 3_000_00
                                elif faixa_idx == 8 or faixa_idx == 9:
                                    incremento = 5_000_00
                                elif faixa_idx <= 3:
                                    incremento = 100
                                else:
                                    incremento = 5000
                                min_val = faixas_personalizadas[faixa_idx-1] + incremento
                                
                                # Determinar max_val baseado na posição da faixa
                                if faixa_idx < 3:
                                    max_val = 500000
                                    step = 100
                                elif faixa_idx < 6:
                                    max_val = 5000000
                                    step = 1000
                                else:
                                    max_val = 25000000
                                    step = 10000
                                
                                default_val = max(valores_padrao_base[faixa_idx] if faixa_idx < len(valores_padrao_base) else min_val + step, min_val)
                                label = f"{cores_icones[faixa_idx]} Limite Faixa {faixa_idx} → Faixa {faixa_idx + 1}"
                                help_text = f"Pax (E + D) até este valor serão classificados como Faixa {faixa_idx}"
                            
                            # Criar o slider e o number input
                            # Inicializar session_state se não existir
                            if f'slider_faixa_{faixa_idx}' not in st.session_state:
                                st.session_state[f'slider_faixa_{faixa_idx}'] = default_val
                            if f'num_faixa_{faixa_idx}' not in st.session_state:
                                st.session_state[f'num_faixa_{faixa_idx}'] = default_val

                            st.markdown(f"**{label}**")
                            s_col, n_col = st.columns([3, 2])
                            with s_col:
                                st.slider(
                                    label,
                                    min_value=min_val,
                                    max_value=max_val,
                                    step=step,
                                    help=help_text,
                                    key=f"slider_faixa_{faixa_idx}",
                                    on_change=slider_callback,
                                    args=(faixa_idx,),
                                    label_visibility="collapsed"
                                )
                            with n_col:
                                st.number_input(
                                    label,
                                    min_value=min_val,
                                    max_value=max_val,
                                    step=step,
                                    key=f"num_faixa_{faixa_idx}",
                                    on_change=num_input_callback,
                                    args=(faixa_idx,),
                                    label_visibility="collapsed"
                                )
                            
                            faixa_limite = st.session_state[f'slider_faixa_{faixa_idx}']
                            faixas_personalizadas.append(faixa_limite)
            
        # Construir as faixas personalizadas dinamicamente
        bins_personalizados = [0] + faixas_personalizadas + [float('inf')]
        
        # Gerar labels dinamicamente baseado na quantidade de faixas
        labels_personalizados = ['Faixa_AvG'] + [f'Faixa_{i}' for i in range(1, num_faixas + 1)]
        
        faixas_utilizadas = {
            'bins': bins_personalizados,
            'labels': labels_personalizados
        }
        
        # Mostrar resumo das faixas personalizadas em destaque
        st.markdown("---")
        st.markdown("#### 📋 **Resumo das Faixas Configuradas**")

        # Determinar número de colunas para o resumo baseado na quantidade de faixas
        num_cols_resumo = min(4, num_faixas + 1)  # Máximo 4 colunas
        cols = st.columns(num_cols_resumo)

        for i, (inicio, fim, label) in enumerate(zip(bins_personalizados[:-1], bins_personalizados[1:], labels_personalizados)):
            col_idx = i % num_cols_resumo
            with cols[col_idx]:
                if fim == float('inf'):
                    st.info(f"**{label}**\n{formatar_numero(inicio)}+ passageiros (E + D)")
                else:
                    st.info(f"**{label}**\n{formatar_numero(inicio)} - {formatar_numero(fim)} passageiros (E + D)")
    else:
        # Usar faixas padrão quando não estiver usando faixas personalizadas
        faixas_utilizadas = faixas_padrao

    st.markdown("---")

    # Função para aplicar as faixas aos dados
    def aplicar_faixas_personalizadas(df, faixas):
        """Aplica as faixas personalizadas aos dados de passageiros"""
        bins = faixas['bins']
        labels = faixas['labels']
        
        # Criar condições para cada faixa
        conditions = []
        for i in range(len(bins) - 1):
            if i == 0:
                # Primeira faixa: passageiros >= bins[0] e < bins[1]
                condition = (pl.col("passageiros_projetado") >= bins[i]) & (pl.col("passageiros_projetado") < bins[i + 1])
            elif i == len(bins) - 2:
                # Última faixa: passageiros >= bins[i] (incluindo infinito)
                condition = pl.col("passageiros_projetado") >= bins[i]
            else:
                # Faixas intermediárias: passageiros >= bins[i] e < bins[i + 1]
                condition = (pl.col("passageiros_projetado") >= bins[i]) & (pl.col("passageiros_projetado") < bins[i + 1])
            
            conditions.append((condition, labels[i]))
        
        # Aplicar as condições usando when/then/otherwise
        faixa_expr = pl.when(conditions[0][0]).then(pl.lit(conditions[0][1]))
        
        for condition, label in conditions[1:]:
            faixa_expr = faixa_expr.when(condition).then(pl.lit(label))
        
        faixa_expr = faixa_expr.otherwise(pl.lit("Indefinido"))
        
        df_com_faixas = df.with_columns([
            faixa_expr.alias("faixa_personalizada")
        ])
        
        return df_com_faixas

    # Aplicar as faixas aos dados filtrados
    df_com_faixas = aplicar_faixas_personalizadas(df_filtrado2, faixas_utilizadas)

    # Seção de Análise das Faixas (Compacta)
    st.header("📊 Resultado da Configuração")

    # Controles de filtro por ano para o gráfico principal
    st.markdown("#### 🔍 **Distribuição de Aeroportos por Faixa**")

    # Usar dados consolidados (todos os anos) - mostrar por ano e faixa
    distribuicao_faixas = (df_com_faixas
                          .group_by(["faixa_personalizada", "ano"])
                          .agg([
                              pl.count("aeroporto").alias("quantidade_aeroportos"),
                              pl.mean("passageiros_projetado").alias("media_passageiros")
                          ])
                          .sort(["faixa_personalizada", "ano"]))
    
    titulo_grafico = "📊 **Distribuição por Faixa - Comparação entre Anos**"

    # Mostrar gráfico principal
    st.markdown(titulo_grafico)
    chart_data = distribuicao_faixas.to_pandas()

    # Verificar se há dados para mostrar
    if len(chart_data) > 0:
        # Criar gráfico de barras agrupadas para comparação entre anos
        # Criar pivot table para o gráfico
        df_pivot_anos = chart_data.pivot(index='faixa_personalizada', columns='ano', values='quantidade_aeroportos').fillna(0)
        
        # Ordenar as faixas na ordem correta: Faixa_AvG, Faixa_1, Faixa_2, ..., Faixa_10
        def ordenar_faixas(faixa):
            if faixa == 'Faixa_AvG':
                return (0, 'AvG')
            else:
                # Extrair número da faixa (ex: 'Faixa_5' -> 5)
                numero = int(faixa.split('_')[1])
                return (1, numero)
        
        # Aplicar ordenação personalizada
        faixas_ordenadas = sorted(df_pivot_anos.index, key=ordenar_faixas)
        df_pivot_anos = df_pivot_anos.reindex(faixas_ordenadas)
        
        # Criar gráfico Plotly para barras agrupadas
        fig_anos = go.Figure()
        
        # Adicionar uma série de barras para cada ano
        anos_disponiveis = sorted(df_pivot_anos.columns)
        cores_anos = px.colors.qualitative.Set3[:len(anos_disponiveis)]  # Cores distintas para cada ano
        
        for i, ano in enumerate(anos_disponiveis):
            fig_anos.add_trace(go.Bar(
                x=df_pivot_anos.index,
                y=df_pivot_anos[ano],
                name=f'Ano {ano}',
                marker_color=cores_anos[i],
                            hovertemplate=f'<b>Ano {ano}</b><br>' +
                            'Faixa: %{x}<br>' +
                            'Aeroportos: %{y}<br>' +
                            '<extra></extra>'
            ))
        
        # Configurar layout do gráfico
        fig_anos.update_layout(
            title="Distribuição de Aeroportos por Faixa - Comparação entre Anos",
            xaxis_title="Faixas de Aeroportos",
            yaxis_title="Quantidade de Aeroportos",
            height=500,
            barmode='group',  # Barras agrupadas
            hovermode='x unified',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        # Melhorar aparência do eixo X
        fig_anos.update_xaxes(tickangle=45)
        
        # Mostrar gráfico
        st.plotly_chart(fig_anos, use_container_width=True)
        
    else:
        st.warning("⚠️ Nenhum dado encontrado para o período selecionado.")

    # Tabela resumo compacta
    st.markdown("---")
    with st.expander("📋 **Ver Tabela Detalhada de Distribuição**", expanded=False):
        # Controles de filtro por ano - apenas ano específico
        st.markdown("#### 📅 **Selecione o Ano para Análise Detalhada**")
        
        # Seletor de ano específico (obrigatório)
        anos_disponiveis = sorted(df_com_faixas["ano"].unique().to_list())
        ano_selecionado = st.selectbox(
            "🗓️ **Ano para Análise:**",
            options=anos_disponiveis,
            index=len(anos_disponiveis)-1,  # Último ano por padrão
            help="Selecione o ano para visualizar a distribuição detalhada",
            key="ano_tabela_detalhada"
        )
        
        # Filtrar dados por ano específico selecionado
        df_filtrado_ano = df_com_faixas.filter(pl.col("ano") == ano_selecionado)
        
        # Agregar dados
        distribuicao_por_ano_agg = (df_filtrado_ano
                               .group_by("faixa_personalizada")
                               .agg([
                                   pl.count("aeroporto").alias("quantidade_aeroportos"),
                                   pl.mean("passageiros_projetado").alias("media_passageiros"),
                                   pl.sum("passageiros_projetado").alias("total_passageiros"),
                                   pl.min("passageiros_projetado").alias("min_passageiros"),
                                   pl.max("passageiros_projetado").alias("max_passageiros")
                               ]))
        
        # Adicionar chaves de ordenação e ordenar
        distribuicao_por_ano = (distribuicao_por_ano_agg
                               .with_columns(
                                   pl.when(pl.col("faixa_personalizada") == "Faixa_AvG")
                                   .then(0)
                                   .otherwise(1).alias("sort_key_prefix"),
                                   
                                   pl.when(pl.col("faixa_personalizada") == "Faixa_AvG")
                                   .then(0)
                                   .otherwise(
                                       pl.col("faixa_personalizada").str.extract(r"(\d+)", 1).cast(pl.Int64)
                                   ).alias("sort_key_num")
                               )
                               .sort(["sort_key_prefix", "sort_key_num"])
                               .drop(["sort_key_prefix", "sort_key_num"]))
        
        st.markdown(f"### 📈 **Distribuição para o Ano {ano_selecionado}**")
        
        # Mostrar métricas do ano selecionado
        total_aeroportos_ano = df_filtrado_ano["aeroporto"].n_unique()
        total_passageiros_ano = df_filtrado_ano["passageiros_projetado"].sum()
        
        col_metric1, col_metric2 = st.columns(2)
        with col_metric1:
            st.metric("Aeroportos no Ano", formatar_numero(total_aeroportos_ano))
        with col_metric2:
            st.metric("Total Passageiros (E + D)", formatar_numero(total_passageiros_ano))
        
        # Tabela detalhada por ano
        df_tabela_distribuicao = distribuicao_por_ano.to_pandas()

        # Formatar colunas numéricas para exibição com separador de milhar
        colunas_para_formatar = [
            "quantidade_aeroportos",
            "media_passageiros",
            "total_passageiros",
            "min_passageiros",
            "max_passageiros"
        ]
        
        for col in colunas_para_formatar:
            if col in df_tabela_distribuicao.columns:
                df_tabela_distribuicao[col] = df_tabela_distribuicao[col].apply(formatar_numero)

        st.dataframe(
            df_tabela_distribuicao,
            use_container_width=True,
            column_config={
                "faixa_personalizada": "Faixa",
                "quantidade_aeroportos": "Qtd Aeroportos",
                "media_passageiros": "Média Passageiros (E + D)",
                "total_passageiros": "Total Passageiros (E + D)",
                "min_passageiros": "Mínimo (E + D)",
                "max_passageiros": "Máximo (E + D)"
            }
        )

    # Seção opcional de detalhes por faixa
    with st.expander("🔍 **Explorar Aeroportos por Faixa**", expanded=False):
        st.markdown("#### 🎯 **Análise Detalhada por Faixa e Ano**")
        st.markdown("*Explore os aeroportos de uma faixa específica em um ano determinado*")
        
        # Controles de seleção
        col_explore1, col_explore2 = st.columns(2)
        
        with col_explore1:
            # Seletor de faixa para ver detalhes - usar faixas únicas do DataFrame principal
            faixas_disponiveis = df_com_faixas["faixa_personalizada"].unique().to_list()
            
            # Ordenar faixas na ordem correta: Faixa_AvG, Faixa_1, Faixa_2, ..., Faixa_10
            def ordenar_faixas_explore(faixa):
                if faixa == 'Faixa_AvG':
                    return (0, 'AvG')
                else:
                    try:
                        numero = int(faixa.split('_')[1])
                        return (1, numero)
                    except (ValueError, IndexError):
                        return (2, faixa)
            
            faixas_disponiveis = sorted(faixas_disponiveis, key=ordenar_faixas_explore)
            
            faixa_selecionada = st.selectbox(
                "📊 **Selecione a Faixa:**",
                options=faixas_disponiveis,
                index=0,
                help="Escolha a faixa para análise detalhada",
                key="faixa_explore"
            )
        
        with col_explore2:
            # Seletor de ano específico (obrigatório)
            anos_disponiveis_explore = sorted(df_com_faixas["ano"].unique().to_list())
            ano_selecionado_explore = st.selectbox(
                "🗓️ **Selecione o Ano:**",
                options=anos_disponiveis_explore,
                index=len(anos_disponiveis_explore)-1,  # Último ano por padrão
                help="Selecione o ano específico para análise dos aeroportos",
                key="ano_explore"
            )
        
        # Filtrar aeroportos da faixa e ano selecionados
        aeroportos_faixa = (df_com_faixas
                           .filter(
                               (pl.col("faixa_personalizada") == faixa_selecionada) &
                               (pl.col("ano") == ano_selecionado_explore)
                           )
                           .select(["aeroporto", "passageiros_projetado"])
                           .sort("passageiros_projetado", descending=True))
        
        # Verificar se há dados para a combinação selecionada
        if aeroportos_faixa.height > 0:
            st.markdown(f"### 🏢 **Aeroportos na {faixa_selecionada} - Ano {ano_selecionado_explore}**")
            
            # Estatísticas da faixa selecionada
            total_aeroportos_faixa = aeroportos_faixa.height
            media_passageiros_faixa = aeroportos_faixa["passageiros_projetado"].mean()
            min_passageiros_faixa = aeroportos_faixa["passageiros_projetado"].min()
            max_passageiros_faixa = aeroportos_faixa["passageiros_projetado"].max()
            total_passageiros_faixa = aeroportos_faixa["passageiros_projetado"].sum()
            
            # Métricas em linha
            col_m1, col_m2, col_m3, col_m4, col_m5 = st.columns(5)
            with col_m1:
                st.metric("Total Aeroportos", formatar_numero(total_aeroportos_faixa))
            with col_m2:
                st.metric("Média Passageiros (E + D)", formatar_numero(media_passageiros_faixa))
            with col_m3:
                st.metric("Total Passageiros (E + D)", formatar_numero(total_passageiros_faixa))
            with col_m4:
                st.metric("Mínimo (E + D)", formatar_numero(min_passageiros_faixa))
            with col_m5:
                st.metric("Máximo (E + D)", formatar_numero(max_passageiros_faixa))
            
            # Tabela de aeroportos
            st.markdown("#### 📋 **Lista de Aeroportos**")
            
            # Adicionar ranking aos aeroportos
            aeroportos_com_ranking = aeroportos_faixa.with_row_index("ranking").with_columns([
                (pl.col("ranking") + 1).alias("posicao")
            ]).select(["posicao", "aeroporto", "passageiros_projetado"])
            
            df_aeroportos_ranking = aeroportos_com_ranking.to_pandas()
            
            # Formatar a coluna de passageiros
            if 'passageiros_projetado' in df_aeroportos_ranking.columns:
                df_aeroportos_ranking['passageiros_projetado'] = df_aeroportos_ranking['passageiros_projetado'].apply(lambda x: formatar_numero(x, casas_decimais=0))

            st.dataframe(
                df_aeroportos_ranking,
                use_container_width=True,
                column_config={
                    "posicao": st.column_config.NumberColumn(
                        "#",
                        help="Posição no ranking da faixa",
                        format="%d"
                    ),
                    "aeroporto": st.column_config.TextColumn(
                        "Aeroporto",
                        help="Nome do aeroporto"
                    ),
                    "passageiros_projetado": st.column_config.TextColumn(
                        "Passageiros (E + D)"
                    )
                },
                hide_index=True
            )
            
            # Informações adicionais sobre a faixa
            st.markdown("---")
            col_info1, col_info2 = st.columns(2)
            
            with col_info1:
                # Calcular percentual da faixa no total do ano
                total_ano = df_com_faixas.filter(pl.col("ano") == ano_selecionado_explore)["passageiros_projetado"].sum()
                percentual_faixa = (total_passageiros_faixa / total_ano * 100) if total_ano > 0 else 0
                
                st.info(f"""
                **📊 Participação da {faixa_selecionada} em {ano_selecionado_explore}:**
                - **{percentual_faixa:.1f}%** do total de passageiros (E + D) do ano
                - **{total_aeroportos_faixa}** aeroportos nesta faixa
                """)
            
            with col_info2:
                # Mostrar limites da faixa
                faixa_idx = faixas_utilizadas['labels'].index(faixa_selecionada)
                limite_inferior = faixas_utilizadas['bins'][faixa_idx]
                limite_superior = faixas_utilizadas['bins'][faixa_idx + 1]
                
                if limite_superior == float('inf'):
                    limite_texto = f"{formatar_numero(limite_inferior)}+ passageiros (E + D)"
                else:
                    limite_texto = f"{formatar_numero(limite_inferior)} - {formatar_numero(limite_superior)} passageiros (E + D)"
                
                st.success(f"""
                **🎯 Definição da {faixa_selecionada}:**
                - **Intervalo:** {limite_texto}
                - **Configuração:** {'Personalizada' if usar_faixas_personalizadas else 'Padrão'}
                """)
        
        else:
            st.warning(f"⚠️ **Nenhum aeroporto encontrado na {faixa_selecionada} para o ano {ano_selecionado_explore}.**")
            st.info("💡 Tente selecionar uma faixa ou ano diferente.")

    # Nova Seção: Evolução Temporal de Voos por Aeronave
    st.markdown("---")
    st.header("📈 **Evolução Temporal de Movimentos (P + D) por Aeronave**")

    with st.expander("✈️ **Análise Temporal de Movimentos (P + D) por Aeronave**", expanded=False):
        st.markdown("#### 🛫 **Gráfico de Evolução de Movimentos (P + D)**")
        st.markdown("*Visualize como os movimentos (P + D) de cada aeronave evoluem ao longo do tempo em uma faixa específica*")
        
        # Controles para o gráfico de voos
        col_voos1, col_voos2 = st.columns(2)
        
        with col_voos1:
            # Seletor de faixa para filtrar os aeroportos - usar faixas únicas
            faixas_disponiveis_voos = df_com_faixas["faixa_personalizada"].unique().to_list()
            
            # Ordenar faixas na ordem correta: Faixa_AvG, Faixa_1, Faixa_2, ..., Faixa_10
            def ordenar_faixas(faixa):
                if faixa == 'Faixa_AvG':
                    return (0, 'AvG')
                else:
                    try:
                        numero = int(faixa.split('_')[1])
                        return (1, numero)
                    except (ValueError, IndexError):
                        return (2, faixa)
            
            faixas_disponiveis_voos = sorted(faixas_disponiveis_voos, key=ordenar_faixas)
            
            faixa_selecionada_voos = st.selectbox(
                "📊 **Selecione a Faixa para Análise:**",
                options=faixas_disponiveis_voos,
                index=0,
                help="Escolha a faixa para filtrar os aeroportos e analisar movimentos (P + D)",
                key="faixa_voos"
            )
        
        with col_voos2:
            # Opção para limitar número de aeronaves mostradas
            max_aeronaves = st.slider(
                "🔢 **Máximo de Aeronaves no Gráfico:**",
                min_value=5,
                max_value=53,
                value=10,
                step=1,
                help="Limite o número de aeronaves para melhor visualização",
                key="max_aeronaves"
            )
        
        # Filtrar aeroportos da faixa selecionada
        aeroportos_da_faixa = (df_com_faixas
                              .filter(pl.col("faixa_personalizada") == faixa_selecionada_voos)
                              .select("aeroporto")
                              .unique())
        
        if aeroportos_da_faixa.height > 0:
            # Obter lista de aeroportos da faixa
            lista_aeroportos_faixa = aeroportos_da_faixa["aeroporto"].to_list()
            
            # Filtrar df_filtrado1 pelos aeroportos da faixa selecionada
            df_voos_faixa = df_filtrado1.filter(pl.col("aeroporto").is_in(lista_aeroportos_faixa))
            
            
            
            if df_voos_faixa.height > 0:
                # Criar coluna de período (ano-mês)
                df_voos_periodo = df_voos_faixa.with_columns([
                    (pl.col("ano").cast(pl.Utf8) + "-M" + pl.col("mes").cast(pl.Utf8)).alias("periodo")
                ])
                
                # NOVA LÓGICA: Filtrar por período específico
                # Para cada período, incluir apenas aeroportos que estavam na faixa selecionada naquele período específico
                
                # Obter dados de faixas por período
                faixas_por_periodo = (df_com_faixas
                                    .with_columns([
                                        (pl.col("ano").cast(pl.Utf8) + "-M" + pl.lit("1")).alias("periodo_base")
                                    ])
                                    .select(["aeroporto", "ano", "faixa_personalizada", "periodo_base"]))
                
                # Criar lista de períodos únicos dos dados de voos
                periodos_voos = df_voos_periodo["periodo"].unique().to_list()
                
                # Para cada período, filtrar apenas aeroportos que estavam na faixa naquele ano
                df_voos_filtrado_por_periodo = pl.DataFrame()
                
                for periodo in periodos_voos:
                    # Extrair ano do período (formato: "2022-M1")
                    ano_periodo = int(periodo.split("-M")[0])
                    
                    # Obter aeroportos que estavam na faixa selecionada neste ano específico
                    aeroportos_faixa_ano = (faixas_por_periodo
                                          .filter(
                                              (pl.col("ano") == ano_periodo) &
                                              (pl.col("faixa_personalizada") == faixa_selecionada_voos)
                                          )
                                          .select("aeroporto")
                                          .unique())
                    
                    if aeroportos_faixa_ano.height > 0:
                        # Filtrar voos deste período apenas para aeroportos que estavam na faixa neste ano
                        lista_aeroportos_ano = aeroportos_faixa_ano["aeroporto"].to_list()
                        
                        voos_periodo_filtrado = (df_voos_periodo
                                               .filter(
                                                   (pl.col("periodo") == periodo) &
                                                   (pl.col("aeroporto").is_in(lista_aeroportos_ano))
                                               ))
                        
                        # Adicionar ao DataFrame consolidado
                        if df_voos_filtrado_por_periodo.height == 0:
                            df_voos_filtrado_por_periodo = voos_periodo_filtrado
                        else:
                            df_voos_filtrado_por_periodo = pl.concat([df_voos_filtrado_por_periodo, voos_periodo_filtrado])
                
                # Agregar voos por período e aeronave (agora com filtro correto por período)
                voos_por_periodo_aeronave = (df_voos_filtrado_por_periodo
                                            .group_by(["periodo", "aeronave"])
                                            .agg([
                                                pl.sum("quantidade_voos").alias("total_voos"),
                                                pl.sum("pax").alias("total_passageiros")
                                            ])
                                            .sort(["periodo", "aeronave"]))
                
                
                # Identificar as aeronaves com mais voos para limitar a visualização
                top_aeronaves = (voos_por_periodo_aeronave
                               .group_by("aeronave")
                               .agg([
                                   pl.sum("total_voos").alias("voos_totais"),
                                   pl.sum("total_passageiros").alias("passageiros_totais")
                               ])
                               .sort("voos_totais", "passageiros_totais", "aeronave", descending=[True, True, False])
                               .head(max_aeronaves))
                
                # Filtrar apenas as top aeronaves
                df_grafico_voos = voos_por_periodo_aeronave.filter(
                    pl.col("aeronave").is_in(top_aeronaves["aeronave"].to_list())
                )
                
                # Converter para pandas para o gráfico
                df_pandas_voos = df_grafico_voos.to_pandas()
                
                # Criar pivot table para o gráfico de linhas
                df_pivot_voos = df_pandas_voos.pivot(index='periodo', columns='aeronave', values='total_voos').fillna(0)
                
                # Ordenar os períodos corretamente
                periodos_ordenados = sorted(df_pivot_voos.index, key=lambda x: (int(x.split('-')[0]), int(x.split('-M')[1])))
                df_pivot_voos = df_pivot_voos.reindex(periodos_ordenados)
                
                # Mostrar informações sobre a análise
                st.markdown(f"### 📊 **Evolução de Movimentos (P + D) - {faixa_selecionada_voos}**")
                
                # Gráfico de linhas
                st.markdown("#### 📈 **Gráfico de Evolução Temporal (P + D)**")
                
                if len(df_pivot_voos) > 0:
                    # Obter todas as aeronaves disponíveis nos dados para consistência de cores
                    todas_aeronaves_disponiveis = df_filtrado1["aeronave"].unique().to_list()
                    
                    # Criar gráfico Plotly com cores consistentes
                    fig = go.Figure()
                    
                    # Adicionar uma linha para cada aeronave com cor consistente
                    for aeronave in df_pivot_voos.columns:
                        cor_aeronave = obter_cor_aeronave(aeronave, todas_aeronaves_disponiveis)
                        
                        hover_texts = [formatar_numero(y) for y in df_pivot_voos[aeronave]]

                        fig.add_trace(go.Scatter(
                            x=df_pivot_voos.index,
                            y=df_pivot_voos[aeronave],
                            mode='lines+markers',
                            name=aeronave,
                            line=dict(color=cor_aeronave, width=2),
                            marker=dict(color=cor_aeronave, size=6),
                            text=hover_texts,
                            hovertemplate=f'<b>{aeronave}</b><br>' +
                                        'Período: %{x}<br>' +
                                        'Movimentos (P + D): %{text}<br>' +
                                        '<extra></extra>'
                        ))
                    
                    # Configurar layout do gráfico
                    fig.update_layout(
                        title=f"Evolução Temporal de Movimentos (P + D) - {faixa_selecionada_voos}",
                        xaxis_title="Período (Ano-Mês)",
                        yaxis_title="Quantidade de Movimentos (P + D)",
                        height=500,
                        hovermode='x unified',
                        legend=dict(
                            orientation="v",
                            yanchor="top",
                            y=1,
                            xanchor="left",
                            x=1.02
                        ),
                        margin=dict(r=150)  # Espaço para legenda
                    )
                    
                    # Melhorar aparência do eixo X
                    fig.update_xaxes(
                        tickangle=45,
                        tickmode='linear'
                    )
                    
                    # Mostrar gráfico
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Tabela resumo dos dados
                    st.markdown("---")
                    st.markdown("#### 📋 **Dados Detalhados**")
                    
                    # Checkbox para mostrar/ocultar tabela detalhada
                    mostrar_tabela = st.checkbox(
                        "📊 **Mostrar Tabela de Dados Completa**", 
                        value=False,
                        help="Marque para ver os dados em formato tabular",
                        key="mostrar_tabela_voos"
                    )
                    
                    if mostrar_tabela:
                        # Resetar index para mostrar período como coluna
                        df_tabela_voos = df_pivot_voos.reset_index()

                        # Formatar as colunas de aeronaves
                        for col in df_pivot_voos.columns:
                            df_tabela_voos[col] = df_tabela_voos[col].apply(lambda x: formatar_numero(x, casas_decimais=0))
                        
                        st.markdown(f"#### 📋 **Dados Detalhados - {faixa_selecionada_voos}**")
                        
                        st.dataframe(
                            df_tabela_voos,
                            use_container_width=True,
                            column_config={
                                "periodo": st.column_config.TextColumn(
                                    "Período",
                                    help="Período no formato Ano-Mês"
                                )
                            }
                        )
                    
                    # Análise adicional
                    st.markdown("---")
                    col_analise1, col_analise2 = st.columns(2)
                    
                    with col_analise1:
                        # Aeronave com mais voos no período
                        aeronave_top = top_aeronaves.row(0, named=True)
                        st.success(f"""
                        **🏆 Aeronave Líder na {faixa_selecionada_voos}:**
                        - **{aeronave_top['aeronave']}**
                        - **{formatar_numero(aeronave_top['voos_totais'])}** movimentos (P + D)
                        """)
                    
                    with col_analise2:
                        # Período com mais voos
                        voos_por_periodo_total = df_pandas_voos.groupby('periodo')['total_voos'].sum()
                        periodo_pico = voos_por_periodo_total.idxmax()
                        voos_pico = voos_por_periodo_total.max()
                        
                        st.info(f"""
                        **📈 Período de Pico na {faixa_selecionada_voos}:**
                        - **{periodo_pico}**
                        - **{formatar_numero(voos_pico)}** movimentos (P + D)
                        """)
                    
                    # Nova seção: Tabela de Passageiros por Aeronave (Pivot)
                    st.markdown("---")
                    st.markdown("#### 👥 **Tabela de Passageiros (E + D) por Aeronave**")
                    st.markdown(f"*Matriz mostrando passageiros (E + D) por aeronave e período na **{faixa_selecionada_voos}***")
        
                    
                    # Usar passageiros reais do DataFrame
                    df_passageiros_estimados = df_grafico_voos.select(
                        ["periodo", "aeronave", pl.col("total_passageiros").alias("passageiros_estimados")]
                    )
                    
                    # Criar pivot table: períodos como linhas, aeronaves como colunas
                    df_pivot_passageiros = df_passageiros_estimados.to_pandas().pivot(
                        index='periodo', 
                        columns='aeronave', 
                        values='passageiros_estimados'
                    ).fillna(0)
                    
                    # Ordenar os períodos corretamente
                    periodos_ordenados_passageiros = sorted(df_pivot_passageiros.index, key=lambda x: (int(x.split('-')[0]), int(x.split('-M')[1])))
                    df_pivot_passageiros = df_pivot_passageiros.reindex(periodos_ordenados_passageiros)
                    
                    # Verificar se há dados para mostrar
                    if len(df_pivot_passageiros) > 0 and len(df_pivot_passageiros.columns) > 0:
                        st.markdown(f"### 👥 **Passageiros (E + D) Estimados por Aeronave - {faixa_selecionada_voos}**")
                        
                        # Métricas gerais da tabela
                        total_passageiros_geral = df_pivot_passageiros.sum().sum()
                        total_periodos = len(df_pivot_passageiros)
                        total_aeronaves = len(df_pivot_passageiros.columns)
                        media_passageiros_por_periodo = df_pivot_passageiros.sum(axis=1).mean()
                        
                        col_metric_geral1, col_metric_geral2, col_metric_geral3, col_metric_geral4 = st.columns(4)
                        with col_metric_geral1:
                            st.metric("Total de Passageiros (E + D)", formatar_numero(total_passageiros_geral))
                        with col_metric_geral2:
                            st.metric("Períodos", formatar_numero(total_periodos))
                        with col_metric_geral3:
                            st.metric("Aeronaves", formatar_numero(total_aeronaves))
                        with col_metric_geral4:
                            st.metric("Média por Período", formatar_numero(media_passageiros_por_periodo))
                        
                        
                        # Tabela pivot de passageiros
                        st.markdown("#### 📋 **Matriz: Passageiros (E + D) por Aeronave e Período**")
                        
                        # Resetar index para mostrar período como coluna
                        df_tabela_passageiros = df_pivot_passageiros.reset_index()

                        # Formatar as colunas de aeronaves
                        for aeronave_col in df_pivot_passageiros.columns:
                            df_tabela_passageiros[aeronave_col] = df_tabela_passageiros[aeronave_col].apply(lambda x: formatar_numero(x, casas_decimais=0))
                        
                        # Configurar colunas da tabela
                        column_config = {
                            "periodo": st.column_config.TextColumn(
                                "Período",
                                help="Período no formato Ano-Mês",
                                width="medium"
                            )
                        }
                        
                        # Adicionar configuração para cada aeronave (agora como texto)
                        for aeronave in df_pivot_passageiros.columns:
                            column_config[aeronave] = st.column_config.TextColumn(
                                aeronave,
                                help=f"Passageiros (E + D) estimados da aeronave {aeronave}",
                                width="small"
                            )
                        
                        st.dataframe(
                            df_tabela_passageiros,
                            use_container_width=True,
                            column_config=column_config,
                            hide_index=True
                        )
                        
                        # Análise adicional da tabela
                        st.markdown("---")
                        col_analise_geral1, col_analise_geral2 = st.columns(2)
                        
                        with col_analise_geral1:
                            # Aeronave com mais passageiros totais
                            total_por_aeronave = df_pivot_passageiros.sum().sort_values(ascending=False)
                            aeronave_lider = total_por_aeronave.index[0]
                            passageiros_lider = total_por_aeronave.iloc[0]
                            
                            st.success(f"""
                            **🏆 Aeronave Líder em Passageiros (E + D):**
                            - **{aeronave_lider}**
                            - **{formatar_numero(passageiros_lider)}** passageiros (E + D)
                            """)
                        
                        with col_analise_geral2:
                            # Período com mais passageiros
                            total_por_periodo = df_pivot_passageiros.sum(axis=1).sort_values(ascending=False)
                            periodo_pico = total_por_periodo.index[0]
                            passageiros_pico = total_por_periodo.iloc[0]
                            
                            st.info(f"""
                            **📈 Período de Pico em Passageiros (E + D):**
                            - **{periodo_pico}**
                            - **{formatar_numero(passageiros_pico)}** passageiros (E + D)
                            """)
                        
                        # Gráfico de barras para visualização
                        st.markdown("---")
                        st.markdown("#### 📊 **Gráfico de Passageiros (E + D) por Período**")
                        
                        # Criar gráfico de barras empilhadas
                        fig_passageiros = go.Figure()
                        
                        # Obter todas as aeronaves disponíveis nos dados para consistência de cores
                        todas_aeronaves_disponiveis_passageiros = df_filtrado1["aeronave"].unique().to_list()
                        
                        # Adicionar uma série de barras para cada aeronave com cor consistente
                        for aeronave in df_pivot_passageiros.columns:
                            cor_aeronave = obter_cor_aeronave(aeronave, todas_aeronaves_disponiveis_passageiros)
                            
                            hover_texts_passageiros = [formatar_numero(y) for y in df_pivot_passageiros[aeronave]]
                            
                            fig_passageiros.add_trace(go.Bar(
                                x=df_pivot_passageiros.index,
                                y=df_pivot_passageiros[aeronave],
                                name=aeronave,
                                marker_color=cor_aeronave,
                                customdata=hover_texts_passageiros,
                                hovertemplate=f'<b>{aeronave}</b><br>' +
                                            'Período: %{x}<br>' +
                                            'Passageiros (E + D): %{customdata}<br>' +
                                            '<extra></extra>'
                            ))
                        
                        # Configurar layout do gráfico
                        fig_passageiros.update_layout(
                            title=f"Passageiros (E + D) por Aeronave - {faixa_selecionada_voos}",
                            xaxis_title="Período (Ano-Mês)",
                            yaxis_title="Passageiros (E + D)",
                            height=500,
                            barmode='stack',  # Barras empilhadas
                            hovermode='x unified',
                            legend=dict(
                                orientation="v",
                                yanchor="top",
                                y=1,
                                xanchor="left",
                                x=1.02
                            ),
                            margin=dict(r=150)  # Espaço para legenda
                        )
                        
                        # Melhorar aparência do eixo X
                        fig_passageiros.update_xaxes(
                            tickangle=45,
                            tickmode='linear'
                        )
                        
                        # Melhorar aparência do eixo Y
                        fig_passageiros.update_yaxes()
                        
                        # Mostrar gráfico
                        st.plotly_chart(fig_passageiros, use_container_width=True)
                    
                    else:
                        st.warning(f"⚠️ **Nenhum dado de passageiros (E + D) encontrado para a {faixa_selecionada_voos}.**")
                        st.info("💡 Verifique os filtros aplicados ou tente uma faixa diferente.")
                    
                    # Nova seção: Tabela de Detalhamento por Aeronave
                    st.markdown("---")
                    st.markdown("#### 🔍 **Detalhamento por Aeronave**")
                    st.markdown(f"*Selecione uma aeronave e período para ver os aeroportos e quantidade de movimentos (P + D) detalhados na **{faixa_selecionada_voos}***")
                    
                    # Controles para a tabela de detalhamento
                    col_detalhe1, col_detalhe2 = st.columns(2)
                    
                    with col_detalhe1:
                        # Seletor de aeronave para detalhamento
                        aeronaves_disponiveis = sorted(df_grafico_voos["aeronave"].unique().to_list())
                        aeronave_selecionada_detalhe = st.selectbox(
                            "✈️ **Selecione a Aeronave:**",
                            options=aeronaves_disponiveis,
                            index=0,
                            help="Escolha a aeronave para ver o detalhamento por aeroporto",
                            key="aeronave_detalhe"
                        )
                    
                    with col_detalhe2:
                        # Seletor de período para detalhamento - ordenar cronologicamente
                        periodos_unicos = df_grafico_voos["periodo"].unique().to_list()
                        
                        # Função para ordenar períodos cronologicamente
                        def ordenar_periodos_cronologicamente(periodos):
                            def chave_ordenacao(periodo):
                                ano, mes = periodo.split("-M")
                                return (int(ano), int(mes))
                            return sorted(periodos, key=chave_ordenacao)
                        
                        periodos_disponiveis = ordenar_periodos_cronologicamente(periodos_unicos)
                        periodo_selecionado_detalhe = st.selectbox(
                            "📅 **Selecione o Período:**",
                            options=periodos_disponiveis,
                            index=len(periodos_disponiveis)-1,  # Último período por padrão
                            help="Escolha o período para ver o detalhamento",
                            key="periodo_detalhe"
                        )
                    
                    # Filtrar dados para a aeronave e período selecionados
                    # NOVA LÓGICA: Usar o mesmo filtro por período específico
                    # Extrair ano do período selecionado
                    ano_periodo_detalhe = int(periodo_selecionado_detalhe.split("-M")[0])
                    
                    # Obter aeroportos que estavam na faixa selecionada neste ano específico
                    aeroportos_faixa_ano_detalhe = (df_com_faixas
                                                  .filter(
                                                      (pl.col("ano") == ano_periodo_detalhe) &
                                                      (pl.col("faixa_personalizada") == faixa_selecionada_voos)
                                                  )
                                                  .select("aeroporto")
                                                  .unique())
                    
                    if aeroportos_faixa_ano_detalhe.height > 0:
                        lista_aeroportos_ano_detalhe = aeroportos_faixa_ano_detalhe["aeroporto"].to_list()
                        
                        # Filtrar dados usando o filtro correto por período específico
                        df_detalhe_aeronave = (df_voos_filtrado_por_periodo
                                             .filter(
                                                 (pl.col("aeronave") == aeronave_selecionada_detalhe) &
                                                 (pl.col("periodo") == periodo_selecionado_detalhe)
                                             )
                                             .select(["aeroporto", "quantidade_voos", pl.col("pax").alias("passageiros_estimados")])
                                             .sort("quantidade_voos", descending=True))
                    else:
                        # Se não há aeroportos na faixa neste ano, criar DataFrame vazio
                        df_detalhe_aeronave = pl.DataFrame({"aeroporto": [], "quantidade_voos": [], "passageiros_estimados": []})
                    
                    # Verificar se há dados para mostrar
                    if df_detalhe_aeronave.height > 0:
                        st.markdown(f"### 📊 **Detalhamento: {aeronave_selecionada_detalhe} - {periodo_selecionado_detalhe} - {faixa_selecionada_voos}**")
                        
                        # Métricas do detalhamento
                        total_voos_aeronave = df_detalhe_aeronave["quantidade_voos"].sum()
                        total_passageiros_estimados = df_detalhe_aeronave["passageiros_estimados"].sum()
                        total_aeroportos_aeronave = df_detalhe_aeronave.height
                        media_voos_por_aeroporto = df_detalhe_aeronave["quantidade_voos"].mean()
                        
                        col_metric_det1, col_metric_det2, col_metric_det3, col_metric_det4 = st.columns(4)
                        with col_metric_det1:
                            st.metric("Total Movimentos (P + D)", formatar_numero(total_voos_aeronave))
                        with col_metric_det2:
                            st.metric("Total Passageiros (E + D)", formatar_numero(total_passageiros_estimados))
                        with col_metric_det3:
                            st.metric("Aeroportos", formatar_numero(total_aeroportos_aeronave))
                        with col_metric_det4:
                            st.metric("Média Movimentos/Aeroporto", formatar_numero(media_voos_por_aeroporto))
                        
                        # Tabela de detalhamento
                        st.markdown("#### 📋 **Aeroportos, Movimentos e Passageiros (E + D)**")
                        
                        # Adicionar ranking aos aeroportos
                        df_detalhe_com_ranking = df_detalhe_aeronave.with_row_index("ranking").with_columns([
                            (pl.col("ranking") + 1).alias("posicao")
                        ]).select(["posicao", "aeroporto", "quantidade_voos", "passageiros_estimados"])

                        df_detalhe_pandas = df_detalhe_com_ranking.to_pandas()

                        # Formatar a coluna de voos
                        if 'quantidade_voos' in df_detalhe_pandas.columns:
                            df_detalhe_pandas['quantidade_voos'] = df_detalhe_pandas['quantidade_voos'].apply(lambda x: formatar_numero(x, casas_decimais=0))
                        if 'passageiros_estimados' in df_detalhe_pandas.columns:
                            df_detalhe_pandas['passageiros_estimados'] = df_detalhe_pandas['passageiros_estimados'].apply(lambda x: formatar_numero(x, casas_decimais=0))
                        
                        st.dataframe(
                            df_detalhe_pandas,
                            use_container_width=True,
                            column_config={
                                "posicao": st.column_config.NumberColumn(
                                    "#",
                                    help="Posição no ranking de movimentos (P + D)",
                                    format="%d"
                                ),
                                "aeroporto": st.column_config.TextColumn(
                                    "Aeroporto",
                                    help="Nome do aeroporto"
                                ),
                                "quantidade_voos": st.column_config.TextColumn(
                                    "Movimentos (P + D)",
                                    help="Número de movimentos (P + D) da aeronave neste aeroporto"
                                ),
                                "passageiros_estimados": st.column_config.TextColumn(
                                    "Passageiros (E + D)",
                                    help="Número de passageiros estimados (E + D) da aeronave neste aeroporto"
                                )
                            },
                            hide_index=True
                        )
                        
                        # Análise adicional do detalhamento
                        st.markdown("---")
                        col_analise_det1, col_analise_det2 = st.columns(2)
                        
                        with col_analise_det1:
                            # Aeroporto com mais voos
                            aeroporto_top = df_detalhe_aeronave.row(0, named=True)
                            st.success(f"""
                            **🏆 Aeroporto Líder:**
                            - **{aeroporto_top['aeroporto']}**
                            - **{formatar_numero(aeroporto_top['quantidade_voos'])}** movimentos (P + D)
                            """)
                        
                        with col_analise_det2:
                            # Percentual de aeroportos com voos (usando a lista correta para o período específico)
                            total_aeroportos_faixa_periodo = len(lista_aeroportos_ano_detalhe)
                            percentual_aeroportos_com_voos = (total_aeroportos_aeronave / total_aeroportos_faixa_periodo * 100) if total_aeroportos_faixa_periodo > 0 else 0
                            
                            st.info(f"""
                            **📊 Cobertura da Aeronave na {faixa_selecionada_voos} ({periodo_selecionado_detalhe}):**
                            - **{percentual_aeroportos_com_voos:.1f}%** dos aeroportos da faixa neste período
                            - **{total_aeroportos_aeronave}** de **{total_aeroportos_faixa_periodo}** aeroportos
                            """)
                    
                    else:
                        st.warning(f"⚠️ **Nenhum voo encontrado para a aeronave {aeronave_selecionada_detalhe} no período {periodo_selecionado_detalhe}.**")
                        st.info("💡 Tente selecionar uma aeronave ou período diferente.")
                    
                
                else:
                    st.warning("⚠️ Não há dados suficientes para gerar o gráfico.")
            
            else:
                st.warning(f"⚠️ **Nenhum voo encontrado para aeroportos da {faixa_selecionada_voos}.**")
                st.info("💡 Verifique os filtros aplicados ou tente uma faixa diferente.")
        
        else:
            st.warning(f"⚠️ **Nenhum aeroporto encontrado na {faixa_selecionada_voos}.**")
            st.info("💡 Tente selecionar uma faixa diferente.")

    # Nova Seção: Percentual de Aeroportos por Aeronave
    st.markdown("---")
    st.header("📊 **Percentual de Aeroportos por Aeronave**")

    with st.expander("✈️ **Análise de Utilização de Aeronaves por Faixa**", expanded=False):
        st.markdown("#### 📈 **Gráfico de Percentual de Aeroportos**")
        st.markdown("*Visualize o percentual de aeroportos que utilizam cada aeronave ao longo do tempo em uma faixa específica*")
        
        # Controles para o gráfico de percentuais
        col_perc1, col_perc2 = st.columns(2)
        
        with col_perc1:
            # Seletor de faixa para análise de percentual - usar faixas únicas
            faixas_disponiveis_perc = df_com_faixas["faixa_personalizada"].unique().to_list()
            
            # Ordenar faixas na ordem correta: Faixa_AvG, Faixa_1, Faixa_2, ..., Faixa_10
            def ordenar_faixas_perc(faixa):
                if faixa == 'Faixa_AvG':
                    return (0, 'AvG')
                else:
                    try:
                        numero = int(faixa.split('_')[1])
                        return (1, numero)
                    except (ValueError, IndexError):
                        return (2, faixa)
            
            faixas_disponiveis_perc = sorted(faixas_disponiveis_perc, key=ordenar_faixas_perc)
            
            faixa_selecionada_perc = st.selectbox(
                "📊 **Selecione a Faixa para Análise:**",
                options=faixas_disponiveis_perc,
                index=0,
                help="Escolha a faixa para analisar o percentual de aeroportos por aeronave",
                key="faixa_percentual"
            )
        
        with col_perc2:
            # Opção para limitar número de aeronaves mostradas
            max_aeronaves_perc = st.slider(
                "🔢 **Máximo de Aeronaves no Gráfico:**",
                min_value=5,
                max_value=53,
                value=8,
                step=1,
                help="Limite o número de aeronaves para melhor visualização",
                key="max_aeronaves_perc"
            )
        
        # Filtrar aeroportos da faixa selecionada
        aeroportos_da_faixa_perc = (df_com_faixas
                                   .filter(pl.col("faixa_personalizada") == faixa_selecionada_perc)
                                   .select("aeroporto")
                                   .unique())
        
        if aeroportos_da_faixa_perc.height > 0:
            # Obter lista de aeroportos da faixa
            lista_aeroportos_faixa_perc = aeroportos_da_faixa_perc["aeroporto"].to_list()
            
            # Filtrar df_filtrado1 pelos aeroportos da faixa selecionada
            df_voos_faixa_perc = df_filtrado1.filter(pl.col("aeroporto").is_in(lista_aeroportos_faixa_perc))
            
            if df_voos_faixa_perc.height > 0:
                # Criar coluna de período (ano-mês)
                df_voos_periodo_perc = df_voos_faixa_perc.with_columns([
                    (pl.col("ano").cast(pl.Utf8) + "-M" + pl.col("mes").cast(pl.Utf8)).alias("periodo")
                ])
                
                # NOVA LÓGICA: Filtrar por período específico
                # Para cada período, incluir apenas aeroportos que estavam na faixa selecionada naquele período específico
                
                # Obter dados de faixas por período
                faixas_por_periodo_perc = (df_com_faixas
                                         .with_columns([
                                             (pl.col("ano").cast(pl.Utf8) + "-M" + pl.lit("1")).alias("periodo_base")
                                         ])
                                         .select(["aeroporto", "ano", "faixa_personalizada", "periodo_base"]))
                
                # Criar lista de períodos únicos dos dados de voos
                periodos_voos_perc = df_voos_periodo_perc["periodo"].unique().to_list()
                
                # Para cada período, filtrar apenas aeroportos que estavam na faixa naquele ano
                df_voos_filtrado_por_periodo_perc = pl.DataFrame()
                
                for periodo in periodos_voos_perc:
                    # Extrair ano do período (formato: "2022-M1")
                    ano_periodo = int(periodo.split("-M")[0])
                    
                    # Obter aeroportos que estavam na faixa selecionada neste ano específico
                    aeroportos_faixa_ano_perc = (faixas_por_periodo_perc
                                               .filter(
                                                   (pl.col("ano") == ano_periodo) &
                                                   (pl.col("faixa_personalizada") == faixa_selecionada_perc)
                                               )
                                               .select("aeroporto")
                                               .unique())
                    
                    if aeroportos_faixa_ano_perc.height > 0:
                        # Filtrar voos deste período apenas para aeroportos que estavam na faixa neste ano
                        lista_aeroportos_ano_perc = aeroportos_faixa_ano_perc["aeroporto"].to_list()
                        
                        voos_periodo_filtrado_perc = (df_voos_periodo_perc
                                                    .filter(
                                                        (pl.col("periodo") == periodo) &
                                                        (pl.col("aeroporto").is_in(lista_aeroportos_ano_perc))
                                                    ))
                        
                        # Adicionar ao DataFrame consolidado
                        if df_voos_filtrado_por_periodo_perc.height == 0:
                            df_voos_filtrado_por_periodo_perc = voos_periodo_filtrado_perc
                        else:
                            df_voos_filtrado_por_periodo_perc = pl.concat([df_voos_filtrado_por_periodo_perc, voos_periodo_filtrado_perc])
                
                # Calcular total de aeroportos únicos por período na faixa (agora com filtro correto)
                total_aeroportos_por_periodo = (df_voos_filtrado_por_periodo_perc
                                              .group_by("periodo")
                                              .agg([
                                                  pl.n_unique("aeroporto").alias("total_aeroportos")
                                              ]))
                
                # Calcular aeroportos únicos que usam cada aeronave por período (agora com filtro correto)
                aeroportos_por_aeronave_periodo = (df_voos_filtrado_por_periodo_perc
                                                 .filter(pl.col("quantidade_voos") > 0)  # Apenas aeroportos com voos
                                                 .group_by(["periodo", "aeronave"])
                                                 .agg([
                                                     pl.n_unique("aeroporto").alias("aeroportos_usando")
                                                 ]))
                
                # Juntar com total de aeroportos para calcular percentual
                percentual_por_aeronave = (aeroportos_por_aeronave_periodo
                                         .join(total_aeroportos_por_periodo, on="periodo")
                                         .with_columns([
                                             (pl.col("aeroportos_usando") / pl.col("total_aeroportos") * 100).alias("percentual")
                                         ])
                                         .sort(["periodo", "aeronave"]))
                
                # Identificar as aeronaves com maior utilização média para limitar visualização
                utilizacao_media = (percentual_por_aeronave
                                  .group_by("aeronave")
                                  .agg([
                                      pl.mean("percentual").alias("utilizacao_media")
                                  ])
                                  .sort("utilizacao_media", "aeronave", descending=[True, False])
                                  .head(max_aeronaves_perc))
                
                # Filtrar apenas as top aeronaves
                df_grafico_perc = percentual_por_aeronave.filter(
                    pl.col("aeronave").is_in(utilizacao_media["aeronave"].to_list())
                )
                
                # Converter para pandas para o gráfico
                df_pandas_perc = df_grafico_perc.to_pandas()
                
                # Criar pivot table para o gráfico
                df_pivot_perc = df_pandas_perc.pivot(index='periodo', columns='aeronave', values='percentual').fillna(0)
                
                # Ordenar os períodos corretamente
                periodos_ordenados_perc = sorted(df_pivot_perc.index, key=lambda x: (int(x.split('-')[0]), int(x.split('-M')[1])))
                df_pivot_perc = df_pivot_perc.reindex(periodos_ordenados_perc)
                
                # Mostrar informações sobre a análise
                st.markdown(f"### 📊 **Percentual de Aeroportos por Aeronave - {faixa_selecionada_perc}**")
                
                # Métricas da análise - Aeroportos por ano na faixa
                utilizacao_maxima = df_pandas_perc['percentual'].max()
                
                # Calcular aeroportos únicos por ano na faixa selecionada (usando dados de faixas diretamente)
                aeroportos_por_ano_faixa = (df_com_faixas
                                           .filter(pl.col("faixa_personalizada") == faixa_selecionada_perc)
                                           .group_by("ano")
                                           .agg([
                                               pl.n_unique("aeroporto").alias("total_aeroportos")
                                           ])
                                           .sort("ano"))
                
                # Mostrar métricas por ano
                st.markdown("#### 📊 **Aeroportos na Faixa por Ano:**")
                aeroportos_anos_data = aeroportos_por_ano_faixa.to_pandas()
                
                # Criar colunas dinamicamente baseado no número de anos
                num_anos = len(aeroportos_anos_data)
                if num_anos > 0:
                    cols_anos = st.columns(min(num_anos, 5))  # Máximo 5 colunas
                    
                    for i, row in aeroportos_anos_data.iterrows():
                        col_idx = i % len(cols_anos)
                        with cols_anos[col_idx]:
                            st.metric(
                                f"Ano {int(row['ano'])}", 
                                f"{formatar_numero(int(row['total_aeroportos']))} aeroportos"
                            )
                
                # Gráfico de barras
                st.markdown("#### 📈 **Percentual de Aeroportos por Aeronave**")
                
                if len(df_pivot_perc) > 0:
                    # Obter todas as aeronaves disponíveis nos dados para consistência de cores
                    todas_aeronaves_disponiveis_perc = df_filtrado1["aeronave"].unique().to_list()
                    
                    # Criar gráfico Plotly com cores consistentes
                    fig_perc = go.Figure()
                    
                    # Adicionar uma série de barras para cada aeronave com cor consistente
                    for aeronave in df_pivot_perc.columns:
                        cor_aeronave = obter_cor_aeronave(aeronave, todas_aeronaves_disponiveis_perc)
                        
                        hover_texts_perc = [formatar_numero(y) for y in df_pivot_perc[aeronave]]
                        
                        fig_perc.add_trace(go.Bar(
                            x=df_pivot_perc.index,
                            y=df_pivot_perc[aeronave],
                            name=aeronave,
                            marker_color=cor_aeronave,
                            customdata=hover_texts_perc,
                            hovertemplate=f'<b>{aeronave}</b><br>' +
                                        'Período: %{x}<br>' +
                                        'Percentual: %{y:.1f}%<br>' +
                                        '<extra></extra>'
                        ))
                    
                    # Configurar layout do gráfico
                    fig_perc.update_layout(
                        title=f"Percentual de Aeroportos por Aeronave - {faixa_selecionada_perc}",
                        xaxis_title="Período (Ano-Mês)",
                        yaxis_title="Percentual de Aeroportos (%)",
                        height=500,
                        barmode='group',  # Barras agrupadas
                        hovermode='x unified',
                        legend=dict(
                            orientation="v",
                            yanchor="top",
                            y=1,
                            xanchor="left",
                            x=1.02
                        ),
                        margin=dict(r=150)  # Espaço para legenda
                    )
                    
                    # Melhorar aparência do eixo X
                    fig_perc.update_xaxes(
                        tickangle=45,
                        tickmode='linear'
                    )
                    
                    # Configurar eixo Y para mostrar percentual
                    fig_perc.update_yaxes(
                        ticksuffix="%",
                        range=[0, min(100, utilizacao_maxima * 1.1)]  # Ajustar escala
                    )
                    
                    # Adicionar anotações com total de aeroportos por período na base do gráfico
                    total_aeroportos_pandas = total_aeroportos_por_periodo.to_pandas().set_index('periodo')
                    
                    for i, periodo in enumerate(df_pivot_perc.index):
                        if periodo in total_aeroportos_pandas.index:
                            total_aeroportos_periodo = total_aeroportos_pandas.loc[periodo, 'total_aeroportos']
                            
                            fig_perc.add_annotation(
                                x=periodo,
                                y=-utilizacao_maxima * 0.15,  # Posição abaixo do eixo X
                                text=f"Total: {total_aeroportos_periodo} aeroportos",
                                showarrow=False,
                                font=dict(size=10, color="gray"),
                                xanchor="center",
                                yanchor="top"
                            )
                    
                    # Ajustar margem inferior para acomodar as anotações
                    fig_perc.update_layout(
                        margin=dict(r=150, b=80)  # Aumentar margem inferior
                    )
                    
                    # Mostrar gráfico
                    st.plotly_chart(fig_perc, use_container_width=True)
                    
                    # Análise adicional
                    st.markdown("---")
                    col_analise_perc1, col_analise_perc2 = st.columns(2)
                    
                    with col_analise_perc1:
                        # Aeronave com maior utilização média
                        aeronave_top_perc = utilizacao_media.row(0, named=True)
                        st.success(f"""
                        **🏆 Aeronave Mais Utilizada na {faixa_selecionada_perc}:**
                        - **{aeronave_top_perc['aeronave']}**
                        - **{aeronave_top_perc['utilizacao_media']:.1f}%** utilização média
                        """)
                    
                    with col_analise_perc2:
                        # Período com maior diversidade de aeronaves
                        diversidade_por_periodo = df_pandas_perc.groupby('periodo')['aeronave'].nunique()
                        periodo_mais_diverso = diversidade_por_periodo.idxmax()
                        max_diversidade = diversidade_por_periodo.max()
                        
                        st.info(f"""
                        **📈 Período com Maior Diversidade na {faixa_selecionada_perc}:**
                        - **{periodo_mais_diverso}**
                        - **{max_diversidade}** aeronaves diferentes
                        """)
                    
                    # Nova seção: Tabela Detalhada de Aeroportos por Aeronave
                    st.markdown("---")
                    st.markdown("#### 🔍 **Detalhamento de Aeroportos por Aeronave**")
                    st.markdown(f"*Selecione uma aeronave e período para ver os aeroportos e quantidade de movimentos (P + D) detalhados na **{faixa_selecionada_perc}***")
                    
                    # Controles para a tabela de detalhamento
                    col_detalhe_perc1, col_detalhe_perc2 = st.columns(2)
                    
                    with col_detalhe_perc1:
                        # Seletor de aeronave para detalhamento
                        aeronaves_disponiveis_perc = sorted(df_grafico_perc["aeronave"].unique().to_list())
                        aeronave_selecionada_detalhe_perc = st.selectbox(
                            "✈️ **Selecione a Aeronave:**",
                            options=aeronaves_disponiveis_perc,
                            index=0,
                            help="Escolha a aeronave para ver o detalhamento por aeroporto",
                            key="aeronave_detalhe_perc"
                        )
                    
                    with col_detalhe_perc2:
                        # Seletor de período para detalhamento - ordenar cronologicamente
                        periodos_unicos_perc = df_grafico_perc["periodo"].unique().to_list()
                        
                        # Função para ordenar períodos cronologicamente
                        def ordenar_periodos_cronologicamente_perc(periodos):
                            def chave_ordenacao(periodo):
                                ano, mes = periodo.split("-M")
                                return (int(ano), int(mes))
                            return sorted(periodos, key=chave_ordenacao)
                        
                        periodos_disponiveis_perc = ordenar_periodos_cronologicamente_perc(periodos_unicos_perc)
                        periodo_selecionado_detalhe_perc = st.selectbox(
                            "📅 **Selecione o Período:**",
                            options=periodos_disponiveis_perc,
                            index=len(periodos_disponiveis_perc)-1,  # Último período por padrão
                            help="Escolha o período para ver o detalhamento",
                            key="periodo_detalhe_perc"
                        )
                    
                    # Filtrar dados para a aeronave e período selecionados
                    # NOVA LÓGICA: Usar o mesmo filtro por período específico
                    # Extrair ano do período selecionado
                    ano_periodo_detalhe_perc = int(periodo_selecionado_detalhe_perc.split("-M")[0])
                    
                    # Obter aeroportos que estavam na faixa selecionada neste ano específico
                    aeroportos_faixa_ano_detalhe_perc = (df_com_faixas
                                                       .filter(
                                                           (pl.col("ano") == ano_periodo_detalhe_perc) &
                                                           (pl.col("faixa_personalizada") == faixa_selecionada_perc)
                                                       )
                                                       .select("aeroporto")
                                                       .unique())
                    
                    if aeroportos_faixa_ano_detalhe_perc.height > 0:
                        lista_aeroportos_ano_detalhe_perc = aeroportos_faixa_ano_detalhe_perc["aeroporto"].to_list()
                        
                        # Filtrar dados usando o filtro correto por período específico
                        df_detalhe_aeronave_perc = (df_voos_filtrado_por_periodo_perc
                                                  .filter(
                                                      (pl.col("aeronave") == aeronave_selecionada_detalhe_perc) &
                                                      (pl.col("periodo") == periodo_selecionado_detalhe_perc)
                                                  )
                                                  .select(["aeroporto", "quantidade_voos", pl.col("pax").alias("passageiros_estimados")])
                                                  .sort("quantidade_voos", descending=True))
                    else:
                        # Se não há aeroportos na faixa neste ano, criar DataFrame vazio
                        df_detalhe_aeronave_perc = pl.DataFrame({"aeroporto": [], "quantidade_voos": [], "passageiros_estimados": []})
                    
                    # Verificar se há dados para mostrar
                    if df_detalhe_aeronave_perc.height > 0:
                        st.markdown(f"### 📊 **Detalhamento: {aeronave_selecionada_detalhe_perc} - {periodo_selecionado_detalhe_perc} - {faixa_selecionada_perc}**")
                        
                        # Métricas do detalhamento
                        total_voos_aeronave_perc = df_detalhe_aeronave_perc["quantidade_voos"].sum()
                        total_passageiros_estimados_perc = df_detalhe_aeronave_perc["passageiros_estimados"].sum()
                        total_aeroportos_aeronave_perc = df_detalhe_aeronave_perc.height
                        media_voos_por_aeroporto_perc = df_detalhe_aeronave_perc["quantidade_voos"].mean()
                        
                        col_metric_det_perc1, col_metric_det_perc2, col_metric_det_perc3, col_metric_det_perc4 = st.columns(4)
                        with col_metric_det_perc1:
                            st.metric("Total Movimentos (P + D)", formatar_numero(total_voos_aeronave_perc))
                        with col_metric_det_perc2:
                            st.metric("Total Passageiros (E + D)", formatar_numero(total_passageiros_estimados_perc))
                        with col_metric_det_perc3:
                            st.metric("Aeroportos", formatar_numero(total_aeroportos_aeronave_perc))
                        with col_metric_det_perc4:
                            st.metric("Média Movimentos/Aeroporto", formatar_numero(media_voos_por_aeroporto_perc))
                        
                        # Tabela de detalhamento
                        st.markdown("#### 📋 **Aeroportos, Movimentos e Passageiros (E + D)**")
                        
                        # Adicionar ranking aos aeroportos
                        df_detalhe_com_ranking_perc = df_detalhe_aeronave_perc.with_row_index("ranking").with_columns([
                            (pl.col("ranking") + 1).alias("posicao")
                        ]).select(["posicao", "aeroporto", "quantidade_voos", "passageiros_estimados"])
                        
                        df_detalhe_pandas_perc = df_detalhe_com_ranking_perc.to_pandas()

                        # Formatar a coluna de voos
                        if 'quantidade_voos' in df_detalhe_pandas_perc.columns:
                            df_detalhe_pandas_perc['quantidade_voos'] = df_detalhe_pandas_perc['quantidade_voos'].apply(lambda x: formatar_numero(x, casas_decimais=0))
                        if 'passageiros_estimados' in df_detalhe_pandas_perc.columns:
                            df_detalhe_pandas_perc['passageiros_estimados'] = df_detalhe_pandas_perc['passageiros_estimados'].apply(lambda x: formatar_numero(x, casas_decimais=0))

                        st.dataframe(
                            df_detalhe_pandas_perc,
                            use_container_width=True,
                            column_config={
                                "posicao": st.column_config.NumberColumn(
                                    "#",
                                    help="Posição no ranking de movimentos (P + D)",
                                    format="%d"
                                ),
                                "aeroporto": st.column_config.TextColumn(
                                    "Aeroporto",
                                    help="Nome do aeroporto"
                                ),
                                "quantidade_voos": st.column_config.TextColumn(
                                    "Movimentos (P + D)",
                                    help="Número de movimentos (P + D) da aeronave neste aeroporto"
                                ),
                                "passageiros_estimados": st.column_config.TextColumn(
                                    "Passageiros (E + D)",
                                    help="Número de passageiros estimados (E + D) da aeronave neste aeroporto"
                                )
                            },
                            hide_index=True
                        )
                        
                        # Análise adicional do detalhamento
                        st.markdown("---")
                        col_analise_det_perc1, col_analise_det_perc2 = st.columns(2)
                        
                        with col_analise_det_perc1:
                            # Aeroporto com mais voos
                            aeroporto_top_perc = df_detalhe_aeronave_perc.row(0, named=True)
                            st.success(f"""
                            **🏆 Aeroporto Líder:**
                            - **{aeroporto_top_perc['aeroporto']}**
                            - **{formatar_numero(aeroporto_top_perc['quantidade_voos'])}** movimentos (P + D)
                            """)
                        
                        with col_analise_det_perc2:
                            # Percentual de aeroportos com voos (usando a lista correta para o período específico)
                            total_aeroportos_faixa_periodo_perc = len(lista_aeroportos_ano_detalhe_perc)
                            percentual_aeroportos_com_voos_perc = (total_aeroportos_aeronave_perc / total_aeroportos_faixa_periodo_perc * 100) if total_aeroportos_faixa_periodo_perc > 0 else 0
                            
                            st.info(f"""
                            **📊 Cobertura da Aeronave na {faixa_selecionada_perc} ({periodo_selecionado_detalhe_perc}):**
                            - **{percentual_aeroportos_com_voos_perc:.1f}%** dos aeroportos da faixa neste período
                            - **{total_aeroportos_aeronave_perc}** de **{total_aeroportos_faixa_periodo_perc}** aeroportos
                            """)
                    
                    else:
                        st.warning(f"⚠️ **Nenhum voo encontrado para a aeronave {aeronave_selecionada_detalhe_perc} no período {periodo_selecionado_detalhe_perc}.**")
                        st.info("💡 Tente selecionar uma aeronave ou período diferente.")
                
                else:
                    st.warning("⚠️ Não há dados suficientes para gerar o gráfico de percentuais.")
            
            else:
                st.warning(f"⚠️ **Nenhum movimento encontrado para aeroportos da {faixa_selecionada_perc}.**")
                st.info("💡 Verifique os filtros aplicados ou tente uma faixa diferente.")
        
        else:
            st.warning(f"⚠️ **Nenhum aeroporto encontrado na {faixa_selecionada_perc}.**")
            st.info("💡 Tente selecionar uma faixa diferente.")

with tab2:
    st.header("✈️ **Análise da Participação da Categoria de Aeronave por Faixa de Passageiros do Aeroporto**")
    st.markdown("### Visualize a composição dos movimentos por categoria de aeronave para aeroportos em faixas específicas de movimentação de passageiros (E + D).")

    # Adicionar seção informativa sobre as categorias
    with st.expander("ℹ️ **Sobre as Categorias de Aeronave**", expanded=False):
        # Dicionário de mapeamento de aeronave para categoria
        dicionario_aeronaves = {
            'ATR': '2C',
            'E195': '4C',
            'A20N': '3C',
            'B738': '4C',
            'A321': '4C',
            'A320': '4C',
            'B38M': '4C',
            'E295': '3C',
            'B737': '4C',
            'A21N': '4C',
            'A319': '3C',
            'C208': '1B',
            'A332': '4E',
            'B77W': '4E',
            'A339': '4E',
            'B763': '4D',
            'B789': '4E',
            'B734': '4C',
            'B733': '4C',
            'B722': '4C',
            'B77L': '4E',
            'B744': '4E',
            'B788': '4E',
            'CRJ2': '3B',
            'A343': '4E',
            'B772': '4E',
            'B39M': '4C',
            'A333': '4E',
            'B773': '4E',
            'E190': '4C',
            'B78X': '4E',
            'A388': '4F',
            'B748': '4F',
            'B739': '4C',
            'MD11': '4D',
            'B736': '3C',
            'B764': '4D',
            'B762': '4D',
            'A124': '4F',
            'A30B': '4D',
            'A359': '4E',
            'A35K': '4E',
            'B190': '2B',
            'IL76': '3D',
            'A345': '4E',
            'E145': '3B',
            'L101': '4D',
            'B753': '4D',
            'A346': '4E',
            'B743': '4E',
            'A342': '4E',
            'B703': '4D',
            'T204': '4D'
            }
        
        # Agrupar aeronaves por categoria a partir do dicionário
        aeronaves_por_categoria = {}
        for aeronave, categoria in dicionario_aeronaves.items():
            aeronaves_por_categoria.setdefault(categoria, []).append(aeronave)

        # Ordenar categorias pela ordem desejada
        ordem_info = ["1B", "2B", "3B", "2C", "3C", "3D", "4C", "4D", "4E", "4F"]
        
        # Exibir cada categoria
        for categoria in ordem_info:
            if categoria in aeronaves_por_categoria:
                aeronaves_list = sorted(aeronaves_por_categoria[categoria])
                aeronaves_str = ", ".join(aeronaves_list)
                st.markdown(f"**{categoria}**: {aeronaves_str}")

    # Seletor de anos (múltipla seleção)
    anos_disponiveis_categoria = sorted(df_filtrado1["ano"].unique().to_list())
    anos_selecionados_categoria = st.multiselect(
        "🗓️ **Selecione os Anos para Análise:**",
        options=anos_disponiveis_categoria,
        default=[anos_disponiveis_categoria[-1]],  # Último ano por padrão
        help="Selecione um ou mais anos para somar os dados. Se nenhum ano for selecionado, será usado o último ano disponível.",
        key="anos_categoria_faixa"
    )
    
    # Se nenhum ano foi selecionado, usar o último ano disponível
    if not anos_selecionados_categoria:
        anos_selecionados_categoria = [anos_disponiveis_categoria[-1]]
        st.info(f"ℹ️ Nenhum ano selecionado. Usando o último ano disponível: **{anos_selecionados_categoria[0]}**")
    
    # Mostrar informação sobre os anos selecionados
    if len(anos_selecionados_categoria) > 1:
        anos_ordenados = sorted(anos_selecionados_categoria)
        st.info(f"📊 **Análise agregada:** Os dados dos anos **{', '.join(map(str, anos_ordenados))}** serão somados para esta análise.")
    else:
        st.info(f"📊 **Análise do ano:** {anos_selecionados_categoria[0]}")

    # Filtrar dados pelos anos selecionados
    df_ano_voos = df_filtrado1.filter(pl.col("ano").is_in(anos_selecionados_categoria))
    df_ano_pax = df_filtrado2.filter(pl.col("ano").is_in(anos_selecionados_categoria)).select(["aeroporto", "passageiros_projetado"])
    
    df_joined = df_ano_voos.join(df_ano_pax, on="aeroporto", how="left").drop_nulls()

    if df_joined.height > 0:
        # Definir os pontos do eixo X (limites de passageiros)
        thresholds = [1000.1, 3000, 6500, 12000, 19500, 30000, 50000, 79000, 150000, 310000, 600000, 2000000, 4000000, 10000000, 15000000, 50000000]
        
        # Ordem das categorias para garantir consistência
        ordem_desejada = ["1B", "2B", "3B", "2C", "3C", "3D", "4C", "4D", "4E", "4F"]
        
        # Obter todas as categorias únicas presentes nos dados do ano selecionado
        categorias_nos_dados = df_joined["categoria_aeronave"].unique().to_list()
        
        # Combinar a ordem desejada com as categorias existentes para garantir que todas apareçam
        todas_as_categorias = set(ordem_desejada + categorias_nos_dados)
        
        # Função de ordenação personalizada
        def sort_key_categoria(categoria):
            try:
                return (0, ordem_desejada.index(categoria))
            except ValueError:
                return (1, categoria) # Ordenar categorias extras alfabeticamente
                
        # Aplicar a ordenação
        ordem_final_para_legenda = sorted(list(todas_as_categorias), key=sort_key_categoria)
        
        results_data = []
        detailed_data = {}
        all_categories_df = pl.DataFrame({"categoria_aeronave": ordem_final_para_legenda})

        # Adicionar 0 no início para o primeiro intervalo
        thresholds_with_zero = [0] + thresholds

        for i in range(len(thresholds_with_zero) - 1):
            lower_bound = thresholds_with_zero[i]
            upper_bound = thresholds_with_zero[i+1]

            df_in_range = df_joined.filter(
                (pl.col("passageiros_projetado") > lower_bound) & (pl.col("passageiros_projetado") <= upper_bound)
            )

            if df_in_range.height > 0:
                total_voos_threshold = df_in_range["quantidade_voos"].sum()
                total_passageiros_threshold = df_in_range["pax"].sum()
                if total_voos_threshold > 0:
                    voos_por_categoria = (
                        df_in_range
                        .group_by("categoria_aeronave")
                        .agg([
                            pl.sum("quantidade_voos").alias("voos_categoria"),
                            pl.sum("pax").alias("passageiros_categoria")
                        ])
                    )
                    
                    # Join com todas as categorias para garantir que todas estejam presentes, preencher nulos com 0
                    df_full_cat = all_categories_df.join(voos_por_categoria, on="categoria_aeronave", how="left").with_columns([
                        pl.col("voos_categoria").fill_null(0),
                        pl.col("passageiros_categoria").fill_null(0)
                    ])

                    df_threshold_results = df_full_cat.with_columns([
                        (pl.col("voos_categoria") / total_voos_threshold * 100).alias("percentual_voos"),
                        (pl.col("passageiros_categoria") / total_passageiros_threshold * 100).alias("percentual_passageiros"),
                        pl.lit(upper_bound).cast(pl.Float64).alias("limite_passageiros")
                    ])
                    
                    results_data.append(df_threshold_results)

                    # Armazenar dados detalhados
                    airports_list = df_in_range["aeroporto"].unique().sort().to_list()
                    detailed_data[upper_bound] = {
                        "airports": airports_list,
                        "data": df_threshold_results,
                        "total_movimentos": total_voos_threshold
                    }

        if results_data:
            df_final = pl.concat(results_data).sort("limite_passageiros")

            # Gráfico de linhas
            if len(anos_selecionados_categoria) == 1:
                titulo_anos = f"{anos_selecionados_categoria[0]}"
            else:
                anos_ordenados = sorted(anos_selecionados_categoria)
                titulo_anos = f"{anos_ordenados[0]}-{anos_ordenados[-1]} ({len(anos_selecionados_categoria)} anos)"
            
            st.markdown(f"#### 📈 **Participação de categoria de aeronave por Faixa de Passageiros - {titulo_anos}**")

            # Gerar cores consistentes para as categorias
            cores_paleta_cat = gerar_paleta_cores_aeronaves()
            mapa_cores_categoria = {
                categoria: cores_paleta_cat[i % len(cores_paleta_cat)]
                for i, categoria in enumerate(ordem_final_para_legenda)
            }

            fig_cumulative_fleet = px.line(
                df_final.to_pandas(),
                x="limite_passageiros",
                y="percentual_voos",
                color="categoria_aeronave",
                color_discrete_map=mapa_cores_categoria,
                category_orders={"categoria_aeronave": ordem_final_para_legenda},
                labels={
                    'limite_passageiros': 'Nível de Passageiros (E + D) do Aeroporto',
                    'percentual_voos': 'Participação nos movimentos (P + D)',
                    'categoria_aeronave': 'Categoria da Aeronave'
                },
                markers=True,
                hover_name="categoria_aeronave"
            )

            fig_cumulative_fleet.update_traces(
                hovertemplate="Participação: %{y:.2f}%<extra></extra>"
            )

            # Formatar os labels do eixo x para maior clareza
            ticktext = []
            tickvals_plot = []
            for t in thresholds:
                tickvals_plot.append(1000 if t == 1000.1 else t)
                
                label_val = 1000 if t == 1000.1 else t

                if label_val >= 1000000:
                    label = f'{label_val / 1000000:g}M'
                elif label_val >= 1000:
                    label = f'{label_val / 1000:g}k'
                else:
                    label = str(label_val)
                ticktext.append(label)

            fig_cumulative_fleet.update_layout(
                yaxis_ticksuffix="%",
                hovermode='x unified',
                xaxis=dict(
                    type='log',
                    title='Nível de Passageiros (E + D) do Aeroporto (escala log)',
                    tickvals=tickvals_plot,
                    ticktext=ticktext
                )
            )
            st.plotly_chart(fig_cumulative_fleet, use_container_width=True)

            # Seção de Detalhes por Nível de Passageiros
            with st.expander("🔍 **Explorar Detalhes por Faixa de Passageiros**", expanded=False):
                # Criar labels para o seletor
                threshold_labels = {}
                for i, upper_bound in enumerate(thresholds):
                    lower_bound = 0 if i == 0 else thresholds[i-1]

                    # Format upper bound
                    label_val_upper = 1000 if upper_bound == 1000.1 else int(upper_bound)
                    if label_val_upper >= 1000000: upper_str = f'{label_val_upper / 1000000:g}M'
                    elif label_val_upper >= 1000: upper_str = f'{label_val_upper / 1000:g}k'
                    else: upper_str = str(label_val_upper)

                    # Format lower bound
                    label_val_lower = 0 if lower_bound == 0 else (1000 if lower_bound == 1000.1 else int(lower_bound))
                    if label_val_lower >= 1000000: lower_str = f'{label_val_lower / 1000000:g}M'
                    elif label_val_lower >= 1000: lower_str = f'{label_val_lower / 1000:g}k'
                    else: lower_str = str(label_val_lower)

                    if i == 0:
                        label = f'Até {upper_str} passageiros'
                    else:
                        label = f'Entre {lower_str} e {upper_str} passageiros'
                    
                    threshold_labels[label] = upper_bound

                selected_label = st.selectbox(
                    "**Selecione uma faixa de passageiros para ver os detalhes:**",
                    options=list(threshold_labels.keys()),
                    key="selectbox_nivel_passageiros"
                )

                if selected_label:
                    selected_threshold = threshold_labels[selected_label]

                    if selected_threshold in detailed_data:
                        details = detailed_data[selected_threshold]
                        
                        st.markdown(f"#### Detalhes para a Faixa: **{selected_label}**")
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric("Total de Aeroportos Considerados", formatar_numero(len(details['airports'])))
                        with col2:
                            st.metric("Total de Movimentos (P + D)", formatar_numero(details['total_movimentos']))

                        st.markdown("##### **Composição de Movimentos por Categoria**")
                        
                        df_details_pd = details['data'].select(
                            pl.col("categoria_aeronave").alias("Categoria"),
                            pl.col("voos_categoria").alias("Movimentos (P + D)"),
                            pl.col("percentual_voos").alias("Percentual Movimentos (%)"),
                            pl.col("passageiros_categoria").alias("Passageiros (E + D)"),
                            pl.col("percentual_passageiros").alias("Percentual Passageiros (%)")
                        ).to_pandas()

                        # Formatar números
                        df_details_pd['Movimentos (P + D)'] = df_details_pd['Movimentos (P + D)'].apply(formatar_numero)
                        df_details_pd['Percentual Movimentos (%)'] = df_details_pd['Percentual Movimentos (%)'].apply(lambda x: f"{x:.2f}%")
                        df_details_pd['Passageiros (E + D)'] = df_details_pd['Passageiros (E + D)'].apply(formatar_numero)
                        df_details_pd['Percentual Passageiros (%)'] = df_details_pd['Percentual Passageiros (%)'].apply(lambda x: f"{x:.2f}%")
                        
                        st.dataframe(df_details_pd, use_container_width=True, hide_index=True)

                        st.markdown("##### **Aeroportos Incluídos na Análise**")
                        st.dataframe(pd.DataFrame(details['airports'], columns=["Aeroporto"]), use_container_width=True, height=200, hide_index=True)
                    else:
                        st.warning("Nenhum dado detalhado para o nível selecionado.")

            # Tabela de dados detalhados
            st.markdown("---")
            st.markdown("#### 📋 **Dados Detalhados (Percentual de Movimentos (P + D) %)**")
            
            df_pivot_table = df_final.to_pandas().pivot(
                index='categoria_aeronave',
                columns='limite_passageiros',
                values='percentual_voos'
            ).fillna(0)

            # Format columns
            for col in df_pivot_table.columns:
                df_pivot_table[col] = df_pivot_table[col].apply(lambda x: f'{x:.2f}%')

            st.dataframe(df_pivot_table)
            
            # Nova tabela de dados detalhados para passageiros
            st.markdown("---")
            st.markdown("#### 📋 **Dados Detalhados (Percentual de Passageiros (E + D) %)**")
            
            df_pivot_table_passageiros = df_final.to_pandas().pivot(
                index='categoria_aeronave',
                columns='limite_passageiros',
                values='percentual_passageiros'
            ).fillna(0)

            # Format columns
            for col in df_pivot_table_passageiros.columns:
                df_pivot_table_passageiros[col] = df_pivot_table_passageiros[col].apply(lambda x: f'{x:.2f}%')

            st.dataframe(df_pivot_table_passageiros)

        else:
            anos_str = ", ".join(map(str, sorted(anos_selecionados_categoria)))
            st.warning(f"⚠️ Nenhum dado de categoria encontrado para os anos {anos_str}.")
    else:
        anos_str = ", ".join(map(str, sorted(anos_selecionados_categoria)))
        st.warning(f"⚠️ Nenhum dado de voos encontrado para os anos {anos_str}.")

with tab3:
    st.header("📋 **Tabela de Presença de Movimentos**")
    st.markdown("### Visualize a presença de movimentos por aeroporto, aeronave e período (mês-ano)")
    
    # Usar os dados já filtrados (sem E110)
    if df_filtrado1.height > 0:
        # Criar coluna de período (ano-mês)
        df_presenca = df_filtrado1.with_columns([
            (pl.col("ano").cast(pl.Utf8) + "-" + pl.col("mes").cast(pl.Utf8).str.zfill(2)).alias("periodo")
        ])
        
        # Obter períodos únicos e ordená-los cronologicamente
        periodos_unicos_raw = df_presenca["periodo"].unique().to_list()
        
        # Função para ordenar períodos cronologicamente
        def ordenar_periodos_cronologicamente_presenca(periodos):
            def chave_ordenacao(periodo):
                # Formato: "2022-01" (ano-mes com zero à esquerda)
                ano, mes = periodo.split("-")
                return (int(ano), int(mes))
            return sorted(periodos, key=chave_ordenacao)
        
        periodos_unicos = ordenar_periodos_cronologicamente_presenca(periodos_unicos_raw)
        
        # Filtros por aeroporto e aeronave
        st.markdown("#### 🔍 **Filtros**")
        
        col_filtro1, col_filtro2 = st.columns(2)
        
        with col_filtro1:
            # Filtro por aeroporto
            aeroportos_disponiveis = sorted(df_presenca["aeroporto"].unique().to_list())
            
            # Inicializar session_state se não existir
            if "filtro_aeroportos_presenca" not in st.session_state:
                st.session_state["filtro_aeroportos_presenca"] = aeroportos_disponiveis
            
            # Garantir que o session_state contém apenas valores válidos (permitir lista vazia)
            valores_validos = [a for a in st.session_state["filtro_aeroportos_presenca"] if a in aeroportos_disponiveis]
            st.session_state["filtro_aeroportos_presenca"] = valores_validos
            
            # Botão para selecionar todos os aeroportos
            col_btn_airports, col_info_airports = st.columns([1, 3])
            with col_btn_airports:
                if st.button("✅ Todos", key="btn_select_all_airports", help="Selecionar todos os aeroportos"):
                    # Definir todos os aeroportos como selecionados
                    st.session_state["filtro_aeroportos_presenca"] = aeroportos_disponiveis
            
            with col_info_airports:
                st.caption("💡 Clique em 'Todos' para voltar a ver todos os aeroportos")
            
            aeroportos_selecionados = st.multiselect(
                "🏢 **Filtrar por Aeroportos:**",
                options=aeroportos_disponiveis,
                default=st.session_state["filtro_aeroportos_presenca"],
                help="Selecione os aeroportos para incluir na análise. Se nenhum for selecionado, nenhum resultado será mostrado.",
                key="filtro_aeroportos_presenca"
            )
        
        with col_filtro2:
            # Filtro por aeronave
            aeronaves_disponiveis = sorted(df_presenca["aeronave"].unique().to_list())
            
            # Inicializar session_state se não existir
            if "filtro_aeronaves_presenca" not in st.session_state:
                st.session_state["filtro_aeronaves_presenca"] = aeronaves_disponiveis
            
            # Garantir que o session_state contém apenas valores válidos (permitir lista vazia)
            valores_validos_aeronaves = [a for a in st.session_state["filtro_aeronaves_presenca"] if a in aeronaves_disponiveis]
            st.session_state["filtro_aeronaves_presenca"] = valores_validos_aeronaves
            
            # Botão para selecionar todas as aeronaves
            col_btn_aircraft, col_info_aircraft = st.columns([1, 3])
            with col_btn_aircraft:
                if st.button("✅ Todos", key="btn_select_all_aircraft", help="Selecionar todas as aeronaves"):
                    # Definir todas as aeronaves como selecionadas
                    st.session_state["filtro_aeronaves_presenca"] = aeronaves_disponiveis
            
            with col_info_aircraft:
                st.caption("💡 Clique em 'Todos' para voltar a ver todas as aeronaves")
            
            aeronaves_selecionadas = st.multiselect(
                "✈️ **Filtrar por Aeronaves:**",
                options=aeronaves_disponiveis,
                default=st.session_state["filtro_aeronaves_presenca"],
                help="Selecione as aeronaves para incluir na análise. Se nenhuma for selecionada, nenhum resultado será mostrado.",
                key="filtro_aeronaves_presenca"
            )
        
        # Aplicar filtros (permitir seleção vazia para mostrar nenhum resultado)
        # Filtrar dados pelos seletores
        df_presenca_filtrado = df_presenca.filter(
            (pl.col("aeroporto").is_in(aeroportos_selecionados)) &
            (pl.col("aeronave").is_in(aeronaves_selecionadas))
        )
        
        # Mostrar informações sobre os filtros aplicados
        if len(aeroportos_selecionados) < len(aeroportos_disponiveis) or len(aeronaves_selecionadas) < len(aeronaves_disponiveis):
            if len(aeroportos_selecionados) == 0 and len(aeronaves_selecionadas) == 0:
                st.info("🔍 **Filtros ativos:** Nenhum aeroporto e nenhuma aeronave selecionados")
            elif len(aeroportos_selecionados) == 0:
                st.info(f"🔍 **Filtros ativos:** Nenhum aeroporto selecionado, {len(aeronaves_selecionadas)} aeronaves")
            elif len(aeronaves_selecionadas) == 0:
                st.info(f"🔍 **Filtros ativos:** {len(aeroportos_selecionados)} aeroportos, nenhuma aeronave selecionada")
            else:
                st.info(f"🔍 **Filtros ativos:** {len(aeroportos_selecionados)} aeroportos, {len(aeronaves_selecionadas)} aeronaves")
        
        # Verificar se há dados após filtros
        if df_presenca_filtrado.height == 0:
            st.warning("⚠️ **Nenhum dado encontrado** com os filtros selecionados.")
            st.info("💡 Tente ajustar os filtros de aeroporto ou aeronave.")
        else:
        
            # Criar tabela de presença: aeroporto + aeronave como chave, períodos como colunas
            df_presenca_tabela = (df_presenca_filtrado
                                 .group_by(["aeroporto", "aeronave"])
                                 .agg([
                                     pl.col("periodo").unique().alias("periodos_com_movimento")
                                 ]))
            
            # Criar DataFrame com todas as combinações aeroporto-aeronave e todos os períodos
            aeroportos_unicos = sorted(df_presenca_filtrado["aeroporto"].unique().to_list())
            aeronaves_unicas = sorted(df_presenca_filtrado["aeronave"].unique().to_list())
        
            # Criar todas as combinações
            combinacoes = []
            for aeroporto in aeroportos_unicos:
                for aeronave in aeronaves_unicas:
                    combinacoes.append({"aeroporto": aeroporto, "aeronave": aeronave})
            
            df_combinacoes = pl.DataFrame(combinacoes)
            
            # Fazer join com os dados de presença
            df_final_presenca = df_combinacoes.join(df_presenca_tabela, on=["aeroporto", "aeronave"], how="left")
            
            # Criar colunas para cada período
            for periodo in periodos_unicos:
                df_final_presenca = df_final_presenca.with_columns([
                    pl.when(pl.col("periodos_com_movimento").is_not_null() & 
                           pl.col("periodos_com_movimento").list.contains(periodo))
                    .then(pl.lit("Sim"))
                    .otherwise(pl.lit("Não"))
                    .alias(periodo)
                ])
            
            # Remover coluna auxiliar
            df_final_presenca = df_final_presenca.drop("periodos_com_movimento")
            
            # Filtrar linhas que têm pelo menos um "Sim" (remover linhas com todos "Não")
            # Criar condição: pelo menos uma coluna de período deve ser "Sim"
            condicao_tem_movimento = pl.lit(False)
            for periodo in periodos_unicos:
                condicao_tem_movimento = condicao_tem_movimento | (pl.col(periodo) == "Sim")
            
            # Aplicar filtro para manter apenas linhas com pelo menos um movimento
            df_final_presenca = df_final_presenca.filter(condicao_tem_movimento)
            
            # Ordenar por aeroporto e aeronave
            df_final_presenca = df_final_presenca.sort(["aeroporto", "aeronave"])
        
            # Mostrar informações sobre a tabela
            st.info(f"""
            📊 **Informações da Tabela:**
            - **Total de aeroportos:** {len(aeroportos_unicos)}
            - **Total de aeronaves:** {len(aeronaves_unicas)}
            - **Períodos analisados:** {len(periodos_unicos)} ({periodos_unicos[0]} a {periodos_unicos[-1]})
            - **Total de combinações:** {df_final_presenca.height}
            """)
            
            # Mostrar a tabela
            st.markdown("#### 📋 **Tabela de Presença de Movimentos**")
            st.markdown("*'Sim' = Existe movimento | 'Não' = Sem movimento*")
            
            # Converter para pandas para exibição
            df_pandas_presenca = df_final_presenca.to_pandas()
            
            # Configurar colunas da tabela
            column_config = {
                "aeroporto": st.column_config.TextColumn(
                    "Aeroporto",
                    help="Código do aeroporto",
                    width="medium"
                ),
                "aeronave": st.column_config.TextColumn(
                    "Aeronave", 
                    help="Código da aeronave",
                    width="small"
                )
            }
            
            # Adicionar configuração para colunas de período
            for periodo in periodos_unicos:
                column_config[periodo] = st.column_config.TextColumn(
                    periodo,
                    help=f"Movimento em {periodo}",
                    width="small"
                )
            
            # Mostrar tabela com paginação
            st.dataframe(
                df_pandas_presenca,
                use_container_width=True,
                column_config=column_config,
                hide_index=True
            )
        
            
            # Nova tabela: Meses Consecutivos
            st.markdown("---")
            st.markdown("#### 📅 **Tabela de Meses Consecutivos**")
            st.markdown("### Análise de meses consecutivos com e sem operação (máximo, mínimo e médio) por aeroporto e aeronave")
            
            # Calcular meses consecutivos e sem operação para cada combinação aeroporto-aeronave
            def calcular_meses_consecutivos(row, periodos_unicos):
                """Calcula máximo, mínimo e médio de meses consecutivos com movimento e sem operação"""
                max_consecutivos = 0
                min_consecutivos = float('inf')
                consecutivos_atual = 0
                sequencias = []  # Lista para armazenar todas as sequências de movimento
                
                max_sem_operacao = 0
                min_sem_operacao = float('inf')
                sem_operacao_atual = 0
                sequencias_sem_operacao = []  # Lista para armazenar todas as sequências sem operação
                
                for periodo in periodos_unicos:
                    if row[periodo] == "Sim":
                        # Finalizar sequência sem operação se existir
                        if sem_operacao_atual > 0:
                            sequencias_sem_operacao.append(sem_operacao_atual)
                            min_sem_operacao = min(min_sem_operacao, sem_operacao_atual)
                        sem_operacao_atual = 0
                        
                        # Continuar sequência de movimento
                        consecutivos_atual += 1
                        max_consecutivos = max(max_consecutivos, consecutivos_atual)
                    else:
                        # Finalizar sequência de movimento se existir
                        if consecutivos_atual > 0:
                            sequencias.append(consecutivos_atual)
                            min_consecutivos = min(min_consecutivos, consecutivos_atual)
                        consecutivos_atual = 0
                        
                        # Continuar sequência sem operação
                        sem_operacao_atual += 1
                        max_sem_operacao = max(max_sem_operacao, sem_operacao_atual)
                
                # Adicionar a última sequência se terminar com "Sim"
                if consecutivos_atual > 0:
                    sequencias.append(consecutivos_atual)
                    min_consecutivos = min(min_consecutivos, consecutivos_atual)
                
                # Adicionar a última sequência se terminar com "Não"
                if sem_operacao_atual > 0:
                    sequencias_sem_operacao.append(sem_operacao_atual)
                    min_sem_operacao = min(min_sem_operacao, sem_operacao_atual)
                
                # Calcular médios
                if sequencias:
                    medio_consecutivos = sum(sequencias) / len(sequencias)
                else:
                    medio_consecutivos = 0
                    min_consecutivos = 0
                
                if sequencias_sem_operacao:
                    medio_sem_operacao = sum(sequencias_sem_operacao) / len(sequencias_sem_operacao)
                else:
                    medio_sem_operacao = 0
                    min_sem_operacao = 0
                
                return (max_consecutivos, min_consecutivos, medio_consecutivos, 
                        max_sem_operacao, min_sem_operacao, medio_sem_operacao)
            
            # Aplicar função para calcular meses consecutivos
            resultados = df_pandas_presenca.apply(
                lambda row: calcular_meses_consecutivos(row, periodos_unicos), axis=1
            )
            
            # Separar os resultados em colunas
            df_pandas_presenca['meses_consecutivos_maximo'] = [r[0] for r in resultados]
            df_pandas_presenca['meses_consecutivos_minimo'] = [r[1] for r in resultados]
            df_pandas_presenca['meses_consecutivos_medio'] = [r[2] for r in resultados]
            df_pandas_presenca['meses_sem_operacao_maximo'] = [r[3] for r in resultados]
            df_pandas_presenca['meses_sem_operacao_minimo'] = [r[4] for r in resultados]
            df_pandas_presenca['meses_sem_operacao_medio'] = [r[5] for r in resultados]
            
            # Criar tabela de meses consecutivos
            df_meses_consecutivos = df_pandas_presenca[['aeroporto', 'aeronave', 'meses_consecutivos_maximo', 'meses_consecutivos_minimo', 'meses_consecutivos_medio', 'meses_sem_operacao_maximo', 'meses_sem_operacao_minimo', 'meses_sem_operacao_medio']].copy()
            
            # Filtrar apenas combinações que tiveram pelo menos 1 mês de movimento
            df_meses_consecutivos = df_meses_consecutivos[df_meses_consecutivos['meses_consecutivos_maximo'] > 0]
            
            # Ordenar por meses consecutivos máximo (decrescente) e depois por aeroporto e aeronave
            df_meses_consecutivos = df_meses_consecutivos.sort_values(
                ['meses_consecutivos_maximo', 'aeroporto', 'aeronave'], 
                ascending=[False, True, True]
            )
            
            # Mostrar informações sobre a tabela
            if len(df_meses_consecutivos) > 0:
                max_meses = df_meses_consecutivos['meses_consecutivos_maximo'].max()
                media_meses = df_meses_consecutivos['meses_consecutivos_medio'].mean()
                total_aeroportos = df_meses_consecutivos['aeroporto'].nunique()
                st.info(f"""
                📊 **Informações da Tabela de Meses Consecutivos:**
                - **Total de combinações:** {len(df_meses_consecutivos)}
                - **Total de aeroportos:** {total_aeroportos}
                - **Máximo de meses consecutivos:** {max_meses}
                - **Média de meses consecutivos médio:** {media_meses:.1f}
                """)
            else:
                st.info(f"""
                📊 **Informações da Tabela de Meses Consecutivos:**
                - **Total de combinações com movimento:** 0
                - **Total de aeroportos:** 0
                - **Máximo de meses consecutivos:** 0
                - **Média de meses consecutivos médio:** 0.0
                """)
            
            # Filtros para meses consecutivos e sem operação
            if len(df_meses_consecutivos) > 0:
                # Seção 1: Filtros para Meses Consecutivos
                st.markdown("#### 🔍 **Filtros para Meses Consecutivos**")
                
                # Criar 3 colunas para os filtros de meses consecutivos
                col_filtro_max, col_filtro_min, col_filtro_med = st.columns(3)
                
                with col_filtro_max:
                    st.markdown("##### 📈 **Filtro por Máximo**")
                    
                    # Operador para máximo
                    operador_max = st.selectbox(
                        "📊 **Operador:**",
                        options=["Maior que (>)", "Menor que (<)", "Igual a (=)", "Maior ou igual (≥)", "Menor ou igual (≤)"],
                        index=3,  # "Maior ou igual (≥)" como padrão
                        help="Selecione o operador para filtrar por meses consecutivos máximo",
                        key="operador_meses_maximo"
                    )
                    
                    # Valor para máximo
                    max_valor_max = int(df_meses_consecutivos['meses_consecutivos_maximo'].max())
                    valor_max = st.number_input(
                        "🔢 **Valor:**",
                        min_value=0,
                        max_value=max_valor_max,
                        value=1,
                        help=f"Digite o valor para comparação (0 a {max_valor_max})",
                        key="valor_filtro_maximo"
                    )
                
                with col_filtro_min:
                    st.markdown("##### 📉 **Filtro por Mínimo**")
                    
                    # Operador para mínimo
                    operador_min = st.selectbox(
                        "📊 **Operador:**",
                        options=["Maior que (>)", "Menor que (<)", "Igual a (=)", "Maior ou igual (≥)", "Menor ou igual (≤)"],
                        index=3,  # "Maior ou igual (≥)" como padrão
                        help="Selecione o operador para filtrar por meses consecutivos mínimo",
                        key="operador_meses_minimo"
                    )
                    
                    # Valor para mínimo
                    max_valor_min = int(df_meses_consecutivos['meses_consecutivos_minimo'].max())
                    valor_min = st.number_input(
                        "🔢 **Valor:**",
                        min_value=0,
                        max_value=max_valor_min,
                        value=1,
                        help=f"Digite o valor para comparação (0 a {max_valor_min})",
                        key="valor_filtro_minimo"
                    )
                
                with col_filtro_med:
                    st.markdown("##### 📊 **Filtro por Médio**")
                    
                    # Operador para médio
                    operador_med = st.selectbox(
                        "📊 **Operador:**",
                        options=["Maior que (>)", "Menor que (<)", "Igual a (=)", "Maior ou igual (≥)", "Menor ou igual (≤)"],
                        index=3,  # "Maior ou igual (≥)" como padrão
                        help="Selecione o operador para filtrar por meses consecutivos médio",
                        key="operador_meses_medio"
                    )
                    
                    # Valor para médio
                    max_valor_med = float(df_meses_consecutivos['meses_consecutivos_medio'].max())
                    valor_med = st.number_input(
                        "🔢 **Valor:**",
                        min_value=0.0,
                        max_value=max_valor_med,
                        value=1.0,
                        step=0.1,
                        help=f"Digite o valor para comparação (0.0 a {max_valor_med:.1f})",
                        key="valor_filtro_medio"
                    )
                
                # Seção 2: Filtros para Meses Sem Operação
                
                st.markdown("#### 🔍 **Filtros para Meses Sem Operação**")
                
                # Criar 3 colunas para os filtros de meses sem operação
                col_filtro_sem_max, col_filtro_sem_min, col_filtro_sem_med = st.columns(3)
                
                with col_filtro_sem_max:
                    st.markdown("##### 📈 **Filtro por Máximo Sem Operação**")
                    
                    # Operador para máximo sem operação
                    operador_sem_max = st.selectbox(
                        "📊 **Operador:**",
                        options=["Maior que (>)", "Menor que (<)", "Igual a (=)", "Maior ou igual (≥)", "Menor ou igual (≤)"],
                        index=3,  # "Maior ou igual (≥)" como padrão
                        help="Selecione o operador para filtrar por meses sem operação máximo",
                        key="operador_meses_sem_maximo"
                    )
                    
                    # Valor para máximo sem operação
                    max_valor_sem_max = int(df_meses_consecutivos['meses_sem_operacao_maximo'].max())
                    valor_sem_max = st.number_input(
                        "🔢 **Valor:**",
                        min_value=0,
                        max_value=max_valor_sem_max,
                        value=0,
                        help=f"Digite o valor para comparação (0 a {max_valor_sem_max})",
                        key="valor_filtro_sem_maximo"
                    )
                
                with col_filtro_sem_min:
                    st.markdown("##### 📉 **Filtro por Mínimo Sem Operação**")
                    
                    # Operador para mínimo sem operação
                    operador_sem_min = st.selectbox(
                        "📊 **Operador:**",
                        options=["Maior que (>)", "Menor que (<)", "Igual a (=)", "Maior ou igual (≥)", "Menor ou igual (≤)"],
                        index=3,  # "Maior ou igual (≥)" como padrão
                        help="Selecione o operador para filtrar por meses sem operação mínimo",
                        key="operador_meses_sem_minimo"
                    )
                    
                    # Valor para mínimo sem operação
                    max_valor_sem_min = int(df_meses_consecutivos['meses_sem_operacao_minimo'].max())
                    valor_sem_min = st.number_input(
                        "🔢 **Valor:**",
                        min_value=0,
                        max_value=max_valor_sem_min,
                        value=0,
                        help=f"Digite o valor para comparação (0 a {max_valor_sem_min})",
                        key="valor_filtro_sem_minimo"
                    )
                
                with col_filtro_sem_med:
                    st.markdown("##### 📊 **Filtro por Médio Sem Operação**")
                    
                    # Operador para médio sem operação
                    operador_sem_med = st.selectbox(
                        "📊 **Operador:**",
                        options=["Maior que (>)", "Menor que (<)", "Igual a (=)", "Maior ou igual (≥)", "Menor ou igual (≤)"],
                        index=3,  # "Maior ou igual (≥)" como padrão
                        help="Selecione o operador para filtrar por meses sem operação médio",
                        key="operador_meses_sem_medio"
                    )
                    
                    # Valor para médio sem operação
                    max_valor_sem_med = float(df_meses_consecutivos['meses_sem_operacao_medio'].max())
                    valor_sem_med = st.number_input(
                        "🔢 **Valor:**",
                        min_value=0.0,
                        max_value=max_valor_sem_med,
                        value=0.0,
                        step=0.1,
                        help=f"Digite o valor para comparação (0.0 a {max_valor_sem_med:.1f})",
                        key="valor_filtro_sem_medio"
                    )
                
                # Aplicar filtros combinados
                df_meses_filtrado = df_meses_consecutivos.copy()
                
                # Filtro por máximo
                if operador_max == "Maior que (>)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_consecutivos_maximo'] > valor_max]
                elif operador_max == "Menor que (<)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_consecutivos_maximo'] < valor_max]
                elif operador_max == "Igual a (=)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_consecutivos_maximo'] == valor_max]
                elif operador_max == "Maior ou igual (≥)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_consecutivos_maximo'] >= valor_max]
                elif operador_max == "Menor ou igual (≤)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_consecutivos_maximo'] <= valor_max]
                
                # Filtro por mínimo
                if operador_min == "Maior que (>)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_consecutivos_minimo'] > valor_min]
                elif operador_min == "Menor que (<)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_consecutivos_minimo'] < valor_min]
                elif operador_min == "Igual a (=)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_consecutivos_minimo'] == valor_min]
                elif operador_min == "Maior ou igual (≥)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_consecutivos_minimo'] >= valor_min]
                elif operador_min == "Menor ou igual (≤)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_consecutivos_minimo'] <= valor_min]
                
                # Filtro por médio
                if operador_med == "Maior que (>)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_consecutivos_medio'] > valor_med]
                elif operador_med == "Menor que (<)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_consecutivos_medio'] < valor_med]
                elif operador_med == "Igual a (=)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_consecutivos_medio'] == valor_med]
                elif operador_med == "Maior ou igual (≥)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_consecutivos_medio'] >= valor_med]
                elif operador_med == "Menor ou igual (≤)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_consecutivos_medio'] <= valor_med]
                
                # Filtro por máximo sem operação
                if operador_sem_max == "Maior que (>)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_sem_operacao_maximo'] > valor_sem_max]
                elif operador_sem_max == "Menor que (<)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_sem_operacao_maximo'] < valor_sem_max]
                elif operador_sem_max == "Igual a (=)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_sem_operacao_maximo'] == valor_sem_max]
                elif operador_sem_max == "Maior ou igual (≥)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_sem_operacao_maximo'] >= valor_sem_max]
                elif operador_sem_max == "Menor ou igual (≤)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_sem_operacao_maximo'] <= valor_sem_max]
                
                # Filtro por mínimo sem operação
                if operador_sem_min == "Maior que (>)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_sem_operacao_minimo'] > valor_sem_min]
                elif operador_sem_min == "Menor que (<)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_sem_operacao_minimo'] < valor_sem_min]
                elif operador_sem_min == "Igual a (=)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_sem_operacao_minimo'] == valor_sem_min]
                elif operador_sem_min == "Maior ou igual (≥)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_sem_operacao_minimo'] >= valor_sem_min]
                elif operador_sem_min == "Menor ou igual (≤)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_sem_operacao_minimo'] <= valor_sem_min]
                
                # Filtro por médio sem operação
                if operador_sem_med == "Maior que (>)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_sem_operacao_medio'] > valor_sem_med]
                elif operador_sem_med == "Menor que (<)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_sem_operacao_medio'] < valor_sem_med]
                elif operador_sem_med == "Igual a (=)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_sem_operacao_medio'] == valor_sem_med]
                elif operador_sem_med == "Maior ou igual (≥)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_sem_operacao_medio'] >= valor_sem_med]
                elif operador_sem_med == "Menor ou igual (≤)":
                    df_meses_filtrado = df_meses_filtrado[df_meses_filtrado['meses_sem_operacao_medio'] <= valor_sem_med]
                
                # Mostrar informações sobre os filtros aplicados
                filtros_ativos = []
                if len(df_meses_filtrado) != len(df_meses_consecutivos):
                    if operador_max != "Maior ou igual (≥)" or valor_max != 1:  # Se não for o padrão
                        filtros_ativos.append(f"Máximo: {operador_max} {valor_max}")
                    if operador_min != "Maior ou igual (≥)" or valor_min != 1:  # Se não for o padrão
                        filtros_ativos.append(f"Mínimo: {operador_min} {valor_min}")
                    if operador_med != "Maior ou igual (≥)" or valor_med != 1.0:  # Se não for o padrão
                        filtros_ativos.append(f"Médio: {operador_med} {valor_med}")
                    if operador_sem_max != "Maior ou igual (≥)" or valor_sem_max != 1:  # Se não for o padrão
                        filtros_ativos.append(f"Máx. Sem Op.: {operador_sem_max} {valor_sem_max}")
                    if operador_sem_min != "Maior ou igual (≥)" or valor_sem_min != 1:  # Se não for o padrão
                        filtros_ativos.append(f"Mín. Sem Op.: {operador_sem_min} {valor_sem_min}")
                    if operador_sem_med != "Maior ou igual (≥)" or valor_sem_med != 1.0:  # Se não for o padrão
                        filtros_ativos.append(f"Méd. Sem Op.: {operador_sem_med} {valor_sem_med}")
                    
                    if filtros_ativos:
                        st.info(f"🔍 **Filtros ativos:** {' | '.join(filtros_ativos)} | **Resultados:** {len(df_meses_filtrado)} de {len(df_meses_consecutivos)} combinações")
                
                # Verificar se há resultados após filtro
                if len(df_meses_filtrado) == 0:
                    st.warning("⚠️ **Nenhum resultado encontrado** com os filtros aplicados.")
                    st.info("💡 Tente ajustar os operadores ou valores dos filtros.")
                else:
                    # Mostrar a tabela filtrada
                    # Configurar colunas da tabela
                    column_config_meses = {
                        "aeroporto": st.column_config.TextColumn(
                            "Aeroporto",
                            help="Código do aeroporto",
                            width="medium"
                        ),
                        "aeronave": st.column_config.TextColumn(
                            "Aeronave", 
                            help="Código da aeronave",
                            width="small"
                        ),
                        "meses_consecutivos_maximo": st.column_config.NumberColumn(
                            "Meses Consecutivos Máximo",
                            help="Máximo de meses consecutivos com movimentação",
                            width="medium",
                            format="%d"
                        ),
                        "meses_consecutivos_minimo": st.column_config.NumberColumn(
                            "Meses Consecutivos Mínimo",
                            help="Mínimo de meses consecutivos com movimentação",
                            width="medium",
                            format="%d"
                        ),
                        "meses_consecutivos_medio": st.column_config.NumberColumn(
                            "Meses Consecutivos Médio",
                            help="Média de meses consecutivos com movimentação",
                            width="medium",
                            format="%.1f"
                        ),
                        "meses_sem_operacao_maximo": st.column_config.NumberColumn(
                            "Meses Sem Operação Máximo",
                            help="Máximo de meses consecutivos sem operação",
                            width="medium",
                            format="%d"
                        ),
                        "meses_sem_operacao_minimo": st.column_config.NumberColumn(
                            "Meses Sem Operação Mínimo",
                            help="Mínimo de meses consecutivos sem operação",
                            width="medium",
                            format="%d"
                        ),
                        "meses_sem_operacao_medio": st.column_config.NumberColumn(
                            "Meses Sem Operação Médio",
                            help="Média de meses consecutivos sem operação",
                            width="medium",
                            format="%.1f"
                        )
                    }
                    
                    # Mostrar tabela
                    st.dataframe(
                        df_meses_filtrado,
                        use_container_width=True,
                        column_config=column_config_meses,
                        hide_index=True
                    )
                
                # Gráfico de Presença de Movimentos
                st.markdown("---")
                st.markdown("#### 📊 **Gráfico de Presença de Movimentos**")
                st.markdown("Visualize a presença de movimentos (0 = Não, 1 = Sim) ao longo do tempo para cada combinação aeroporto-aeronave")
                
                # Preparar dados para o gráfico
                # Criar DataFrame com dados de presença para as combinações filtradas
                df_grafico = df_pandas_presenca[df_pandas_presenca[['aeroporto', 'aeronave']].apply(
                    lambda row: f"{row['aeroporto']}-{row['aeronave']}" in 
                    df_meses_filtrado[['aeroporto', 'aeronave']].apply(
                        lambda x: f"{x['aeroporto']}-{x['aeronave']}", axis=1
                    ).values, axis=1
                )].copy()
                
                if len(df_grafico) > 0:
                    # Criar dados longos para o gráfico
                    df_grafico_long = df_grafico.melt(
                        id_vars=['aeroporto', 'aeronave'],
                        value_vars=periodos_unicos,
                        var_name='periodo',
                        value_name='presenca'
                    )
                    
                    # Converter "Sim"/"Não" para 1/0
                    df_grafico_long['valor_presenca'] = df_grafico_long['presenca'].map({'Sim': 1, 'Não': 0})
                    
                    # Criar coluna de combinação aeroporto-aeronave para legenda
                    df_grafico_long['combinacao'] = df_grafico_long['aeroporto'] + '-' + df_grafico_long['aeronave']
                    
                    # Criar gráfico
                    import plotly.express as px
                    import plotly.graph_objects as go
                    
                    # Criar gráfico de linha
                    fig = px.line(
                        df_grafico_long,
                        x='periodo',
                        y='valor_presenca',
                        color='combinacao',
                        title='Presença de Movimentos por Período',
                        labels={
                            'periodo': 'Período (Mês-Ano)',
                            'valor_presenca': 'Presença de Movimento',
                            'combinacao': 'Aeroporto-Aeronave'
                        },
                        height=600
                    )
                    
                    # Personalizar o gráfico
                    fig.update_layout(
                        yaxis=dict(
                            tickmode='array',
                            tickvals=[0, 1],
                            ticktext=['Não', 'Sim'],
                            range=[-0.1, 1.1]
                        ),
                        xaxis=dict(
                            tickangle=45
                        ),
                        legend=dict(
                            orientation="v",
                            yanchor="top",
                            y=1,
                            xanchor="left",
                            x=1.02
                        ),
                        hovermode='x unified'
                    )
                    
                    # Adicionar pontos para melhor visualização
                    fig.update_traces(
                        mode='lines+markers',
                        marker=dict(size=4),
                        line=dict(width=2)
                    )
                    
                    # Mostrar gráfico
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Informações sobre o gráfico
                    st.info(f"""
                    📊 **Informações do Gráfico:**
                    - **Combinações mostradas (aeroporto-aeronave):** {len(df_grafico_long['combinacao'].unique())}
                    - **Aeroportos mostrados:** {len(df_grafico_long['aeroporto'].unique())}
                    - **Aeronaves mostradas:** {len(df_grafico_long['aeronave'].unique())}
                    - **Valores:** 0 = Sem movimento, 1 = Com movimento
                    - **Dica:** Clique duas vezes em um aeroporto-aeronave na legenda para destacar apenas essa combinação. Clique duas vezes para voltar a ver todas as combinações.
                    - **Observação:** O gráfico mostra as combinações mostradas anteriormente na Tabela de Meses Consecutivos
                    """)
                
                # Nova tabela: Detalhamento de Movimentos e Passageiros por Período
                st.markdown("---")
                st.markdown("#### 📋 **Detalhamento de Movimentos e Passageiros por Período**")
                st.markdown("Visualize a quantidade de movimentação (P + D) e passageiros (E + D) por mês-ano para as combinações aeroporto-aeronave selecionadas")
                
                # Filtros específicos para a tabela de detalhamento
                st.markdown("##### 🔍 **Filtros para Detalhamento**")
                
                col_filtro_det1, col_filtro_det2 = st.columns(2)
                
                with col_filtro_det1:
                    # Filtro por aeroporto específico - apenas aeroportos presentes no gráfico
                    aeroportos_detalhamento = sorted(df_meses_filtrado["aeroporto"].unique().tolist())
                    aeroporto_detalhamento = st.selectbox(
                        "🏢 **Selecionar Aeroporto:**",
                        options=["Todos"] + aeroportos_detalhamento,
                        help="Selecione um aeroporto específico ou 'Todos' para ver todos os aeroportos presentes no Gráfico de Presença de Movimentos",
                        key="aeroporto_detalhamento"
                    )
                
                with col_filtro_det2:
                    # Filtro por aeronave específica - apenas aeronaves presentes no gráfico
                    aeronaves_detalhamento = sorted(df_meses_filtrado["aeronave"].unique().tolist())
                    aeronave_detalhamento = st.selectbox(
                        "✈️ **Selecionar Aeronave:**",
                        options=["Todos"] + aeronaves_detalhamento,
                        help="Selecione uma aeronave específica ou 'Todos' para ver todas as aeronaves presentes no Gráfico de Presença de Movimentos",
                        key="aeronave_detalhamento"
                    )
                
                # Aplicar filtros específicos - usar apenas combinações presentes no gráfico
                # Primeiro, filtrar apenas as combinações que estão no df_meses_filtrado
                combinacoes_grafico = df_meses_filtrado[['aeroporto', 'aeronave']].apply(
                    lambda x: f"{x['aeroporto']}-{x['aeronave']}", axis=1
                ).tolist()
                
                df_detalhamento_filtrado = df_presenca_filtrado.filter(
                    pl.concat_str([pl.col("aeroporto"), pl.lit("-"), pl.col("aeronave")]).is_in(combinacoes_grafico)
                )
                
                if aeroporto_detalhamento != "Todos":
                    df_detalhamento_filtrado = df_detalhamento_filtrado.filter(
                        pl.col("aeroporto") == aeroporto_detalhamento
                    )
                
                if aeronave_detalhamento != "Todos":
                    df_detalhamento_filtrado = df_detalhamento_filtrado.filter(
                        pl.col("aeronave") == aeronave_detalhamento
                    )
                
                # Mostrar informações sobre os filtros aplicados
                filtros_detalhamento = []
                if aeroporto_detalhamento != "Todos":
                    filtros_detalhamento.append(f"Aeroporto: {aeroporto_detalhamento}")
                if aeronave_detalhamento != "Todos":
                    filtros_detalhamento.append(f"Aeronave: {aeronave_detalhamento}")
                
                if filtros_detalhamento:
                    st.info(f"🔍 **Filtros aplicados:** {' | '.join(filtros_detalhamento)}")
                
                # Preparar dados para a tabela de detalhamento
                df_detalhamento = df_detalhamento_filtrado.select([
                    "aeroporto", "aeronave", "periodo", 
                    pl.col("quantidade_voos").alias("movimentos_p_d"),
                    pl.col("pax").alias("passageiros_e_d")
                ]).sort(["aeroporto", "aeronave", "periodo"])
                
                if df_detalhamento.height > 0:
                    # Converter para pandas para exibição
                    df_detalhamento_pandas = df_detalhamento.to_pandas()
                    
                    # Configurar colunas da tabela
                    column_config_detalhamento = {
                        "aeroporto": st.column_config.TextColumn(
                            "Aeroporto",
                            help="Código do aeroporto",
                            width="medium"
                        ),
                        "aeronave": st.column_config.TextColumn(
                            "Aeronave", 
                            help="Código da aeronave",
                            width="small"
                        ),
                        "periodo": st.column_config.TextColumn(
                            "Período",
                            help="Período no formato Ano-Mês",
                            width="medium"
                        ),
                        "movimentos_p_d": st.column_config.NumberColumn(
                            "Movimentos (P + D)",
                            help="Quantidade de movimentos de pouso e decolagem",
                            width="medium",
                            format="%d"
                        ),
                        "passageiros_e_d": st.column_config.NumberColumn(
                            "Passageiros (E + D)",
                            help="Quantidade de passageiros embarcados e desembarcados",
                            width="medium",
                            format="%d"
                        )
                    }
                    
                    # Mostrar tabela
                    st.dataframe(
                        df_detalhamento_pandas,
                        use_container_width=True,
                        column_config=column_config_detalhamento,
                        hide_index=True
                    )
                    
                    # Estatísticas da tabela de detalhamento
                    total_movimentos = df_detalhamento_pandas['movimentos_p_d'].sum()
                    total_passageiros = df_detalhamento_pandas['passageiros_e_d'].sum()
                    combinacoes_unicas = len(df_detalhamento_pandas[['aeroporto', 'aeronave']].drop_duplicates())
                    periodos_unicos_detalhamento = len(df_detalhamento_pandas['periodo'].unique())
                    
                    st.info(f"""
                    📊 **Estatísticas da Tabela de Detalhamento:**
                    - **Total de movimentos (P + D):** {formatar_numero(total_movimentos)}
                    - **Total de passageiros (E + D):** {formatar_numero(total_passageiros)}
                    - **Períodos únicos:** {periodos_unicos_detalhamento}
                    """)
                else:
                    st.warning("⚠️ **Nenhum dado disponível para a tabela de detalhamento** com os filtros aplicados.")
                
            else:
                st.warning("⚠️ **Nenhuma combinação com movimentação encontrada.**")
                st.info("💡 Todas as combinações aeroporto-aeronave não tiveram movimentação nos períodos analisados.")
        
    else:
        st.warning("⚠️ **Nenhum dado de voos encontrado.**")
        st.info("💡 Verifique os filtros aplicados ou se há dados disponíveis.")


