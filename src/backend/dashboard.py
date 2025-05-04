import pandas as pd
import streamlit as st
import plotly.express as px
from datetime import datetime, timedelta
import requests
import json

# Configuração da página
st.set_page_config(
    page_title="Fake Insights Dashboard",
    page_icon="📊",
    layout="wide"
)

# Título e descrição
st.title("📊 Fake Insights Dashboard")
st.markdown("""
    Este dashboard exibe análises e visualizações dos dados de usuários gerados.
    Use os filtros abaixo para explorar os dados.
""")

# Sidebar com filtros
st.sidebar.title("Filtros")
n_users = st.sidebar.slider("Número de usuários", 10, 1000, 100)
refresh_data = st.sidebar.button("Atualizar Dados")

# Carrega dados da API
@st.cache_data(ttl=300)  # Cache por 5 minutos
def load_data(n_users):
    try:
        response = requests.get(f"http://localhost:8000/users/?n={n_users}")
        if response.status_code == 200:
            return pd.DataFrame(response.json())
        else:
            st.error("Erro ao carregar dados da API")
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Erro ao conectar com a API: {str(e)}")
        return pd.DataFrame()

# Carrega os dados
df = load_data(n_users)

if not df.empty:
    # Métricas principais
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total de Usuários", len(df))
    with col2:
        st.metric("Média Salarial", f"R$ {df['salario'].mean():,.2f}")
    with col3:
        st.metric("Salário Máximo", f"R$ {df['salario'].max():,.2f}")
    with col4:
        st.metric("Salário Mínimo", f"R$ {df['salario'].min():,.2f}")

    # Tabs para diferentes visualizações
    tab1, tab2, tab3 = st.tabs(["Distribuição", "Temporal", "Geográfica"])

    with tab1:
        # Distribuição de salários
        st.subheader("Distribuição de Salários")
        fig_salary = px.histogram(df, x="salario", nbins=20, 
                                title="Distribuição de Salários",
                                labels={"salario": "Salário (R$)"})
        st.plotly_chart(fig_salary, use_container_width=True)

        # Top 10 cidades
        st.subheader("Top 10 Cidades")
        top_cities = df['cidade'].value_counts().head(10)
        fig_cities = px.bar(top_cities, 
                          title="Top 10 Cidades por Número de Usuários",
                          labels={"value": "Número de Usuários", "index": "Cidade"})
        st.plotly_chart(fig_cities, use_container_width=True)

    with tab2:
        # Análise temporal
        st.subheader("Análise Temporal")
        df['data_cadastro'] = pd.to_datetime(df['data_cadastro'])
        df['mes_cadastro'] = df['data_cadastro'].dt.to_period('M')
        
        # Cadastros por mês
        cadastros_mes = df.groupby('mes_cadastro').size().reset_index(name='count')
        cadastros_mes['mes_cadastro'] = cadastros_mes['mes_cadastro'].astype(str)
        
        fig_temporal = px.line(cadastros_mes, x='mes_cadastro', y='count',
                             title="Cadastros por Mês",
                             labels={"mes_cadastro": "Mês", "count": "Número de Cadastros"})
        st.plotly_chart(fig_temporal, use_container_width=True)

    with tab3:
        # Análise geográfica
        st.subheader("Análise Geográfica")
        
        # Mapa de calor por cidade
        city_stats = df.groupby('cidade').agg({
            'salario': 'mean',
            'id': 'count'
        }).reset_index()
        city_stats.columns = ['cidade', 'salario_medio', 'total_usuarios']
        
        fig_geo = px.scatter(city_stats, x='total_usuarios', y='salario_medio',
                           size='total_usuarios', color='salario_medio',
                           hover_name='cidade',
                           title="Distribuição Geográfica de Usuários",
                           labels={"total_usuarios": "Total de Usuários",
                                 "salario_medio": "Salário Médio (R$)"})
        st.plotly_chart(fig_geo, use_container_width=True)

    # Tabela de dados
    st.subheader("Dados Detalhados")
    st.dataframe(df, use_container_width=True)

else:
    st.warning("Nenhum dado disponível. Verifique se a API está rodando.")