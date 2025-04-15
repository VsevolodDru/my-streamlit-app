import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
from datetime import datetime, timedelta
import pytz
import numpy as np
import io
from openpyxl import Workbook
import requests

# Настройки страницы
st.set_page_config(
    layout="wide",
    page_title="WB Analytics Pro",
    page_icon="📈",
    initial_sidebar_state="expanded"
)

# Загрузка данных из JSON файла по ссылке
@st.cache_data(ttl=3600)
def load_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        # Преобразование данных
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize('Europe/Moscow')
        df['lastChangeDate'] = pd.to_datetime(df['lastChangeDate']).dt.tz_localize('Europe/Moscow')
        df['is_return'] = df.get('srid', '').str.startswith('R')
        df['revenue'] = df['totalPrice']
        df['week'] = df['date'].dt.isocalendar().week
        df['month'] = df['date'].dt.month
        df['isCancel'] = df.get('isCancel', False)
        # Русские названия для отображения
        df = df.rename(columns={
            'date': 'Дата',
            'warehouseName': 'Склад',
            'warehouseType': 'Тип склада',
            'regionName': 'Регион',
            'category': 'Категория',
            'brand': 'Бренд',
            'subject': 'Подкатегория',
            'totalPrice': 'Цена',
            'revenue': 'Выручка',
            'spp': 'СПП',
            'supplierArticle': 'Артикул'
        })
        # Объединение брендов
        df['Бренд'] = df['Бренд'].str.lower()
        # Обработка артикулов
        df['Артикул'] = df['Артикул'].astype(str)
        df['Артикул'] = df['Артикул'].apply(lambda x: x[:len(x)//2] if len(x) == 20 and x[:10] == x[10:] else x)
        return df
    except Exception as e:
        st.error(f"Ошибка при загрузке данных: {str(e)}")
        return pd.DataFrame()

# Загрузка данных из Excel файла
@st.cache_data(ttl=3600)
def load_excel_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        excel_file = io.BytesIO(response.content)
        df = pd.read_excel(excel_file)
        # Проверка наличия необходимых столбцов
        required_columns = ['Артикул продавца', 'Наименование']
        if not all(col in df.columns for col in required_columns):
            st.error(f"В Excel файле отсутствуют необходимые столбцы: {required_columns}")
            return pd.DataFrame()
        # Переименование столбцов
        df = df.rename(columns={'Артикул продавца': 'Артикул', 'Наименование': 'Наименование товара'})
        # Преобразование артикула в строковый тип
        df['Артикул'] = df['Артикул'].astype(str)
        return df[['Артикул', 'Наименование товара']]
    except Exception as e:
        st.error(f"Ошибка при обработке Excel файла: {str(e)}")
        return pd.DataFrame()

# Функция для создания Excel-файла из DataFrame
def to_excel(df):
    try:
        df_copy = df.copy()
        datetime_cols = ['Дата', 'lastChangeDate']
        for col in datetime_cols:
            if col in df_copy.columns:
                df_copy[col] = df_copy[col].dt.tz_localize(None)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_copy.to_excel(writer, index=False, sheet_name='SalesData')
        return output.getvalue()
    except Exception as e:
        st.error(f"Ошибка при создании Excel файла: {str(e)}")
        return None

# Основной интерфейс
def main():
    st.title("🔍 Wildberries Analytics Pro")
    
    # URL данных
    json_url = "https://storage.yandexcloud.net/my-json-bucket-chat-wb/wb_dashboard/all_sales_data.json"
    excel_url = "https://storage.yandexcloud.net/my-json-bucket-chat-wb/14_04_2025_07_26_%D0%9E%D0%B1%D1%89%D0%B8%D0%B5_%D1%85%D0%B0%D1%80%D0%B0%D0%BA%D1%82%D0%B5%D1%80%D0%B8%D1%81%D1%82%D0%B8%D0%BA%D0%B8_%D0%BE%D0%B4%D0%BD%D0%B8%D0%BC_%D1%84%D0%B0%D0%B9%D0%BB%D0%BE%D0%BC.xlsx"
    
    with st.spinner("Загрузка данных..."):
        df = load_data(json_url)
        excel_df = load_excel_data(excel_url)
    
    if df.empty:
        st.warning("Не удалось загрузить данные из JSON. Пожалуйста, попробуйте позже.")
        return
    
    if excel_df.empty:
        st.warning("Не удалось загрузить данные из Excel. Анализ будет продолжен без наименований товаров.")
        excel_df = pd.DataFrame(columns=['Артикул', 'Наименование товара'])
    
    # Объединение данных
    try:
        df = pd.merge(df, excel_df, on='Артикул', how='left')
    except Exception as e:
        st.error(f"Ошибка при объединении данных: {str(e)}")
        return
    
    # Сброс кэша
    if st.button("Сбросить кэш"):
        st.cache_data.clear()
        st.experimental_rerun()
    
    # Сайдбар с фильтрами
    with st.sidebar:
        st.header("⏱ Период анализа")
        try:
            default_start = datetime(2025, 4, 9).date()
            default_end = datetime(2025, 4, 10).date()
            date_range = st.date_input(
                "Выберите даты",
                [default_start, default_end],
                format="DD.MM.YYYY"
            )
            if len(date_range) != 2:
                st.error("Пожалуйста, выберите диапазон дат")
                st.stop()
        except Exception as e:
            st.error(f"Ошибка при выборе даты: {str(e)}")
            st.stop()
        
        include_cancelled = st.checkbox("Учитывать отмены", value=False)
        
        st.header("🗂 Фильтры")
        try:
            warehouse_options = df['Склад'].unique().tolist()
            default_warehouse = warehouse_options[0] if len(warehouse_options) > 0 else None
            warehouse_type = st.multiselect(
                "Тип склада",
                options=warehouse_options,
                default=default_warehouse
            )
        except Exception as e:
            st.error(f"Ошибка при загрузке фильтров склада: {str(e)}")
            warehouse_type = []
    
    # Фильтрация данных
    try:
        filtered_df = df[
            (df['Дата'].dt.date >= date_range[0]) &
            (df['Дата'].dt.date <= date_range[1]) &
            (~df['is_return'])
        ]
        
        if not include_cancelled:
            filtered_df = filtered_df[~filtered_df['isCancel']]
            
        if warehouse_type:
            filtered_df = filtered_df[filtered_df['Склад'].isin(warehouse_type)]
            
    except Exception as e:
        st.error(f"Ошибка при фильтрации данных: {str(e)}")
        st.stop()
    
    # Проверка на пустые данные после фильтрации
    if filtered_df.empty:
        st.warning("Нет данных, соответствующих выбранным фильтрам")
        st.stop()
    
    # Диагностика данных
    st.subheader("🔍 Диагностика данных")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Всего записей", len(filtered_df))
    with col2:
        st.metric("Уникальных заказов", filtered_df['srid'].nunique())
    with col3:
        st.metric("Возвратов", filtered_df['is_return'].sum())
    
    # Ключевые показатели
    st.header("📊 Ключевые показатели")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        revenue = filtered_df['Выручка'].sum()
        st.metric("Выручка", f"{revenue:,.0f} ₽")
    with col2:
        order_count = filtered_df['srid'].nunique()
        avg_check = revenue / order_count if order_count > 0 else 0
        st.metric("Средний чек", f"{avg_check:,.0f} ₽")
    with col3:
        st.metric("Количество заказов", order_count)
    with col4:
        avg_spp = filtered_df['СПП'].mean()
        if not pd.isna(avg_spp):
            st.metric("Средний СПП", f"{np.ceil(avg_spp * 100) / 100:.2f}%")
        else:
            st.metric("Средний СПП", "N/A")
    
    # Вкладки с аналитикой
    tab1, tab2, tab3 = st.tabs(["📈 Динамика", "📦 Товары", "💰 Выручка"])
    
    with tab1:
        st.subheader("Динамика продаж")
        try:
            freq = st.radio("Группировка", ["День", "Неделя", "Месяц"], horizontal=True)
            freq_map = {"День": "D", "Неделя": "W", "Месяц": "ME"}
            dynamic_df = filtered_df.groupby(pd.Grouper(key='Дата', freq=freq_map[freq])).agg({
                'Выручка': 'sum',
                'srid': 'nunique'
            }).reset_index()
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=dynamic_df['Дата'],
                y=dynamic_df['Выручка'],
                name="Выручка",
                line=dict(color='#1f77b4', width=2)
            ))
            fig.add_trace(go.Scatter(
                x=dynamic_df['Дата'],
                y=dynamic_df['srid'],
                name="Количество заказов",
                line=dict(color='#ff7f0e', width=2),
                yaxis="y2"
            ))
            
            fig.update_layout(
                title=f"Динамика продаж по {freq.lower()}м",
                yaxis=dict(title="Выручка (₽)"),
                yaxis2=dict(title="Количество заказов", overlaying="y", side="right"),
                hovermode="x unified",
                legend=dict(orientation="h", y=1.1)
            )
            st.plotly_chart(fig, use_container_width=True)
            
        except Exception as e:
            st.error(f"Ошибка при построении динамики: {str(e)}")
    
    with tab2:
        st.subheader("Товарная аналитика")
        try:
            # Топ брендов
            top_brands = filtered_df.groupby('Бренд').agg({
                'Выручка': 'sum',
                'srid': 'nunique'
            }).nlargest(10, 'Выручка').reset_index()
            
            fig = px.bar(top_brands, x='Бренд', y='Выручка',
                        hover_data=['srid'],
                        labels={'srid': 'Заказов', 'Выручка': 'Выручка (₽)'},
                        title='Топ-10 брендов по выручке')
            st.plotly_chart(fig, use_container_width=True)
            
            # Топ товаров
            st.subheader("Топ товаров")
            top_items = filtered_df.groupby(['Бренд', 'Категория', 'Артикул', 'Наименование товара']).agg({
                'Выручка': 'sum',
                'srid': 'nunique',
                'Цена': 'mean'
            }).nlargest(20, 'Выручка').reset_index()
            
            st.dataframe(
                top_items.rename(columns={
                    'srid': 'Заказов',
                    'Цена': 'Средняя цена'
                }),
                height=600
            )
            
        except Exception as e:
            st.error(f"Ошибка при анализе товаров: {str(e)}")
    
    with tab3:
        st.subheader("Анализ выручки")
        try:
            # Выручка по категориям
            category_revenue = filtered_df.groupby('Категория').agg({
                'Выручка': 'sum',
                'srid': 'nunique'
            }).reset_index()
            category_revenue['Доля'] = (category_revenue['Выручка'] / revenue) * 100
            
            fig = px.pie(category_revenue, values='Выручка', names='Категория',
                        title='Распределение выручки по категориям',
                        hover_data=['Доля'])
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)
            
            # Детализация по часам (если выбран один день)
            if date_range[0] == date_range[1]:
                st.subheader("Выручка по часам")
                hourly_data = filtered_df.groupby(filtered_df['Дата'].dt.hour).agg({
                    'Выручка': 'sum',
                    'srid': 'nunique'
                }).reset_index().rename(columns={'Дата': 'Час'})
                
                if not hourly_data.empty:
                    fig = px.bar(hourly_data, x='Час', y='Выручка',
                                hover_data=['srid'],
                                labels={'srid': 'Заказов'},
                                title='Выручка по часам')
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("Нет данных для отображения почасовой статистики")
                    
        except Exception as e:
            st.error(f"Ошибка при анализе выручки: {str(e)}")
    
    # Экспорт данных
    with st.expander("📁 Экспорт данных"):
        st.subheader("Отфильтрованные данные")
        st.dataframe(filtered_df.head(1000))
        
        col1, col2 = st.columns(2)
        with col1:
            st.download_button(
                label="Скачать данные (Excel)",
                data=to_excel(filtered_df),
                file_name="wb_analytics.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        with col2:
            st.download_button(
                label="Скачать данные (CSV)",
                data=filtered_df.to_csv(index=False).encode('utf-8'),
                file_name="wb_analytics.csv",
                mime="text/csv"
            )

if __name__ == "__main__":
    main()
