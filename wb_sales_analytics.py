import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
from datetime import datetime, date, timedelta
import pytz
import numpy as np
import io
from openpyxl import Workbook
import requests
import gc
import logging
from typing import Optional, Tuple
import tempfile
import os
import traceback

# Отключаем watcher для решения проблемы с inotify
os.environ['STREAMLIT_SERVER_ENABLE_WATCHER'] = 'false'

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация страницы
st.set_page_config(
    layout="wide",
    page_title="WB Analytics Pro",
    page_icon="📈",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://example.com',
        'Report a bug': "https://example.com",
        'About': "# Оптимизированная аналитика Wildberries"
    }
)

# Константы
MAX_JSON_SIZE_MB = 500
JSON_LOAD_TIMEOUT = 600
CHUNK_SIZE = 1024 * 1024

# Глобальные переменные
global_df = None
global_excel_df = None

@st.cache_data(ttl=3600, max_entries=3, show_spinner="Загрузка данных...")
def load_large_json(url: str) -> pd.DataFrame:
    try:
        logger.info(f"Начало загрузки JSON файла из {url}")
        
        with requests.head(url, timeout=10) as r:
            size_mb = int(r.headers.get('content-length', 0)) / (1024 * 1024)
            if size_mb > MAX_JSON_SIZE_MB:
                st.warning(f"⚠️ Файл очень большой ({size_mb:.1f} МБ). Загрузка может занять несколько минут...")

        progress_bar = st.progress(0)
        status_text = st.empty()
        
        response = requests.get(url, stream=True, timeout=(30, JSON_LOAD_TIMEOUT))
        response.raise_for_status()
        
        chunks = []
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        
        for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
            chunks.append(chunk)
            downloaded += len(chunk)
            progress = min(downloaded / total_size, 1.0) if total_size > 0 else 0
            progress_bar.progress(progress)
            status_text.text(f"Загружено: {downloaded/(1024*1024):.1f} МБ / {total_size/(1024*1024):.1f} МБ")
        
        status_text.text("Обработка JSON...")
        data = json.loads(b''.join(chunks).decode('utf-8'))
        df = pd.DataFrame(data)
        
        datetime_cols = ['date', 'lastChangeDate']
        for col in datetime_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
                if df[col].dt.tz is None:
                    df[col] = df[col].dt.tz_localize('Europe/Moscow')
        
        df['is_return'] = df.get('srid', '').str.startswith('R')
        df['revenue'] = df['totalPrice']
        if 'date' in df.columns:
            df['week'] = df['date'].dt.isocalendar().week
            df['month'] = df['date'].dt.month
        df['isCancel'] = df.get('isCancel', False)

        column_mapping = {
            'date': 'Дата',
            'warehouseName': 'Склад',
            'warehouse': 'Склад',
            'warehouseType': 'Тип склада',
            'regionName': 'Регион',
            'category': 'Категория',
            'brand': 'Бренд',
            'subject': 'Подкатегория',
            'totalPrice': 'Цена',
            'revenue': 'Выручка',
            'spp': 'СПП',
            'supplierArticle': 'Артикул'
        }
        df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})

        str_cols = ['Бренд', 'Артикул', 'Категория', 'Подкатегория', 'Склад', 'Тип склада']
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].astype('string')
        
        if 'Бренд' in df.columns:
            df['Бренд'] = df['Бренд'].str.lower()
        
        if 'Артикул' in df.columns:
            df['Артикул'] = df['Артикул'].apply(
                lambda x: x[:len(x)//2] if isinstance(x, str) and len(x) == 20 and x[:10] == x[10:] else x
            )

        logger.info(f"Успешно загружено {len(df)} записей")
        return df
        
    except Exception as e:
        logger.error(f"Ошибка загрузки: {str(e)}\n{traceback.format_exc()}")
        st.error(f"Ошибка при загрузке данных: {str(e)}")
        return pd.DataFrame()
    finally:
        if 'progress_bar' in locals(): progress_bar.empty()
        if 'status_text' in locals(): status_text.empty()

@st.cache_data(ttl=3600, max_entries=2)
def load_excel_data(url: str) -> pd.DataFrame:
    try:
        logger.info("Начало загрузки Excel данных")
        response = requests.get(url, timeout=(30, 300))
        response.raise_for_status()
        
        with io.BytesIO(response.content) as excel_file:
            df = pd.read_excel(
                excel_file,
                usecols=['Артикул продавца', 'Наименование'],
                dtype={'Артикул продавца': 'string', 'Наименование': 'string'}
            )
        
        if 'Артикул продавца' not in df.columns or 'Наименование' not in df.columns:
            raise ValueError("Отсутствуют обязательные колонки в Excel файле")
        
        df = df.rename(columns={
            'Артикул продавца': 'Артикул',
            'Наименование': 'Наименование товара'
        })
        
        logger.info(f"Excel данные загружены. Размер: {len(df)} строк")
        return df[['Артикул', 'Наименование товара']]
    
    except Exception as e:
        logger.error(f"Ошибка загрузки Excel: {str(e)}\n{traceback.format_exc()}")
        st.error(f"Ошибка при обработке Excel файла: {str(e)}")
        return pd.DataFrame()

def to_excel(df: pd.DataFrame) -> bytes:
    """Конвертирует DataFrame в Excel с оптимизацией памяти."""
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
        logger.error(f"Ошибка экспорта в Excel: {str(e)}\n{traceback.format_exc()}")
        st.error(f"Ошибка при создании Excel файла: {str(e)}")
        raise

def apply_filters(df: pd.DataFrame, date_range: Tuple[date, date], 
                 include_cancelled: bool, warehouse_type: list) -> pd.DataFrame:
    try:
        if df.empty or 'Дата' not in df.columns:
            return pd.DataFrame()
            
        start_date, end_date = date_range
        if start_date > end_date:
            start_date, end_date = end_date, start_date
            
        filtered = df[
            (df['Дата'].dt.date >= start_date) &
            (df['Дата'].dt.date <= end_date) &
            (~df['is_return'])
        ].copy()
        
        if not include_cancelled:
            filtered = filtered[~filtered['isCancel']]
            
        if warehouse_type:
            warehouse_col = 'Тип склада' if 'Тип склада' in filtered.columns else 'Склад'
            if warehouse_col in filtered.columns:
                filtered = filtered[filtered[warehouse_col].isin(warehouse_type)]
        
        for col in filtered.select_dtypes(include=['object']):
            filtered[col] = filtered[col].astype('string')
            
        return filtered
    except Exception as e:
        logger.error(f"Ошибка фильтрации: {str(e)}\n{traceback.format_exc()}")
        st.error(f"Ошибка при фильтрации данных: {str(e)}")
        return pd.DataFrame()

def main():
    global global_df, global_excel_df
    
    st.title("🔍 Wildberries Analytics Pro (Large Files Support)")
    
    # Инициализация session state
    if 'filtered_df' not in st.session_state:
        st.session_state.filtered_df = pd.DataFrame()
    
    json_url = "https://storage.yandexcloud.net/my-json-bucket-chat-wb/wb_dashboard/all_sales_data.json"
    excel_url = "https://storage.yandexcloud.net/my-json-bucket-chat-wb/14_04_2025_07_26_%D0%9E%D0%B1%D1%89%D0%B8%D0%B5_%D1%85%D0%B0%D1%80%D0%B0%D0%BA%D1%82%D0%B5%D1%80%D0%B8%D1%81%D1%82%D0%B8%D0%BA%D0%B8_%D0%BE%D0%B4%D0%BD%D0%B8%D0%BC_%D1%84%D0%B0%D0%B9%D0%BB%D0%BE%D0%BC.xlsx"
    
    if 'data_loaded' not in st.session_state:
        with st.spinner("Загрузка и обработка данных (это может занять время для больших файлов)..."):
            try:
                global_df = load_large_json(json_url)
                
                if global_df is not None and not global_df.empty:
                    global_excel_df = load_excel_data(excel_url)
                    
                    if global_excel_df is not None and not global_excel_df.empty:
                        duplicates = global_excel_df.duplicated(subset=['Артикул']).sum()
                        if duplicates > 0:
                            st.warning(f"Найдено {duplicates} дубликатов артикулов в Excel файле. Будет использовано первое значение.")
                            global_excel_df = global_excel_df.drop_duplicates(subset=['Артикул'], keep='first')
                        
                        global_df = pd.merge(
                            global_df,
                            global_excel_df,
                            on='Артикул',
                            how='left'
                        )
                        st.session_state.data_loaded = True
            except Exception as e:
                st.error(f"Ошибка при загрузке данных: {str(e)}")
                return
    
    if global_df is None or global_df.empty:
        st.warning("Не удалось загрузить данные. Пожалуйста, попробуйте позже.")
        return
    
    if st.button("🔄 Сбросить кэш и перезагрузить данные"):
        st.cache_data.clear()
        st.session_state.clear()
        global_df = None
        global_excel_df = None
        st.experimental_rerun()
    
    # Безопасное получение дат
    try:
        min_date = global_df['Дата'].min().date() if not global_df.empty and 'Дата' in global_df.columns else date.today()
        max_date = global_df['Дата'].max().date() if not global_df.empty and 'Дата' in global_df.columns else date.today()
    except:
        min_date = max_date = date.today()
    
    with st.sidebar:
        st.header("⏱ Период анализа")
        try:
            date_range = st.date_input(
                "Выберите даты",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date,
                format="DD.MM.YYYY",
                key="date_range_selector"
            )
            
            if len(date_range) != 2:
                st.warning("Пожалуйста, выберите диапазон дат (начало и конец)")
                st.stop()
                
            start_date, end_date = date_range
            if start_date > end_date:
                st.warning("Дата начала позже даты окончания. Автоматически исправлено.")
                start_date, end_date = end_date, start_date
                
        except Exception as e:
            logger.error(f"Ошибка выбора даты: {str(e)}\n{traceback.format_exc()}")
            st.error("Ошибка при выборе дат. Пожалуйста, попробуйте еще раз.")
            st.stop()
        
        include_cancelled = st.checkbox("Учитывать отмены", value=False, key="include_cancelled")
        st.header("🗂 Фильтры")
        
        warehouse_col = 'Тип склада' if 'Тип склада' in global_df.columns else 'Склад'
        warehouse_options = global_df[warehouse_col].unique() if warehouse_col in global_df.columns else []
        
        warehouse_type = st.multiselect(
            "Тип склада",
            options=warehouse_options,
            default=warehouse_options[0] if len(warehouse_options) > 0 else [],
            key="warehouse_filter"
        )
    
    # Применение фильтров
    if st.button("Применить фильтры") or 'filtered_df' not in st.session_state or st.session_state.filtered_df.empty:
        with st.spinner("Применение фильтров..."):
            try:
                filtered_data = apply_filters(
                    global_df,
                    (start_date, end_date),
                    include_cancelled,
                    warehouse_type
                )
                
                if not filtered_data.empty:
                    st.session_state.filtered_df = filtered_data
                    st.success("Фильтры успешно применены!")
                else:
                    st.warning("Нет данных по выбранным фильтрам")
                    st.session_state.filtered_df = pd.DataFrame()
                    
            except Exception as e:
                logger.error(f"Ошибка при фильтрации: {str(e)}\n{traceback.format_exc()}")
                st.error("Ошибка при применении фильтров. Пожалуйста, проверьте параметры.")
                st.stop()
    
    filtered_df = st.session_state.get('filtered_df', pd.DataFrame())
    
    if filtered_df.empty:
        st.warning("Нет данных для отображения. Измените параметры фильтров.")
        st.stop()
    
    # Отображение данных
    st.subheader("🔍 Диагностика данных")
    cols = st.columns(3)
    cols[0].metric("Всего записей", len(filtered_df))
    cols[1].metric("Уникальных заказов", filtered_df['srid'].nunique() if 'srid' in filtered_df.columns else 0)
    cols[2].metric("Дубликатов srid", filtered_df.duplicated(subset=['srid']).sum() if 'srid' in filtered_df.columns else 0)
    
    st.header("📊 Ключевые показатели")
    
    with st.spinner("Расчет показателей..."):
        try:
            revenue = filtered_df['Выручка'].sum() if 'Выручка' in filtered_df.columns else 0
            order_count = filtered_df['srid'].nunique() if 'srid' in filtered_df.columns else 0
            avg_check = revenue / order_count if order_count > 0 else 0
            avg_spp = filtered_df['СПП'].mean() if 'СПП' in filtered_df.columns else 0
            
            cols = st.columns(4)
            cols[0].metric("Выручка", f"{revenue:,.0f} ₽")
            cols[1].metric("Средний чек", f"{avg_check:,.0f} ₽")
            cols[2].metric("Количество заказов", order_count)
            cols[3].metric("Средний СПП", 
                          f"{avg_spp:.2f}%" if not pd.isna(avg_spp) else "N/A",
                          help="Средний процент скидки по продажам")
        except Exception as e:
            logger.error(f"Ошибка расчета метрик: {str(e)}\n{traceback.format_exc()}")
            st.error("Ошибка при расчете показателей")
    
    tab1, tab2 = st.tabs(["📈 Динамика продаж", "💰 Детализация выручки"])
    
    with tab1:
        st.subheader("Динамика продаж")
        try:
            freq = st.radio("Группировка", ["День", "Неделя", "Месяц"], 
                          horizontal=True, key="freq_selector")
            freq_map = {"День": "D", "Неделя": "W", "Месяц": "ME"}
            
            with st.spinner("Построение графика..."):
                if 'Дата' in filtered_df.columns and 'Выручка' in filtered_df.columns:
                    dynamic_df = filtered_df.groupby(
                        pd.Grouper(key='Дата', freq=freq_map[freq])
                    ).agg({
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
                        name="Заказы",
                        line=dict(color='#ff7f0e', width=2),
                        yaxis="y2"
                    ))
                    
                    fig.update_layout(
                        title=f"Динамика по {freq.lower()}м",
                        yaxis=dict(title="Выручка (₽)"),
                        yaxis2=dict(title="Количество заказов", overlaying="y", side="right"),
                        hovermode="x unified",
                        legend=dict(orientation="h", y=1.1)
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("Отсутствуют необходимые данные для построения графика")
                
        except Exception as e:
            logger.error(f"Ошибка построения графика: {str(e)}\n{traceback.format_exc()}")
            st.error("Ошибка при отображении динамики продаж")
    
    with tab2:
        st.subheader("Детализация выручки")
        
        if st.checkbox("Показать детализацию", True, key="show_details"):
            with st.spinner("Анализ выручки..."):
                try:
                    if 'Выручка' not in filtered_df.columns:
                        st.warning("Нет данных о выручке для анализа")
                        return
                        
                    total_revenue = filtered_df['Выручка'].sum()
                    
                    def display_revenue_analysis(df, group_col, title):
                        if group_col not in df.columns:
                            st.warning(f"Отсутствует колонка {group_col} для анализа")
                            return pd.DataFrame()
                            
                        analysis_df = df.groupby(group_col).agg({
                            'Выручка': ['sum', 'count'],
                            'СПП': 'mean'
                        }).reset_index()
                        
                        analysis_df.columns = [group_col, 'Выручка', 'Количество', 'Средний СПП']
                        analysis_df['Доля'] = (analysis_df['Выручка'] / total_revenue) * 100
                        analysis_df['Средний СПП'] = analysis_df['Средний СПП'].round(2)
                        
                        st.subheader(title)
                        fig = px.bar(
                            analysis_df,
                            x=group_col,
                            y='Выручка',
                            hover_data=['Доля', 'Средний СПП'],
                            labels={'Выручка': 'Выручка, ₽'},
                            title=title
                        )
                        st.plotly_chart(fig, use_container_width=True)
                        
                        analysis_df = analysis_df.sort_values('Выручка', ascending=False)
                        st.dataframe(analysis_df)
                        
                        st.download_button(
                            label=f"Скачать {title.lower()}",
                            data=to_excel(analysis_df),
                            file_name=f"{title.replace(' ', '_')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"download_{group_col}"
                        )
                        return analysis_df
                    
                    if 'Категория' in filtered_df.columns:
                        cat_df = display_revenue_analysis(filtered_df, 'Категория', "Выручка по категориям")
                        if not cat_df.empty:
                            selected_cat = st.selectbox("Выберите категорию", cat_df['Категория'].unique())
                            
                            if 'Подкатегория' in filtered_df.columns:
                                cat_details = filtered_df[filtered_df['Категория'] == selected_cat]
                                display_revenue_analysis(cat_details, 'Подкатегория', 
                                                       f"Выручка по подкатегориям ({selected_cat})")
                    
                    if 'Бренд' in filtered_df.columns:
                        display_revenue_analysis(filtered_df, 'Бренд', "Выручка по брендам")
                    
                    if date_range[0] == date_range[1] and 'Дата' in filtered_df.columns:
                        st.subheader("Почасовая аналитика")
                        hourly_df = filtered_df.groupby(filtered_df['Дата'].dt.hour).agg({
                            'Выручка': 'sum',
                            'srid': 'nunique'
                        }).reset_index().rename(columns={'Дата': 'Час'})
                        
                        fig = px.bar(
                            hourly_df,
                            x='Час',
                            y='Выручка',
                            hover_data=['srid'],
                            labels={'srid': 'Количество заказов'},
                            title='Выручка по часам'
                        )
                        st.plotly_chart(fig, use_container_width=True)
                        
                except Exception as e:
                    logger.error(f"Ошибка детализации: {str(e)}\n{traceback.format_exc()}")
                    st.error("Ошибка при анализе выручки")
    
    with st.expander("📁 Экспорт данных", expanded=False):
        st.subheader("Отфильтрованные данные")
        st.dataframe(
            filtered_df.head(1000),
            height=400,
            use_container_width=True
        )
        
        cols = st.columns(2)
        cols[0].download_button(
            label="📥 Excel (оптимизированный)",
            data=to_excel(filtered_df),
            file_name="wb_analytics.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_excel"
        )
        cols[1].download_button(
            label="📥 CSV (сжатый)",
            data=filtered_df.to_csv(index=False, encoding='utf-8').encode('utf-8'),
            file_name="wb_analytics.csv",
            mime="text/csv",
            key="download_csv"
        )

if __name__ == "__main__":
    main()
