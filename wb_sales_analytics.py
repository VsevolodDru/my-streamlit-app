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
import time

# Настройка окружения и логирования
os.environ['STREAMLIT_SERVER_ENABLE_WATCHER'] = 'false'
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация страницы
st.set_page_config(
    layout="wide",
    page_title="WB Analytics Pro",
    page_icon="📈",
    initial_sidebar_state="expanded"
)

# Константы
MAX_RETRIES = 3
RETRY_DELAY = 5
MAX_JSON_SIZE_MB = 500
JSON_LOAD_TIMEOUT = 600
CHUNK_SIZE = 1024 * 1024

# Хранилище для ошибок и уведомлений
if 'error_log' not in st.session_state:
    st.session_state['error_log'] = []

def log_error(message: str):
    """Добавляет сообщение об ошибке в лог"""
    logger.error(message)
    st.session_state['error_log'].append(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Ошибка: {message}")

def log_warning(message: str):
    """Добавляет предупреждение в лог"""
    logger.warning(message)
    st.session_state['error_log'].append(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Предупреждение: {message}")

class DataLoader:
    @staticmethod
    def load_with_retry(url: str, loader_func, **kwargs):
        for attempt in range(MAX_RETRIES):
            try:
                return loader_func(url, **kwargs)
            except Exception as e:
                error_msg = f"Попытка {attempt + 1} не удалась: {str(e)}\n{traceback.format_exc()}"
                log_error(error_msg)
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
        return pd.DataFrame()

    @staticmethod
    def load_large_json(url: str) -> pd.DataFrame:
        try:
            logger.info(f"Starting JSON load from {url}")
            
            with requests.head(url, timeout=10) as r:
                if r.status_code != 200:
                    log_error(f"URL недоступен. Код статуса: {r.status_code}")
                    return pd.DataFrame()

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
            
            try:
                data = json.loads(b''.join(chunks).decode('utf-8'))
            except json.JSONDecodeError as e:
                log_error(f"Ошибка формата JSON: {str(e)}")
                return pd.DataFrame()
            
            if not data:
                log_warning("Получен пустой JSON")
                return pd.DataFrame()
            
            try:
                df = pd.DataFrame(data)
                if df.empty:
                    log_warning("Данные отсутствуют в JSON")
                    return df
                
                datetime_cols = ['date', 'lastChangeDate']
                for col in datetime_cols:
                    if col in df.columns:
                        try:
                            df[col] = pd.to_datetime(df[col], errors='coerce')
                            if df[col].dt.tz is None:
                                df[col] = df[col].dt.tz_localize('Europe/Moscow')
                        except Exception as e:
                            log_warning(f"Ошибка обработки даты в колонке {col}: {str(e)}")
                            df[col] = pd.to_datetime(df[col], errors='coerce')
                
                df['is_return'] = df.get('srid', '').astype(str).str.startswith('R')
                df['revenue'] = df.get('totalPrice', 0)
                
                if 'date' in df.columns:
                    try:
                        df['week'] = df['date'].dt.isocalendar().week
                        df['month'] = df['date'].dt.month
                    except:
                        pass
                
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
                        df[col] = df[col].astype(str).str.strip()
                
                if 'Бренд' in df.columns:
                    df['Бренд'] = df['Бренд'].str.lower()
                
                if 'Артикул' in df.columns:
                    df['Артикул'] = df['Артикул'].apply(
                        lambda x: x[:len(x)//2] if isinstance(x, str) and len(x) == 20 and x[:10] == x[10:] else x
                    )

                logger.info(f"Успешно загружено {len(df)} записей")
                return df
                
            except Exception as e:
                log_error(f"Ошибка обработки данных: {str(e)}")
                return pd.DataFrame()
                
        except Exception as e:
            log_error(f"Критическая ошибка загрузки: {str(e)}\n{traceback.format_exc()}")
            return pd.DataFrame()
        finally:
            if 'progress_bar' in locals(): progress_bar.empty()
            if 'status_text' in locals(): status_text.empty()

    @staticmethod
    def load_excel_data(url: str) -> pd.DataFrame:
        try:
            logger.info(f"Starting Excel load from {url}")
            
            try:
                with requests.head(url, timeout=10) as r:
                    if r.status_code != 200:
                        log_error(f"Excel URL недоступен. Код статуса: {r.status_code}")
                        return pd.DataFrame()
            except requests.RequestException as e:
                log_error(f"Ошибка подключения к Excel URL: {str(e)}")
                return pd.DataFrame()

            response = requests.get(url, timeout=(30, 300))
            response.raise_for_status()
            
            with io.BytesIO(response.content) as excel_file:
                try:
                    df = pd.read_excel(
                        excel_file,
                        usecols=['Артикул продавца', 'Наименование'],
                        dtype={'Артикул продавца': 'string', 'Наименование': 'string'}
                    )
                except Exception as e:
                    log_error(f"Ошибка чтения Excel: {str(e)}")
                    return pd.DataFrame()
            
            if df.empty:
                log_warning("Excel файл пуст")
                return df
            
            required_cols = ['Артикул продавца', 'Наименование']
            if not all(col in df.columns for col in required_cols):
                log_error("В Excel отсутствуют необходимые колонки")
                return pd.DataFrame()
            
            df = df.rename(columns={
                'Артикул продавца': 'Артикул',
                'Наименование': 'Наименование товара'
            })
            
            df['Артикул'] = df['Артикул'].astype(str).str.strip()
            df['Наименование товара'] = df['Наименование товара'].astype(str).str.strip()
            
            logger.info(f"Excel данные загружены. Размер: {len(df)} строк")
            return df[['Артикул', 'Наименование товара']].drop_duplicates(subset=['Артикул'])
        
        except Exception as e:
            log_error(f"Ошибка загрузки Excel: {str(e)}\n{traceback.format_exc()}")
            return pd.DataFrame()

def to_excel(df: pd.DataFrame) -> bytes:
    """Конвертирует DataFrame в Excel файл в памяти"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

def main():
    # Инициализация состояния
    if 'data_loaded' not in st.session_state:
        st.session_state.update({
            'data_loaded': False,
            'load_error': None,
            'df': pd.DataFrame(),
            'excel_df': pd.DataFrame(),
            'filtered_df': pd.DataFrame(),
            'previous_fbs': True,
            'previous_fbo': True
        })

    st.title("🔍 Wildberries Analytics Pro")
    
    # URL для загрузки данных
    DATA_SOURCES = {
        "fbs": "https://storage.yandexcloud.net/my-json-bucket-chat-wb/wb_dashboard/all_sales_data.json",
        "fbo": "https://storage.yandexcloud.net/my-json-bucket-chat-wb/wb_dashboard/all_sales_data_FBO.json",
        "excel": "https://storage.yandexcloud.net/my-json-bucket-chat-wb/14_04_2025_07_26_%D0%9E%D0%B1%D1%89%D0%B8%D0%B5_%D1%85%D0%B0%D1%80%D0%B0%D0%BA%D1%82%D0%B5%D1%80%D0%B8%D1%81%D1%82%D0%B8%D0%BA%D0%B8_%D0%BE%D0%B4%D0%BD%D0%B8%D0%BC_%D1%84%D0%B0%D0%B9%D0%BB%D0%BE%D0%BC.xlsx"
    }

    # Фильтры в сайдбаре
    with st.sidebar:
        st.header("⏱ Период анализа")
        
        # Фильтр по схемам продаж
        st.header("📦 Схемы продаж")
        use_fbs = st.checkbox("FBS", value=st.session_state.get('previous_fbs', True), key="use_fbs")
        use_fbo = st.checkbox("FBO", value=st.session_state.get('previous_fbo', True), key="use_fbo")

        if not (use_fbs or use_fbo):
            st.warning("Выберите хотя бы одну схему продаж для загрузки данных.")
            st.stop()

        # Проверяем, изменились ли чекбоксы, чтобы перезагрузить данные
        if (use_fbs != st.session_state.get('previous_fbs', True) or 
            use_fbo != st.session_state.get('previous_fbo', True)):
            st.session_state.update({
                'data_loaded': False,
                'load_error': None,
                'previous_fbs': use_fbs,
                'previous_fbo': use_fbo
            })

    # Загрузка данных
    if not st.session_state.data_loaded and st.session_state.load_error is None:
        with st.spinner("Загрузка данных. Пожалуйста, подождите..."):
            try:
                combined_data = pd.DataFrame()

                if use_fbs:
                    fbs_data = DataLoader.load_with_retry(DATA_SOURCES["fbs"], DataLoader.load_large_json)
                    if not fbs_data.empty:
                        fbs_data['Схема продаж'] = 'FBS'
                        combined_data = pd.concat([combined_data, fbs_data], ignore_index=True)
                        logger.info(f"Загружены данные FBS: {len(fbs_data)} записей")
                    else:
                        log_warning("Не удалось загрузить данные по FBS")

                if use_fbo:
                    fbo_data = DataLoader.load_with_retry(DATA_SOURCES["fbo"], DataLoader.load_large_json)
                    if not fbo_data.empty:
                        fbo_data['Схема продаж'] = 'FBO'
                        combined_data = pd.concat([combined_data, fbo_data], ignore_index=True)
                        logger.info(f"Загружены данные FBO: {len(fbo_data)} записей")
                    else:
                        log_warning("Не удалось загрузить данные по FBO")

                if not combined_data.empty:
                    excel_data = DataLoader.load_with_retry(DATA_SOURCES["excel"], DataLoader.load_excel_data)
                    
                    if not excel_data.empty:
                        try:
                            merged_df = pd.merge(
                                combined_data,
                                excel_data,
                                on='Артикул',
                                how='left'
                            )
                            st.session_state.update({
                                'df': merged_df,
                                'excel_df': excel_data,
                                'data_loaded': True,
                                'load_error': None
                            })
                        except Exception as e:
                            st.session_state.load_error = f"Ошибка объединения данных: {str(e)}"
                            log_error(f"Ошибка объединения данных: {str(e)}")
                    else:
                        st.session_state.update({
                            'df': combined_data,
                            'data_loaded': True,
                            'load_error': "Не удалось загрузить Excel данные"
                        })
                        log_warning("Не удалось загрузить Excel данные")
                else:
                    st.session_state.load_error = "Не удалось загрузить основные данные"
                    log_error("Не удалось загрузить основные данные")
                    
            except Exception as e:
                st.session_state.load_error = f"Критическая ошибка: {str(e)}"
                log_error(f"Критическая ошибка: {str(e)}\n{traceback.format_exc()}")

    # Обработка ошибок загрузки
    if st.session_state.load_error:
        st.warning("Произошла ошибка при загрузке данных. Проверьте вкладку 'Логи и ошибки'.")
        
        if st.button("Попробовать снова"):
            st.session_state.update({
                'data_loaded': False,
                'load_error': None
            })
            st.rerun()
        
        st.stop()
    
    if not st.session_state.data_loaded:
        st.warning("Данные еще загружаются...")
        st.stop()

    # Получаем данные из session state
    df = st.session_state.df
    excel_df = st.session_state.excel_df

    # Кнопка перезагрузки данных
    if st.button("🔄 Обновить данные"):
        st.session_state.update({
            'data_loaded': False,
            'load_error': None
        })
        st.rerun()

    # Безопасное получение дат
    try:
        if not df.empty and 'Дата' in df.columns:
            min_date = df['Дата'].min().date()
            max_date = df['Дата'].max().date()
        else:
            min_date = max_date = date.today()
            log_warning("Используются даты по умолчанию из-за отсутствия данных")
    except Exception as e:
        log_error(f"Ошибка получения дат: {str(e)}")
        min_date = max_date = date.today()
        log_warning("Используются даты по умолчанию из-за ошибки")

    # Фильтры в сайдбаре (продолжение)
    with st.sidebar:
        try:
            date_range = st.date_input(
                "Выберите даты",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date,
                format="DD.MM.YYYY",
                key="date_range"
            )
            
            if len(date_range) != 2:
                st.warning("Пожалуйста, выберите обе даты")
                st.stop()
                
            start_date, end_date = date_range
            if start_date > end_date:
                start_date, end_date = end_date, start_date
                st.warning("Автоматически исправлен порядок дат")
                
        except Exception as e:
            log_error(f"Ошибка выбора даты: {str(e)}")
            st.warning("Ошибка при выборе дат. Используются даты по умолчанию.")
            start_date, end_date = min_date, max_date
        
        include_cancelled = st.checkbox("Учитывать отмены", False, key="include_cancelled")
        
        st.header("🗂 Фильтры")
        
        warehouse_col = None
        if not df.empty:
            warehouse_col = next((col for col in ['Тип склада', 'Склад'] if col in df.columns), None)
        
        if warehouse_col:
            warehouse_options = df[warehouse_col].unique().tolist()
            selected_warehouses = st.multiselect(
                "Тип склада",
                options=warehouse_options,
                default=warehouse_options[:1] if warehouse_options else [],
                key="warehouse_filter"
            )
        else:
            selected_warehouses = []
            st.warning("Данные о складах отсутствуют")

    # Вкладки с аналитикой и логами
    tab1, tab2, tab3 = st.tabs(["📈 Динамика продаж", "🔍 Детальный анализ", "🔔 Логи и ошибки"])

    with tab3:
        st.subheader("Логи и ошибки")
        if st.session_state['error_log']:
            for log_entry in st.session_state['error_log']:
                st.text(log_entry)
        else:
            st.info("Ошибок и предупреждений пока нет.")
        
        if st.button("Очистить логи"):
            st.session_state['error_log'] = []
            st.rerun()

    # Применение фильтров
    if st.button("Применить фильтры") or 'filtered_df' not in st.session_state:
        with st.spinner("Применение фильтров..."):
            try:
                if df.empty:
                    st.session_state.filtered_df = pd.DataFrame()
                    st.warning("Нет данных для фильтрации")
                else:
                    filtered = df.copy()
                    
                    # Фильтр по дате
                    if 'Дата' in filtered.columns:
                        filtered = filtered[
                            (filtered['Дата'].dt.date >= start_date) & 
                            (filtered['Дата'].dt.date <= end_date)
                        ]
                    
                    # Дополнительные фильтры
                    if not include_cancelled and 'isCancel' in filtered.columns:
                        filtered = filtered[~filtered['isCancel']]
                    
                    if 'is_return' in filtered.columns:
                        filtered = filtered[~filtered['is_return']]
                    
                    if selected_warehouses and warehouse_col and warehouse_col in filtered.columns:
                        filtered = filtered[filtered[warehouse_col].isin(selected_warehouses)]
                    
                    st.session_state.filtered_df = filtered if not filtered.empty else pd.DataFrame()
                    
                    if st.session_state.filtered_df.empty:
                        st.warning("Нет данных по выбранным фильтрам")
                    else:
                        st.success(f"Загружено {len(st.session_state.filtered_df)} записей")
                        
            except Exception as e:
                log_error(f"Ошибка фильтрации: {str(e)}\n{traceback.format_exc()}")
                st.warning("Ошибка при фильтрации данных. Проверьте вкладку 'Логи и ошибки'.")

    # Получаем отфильтрованные данные
    filtered_df = st.session_state.get('filtered_df', pd.DataFrame())
    
    if filtered_df.empty:
        st.warning("Нет данных для отображения. Измените параметры фильтров.")
        st.stop()

    # Отображение аналитики
    st.header("📊 Ключевые показатели")
    
    try:
        # Расчет показателей
        revenue = filtered_df['Выручка'].sum() if 'Выручка' in filtered_df.columns else 0
        order_count = filtered_df['srid'].nunique() if 'srid' in filtered_df.columns else 0
        avg_check = revenue / order_count if order_count > 0 else 0
        avg_spp = filtered_df['СПП'].mean() if 'СПП' in filtered_df.columns else 0
        
        # Отображение метрик
        cols = st.columns(4)
        cols[0].metric("Выручка", f"{revenue:,.0f} ₽")
        cols[1].metric("Средний чек", f"{avg_check:,.0f} ₽")
        cols[2].metric("Количество заказов", order_count)
        cols[3].metric("Средний СПП", f"{avg_spp:.2f}%" if not pd.isna(avg_spp) else "N/A")
        
    except Exception as e:
        log_error(f"Ошибка расчета показателей: {str(e)}")
        st.warning("Ошибка при расчете показателей. Проверьте вкладку 'Логи и ошибки'.")

    with tab1:
        st.subheader("Динамика продаж")
        
        try:
            if 'Дата' not in filtered_df.columns or 'Выручка' not in filtered_df.columns:
                st.warning("Отсутствуют данные для построения графика")
            else:
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
                    line=dict(color='blue')
                ))
                fig.add_trace(go.Scatter(
                    x=dynamic_df['Дата'],
                    y=dynamic_df['srid'],
                    name="Заказы",
                    line=dict(color='orange'),
                    yaxis="y2"
                ))
                
                fig.update_layout(
                    title=f"Динамика по {freq.lower()}м",
                    yaxis=dict(title="Выручка (₽)"),
                    yaxis2=dict(title="Количество заказов", overlaying="y", side="right"),
                    hovermode="x unified"
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
        except Exception as e:
            log_error(f"Ошибка построения графика: {str(e)}")
            st.warning("Ошибка при отображении динамики продаж. Проверьте вкладку 'Логи и ошибки'.")

    with tab2:
        st.subheader("Детальный анализ")
        
        try:
            if filtered_df.empty:
                st.warning("Нет данных для анализа")
            else:
                # Выбор типа анализа
                analysis_type = st.selectbox(
                    "Тип анализа",
                    options=['Категория', 'Бренд', 'Подкатегория', 'Склад', 'Схема продаж'],
                    index=0
                )
                
                if analysis_type in filtered_df.columns:
                    # Группировка данных
                    analysis_df = filtered_df.groupby(analysis_type).agg({
                        'Выручка': 'sum',
                        'srid': 'nunique',
                        'СПП': 'mean'
                    }).reset_index()
                    
                    analysis_df.columns = [analysis_type, 'Выручка', 'Количество заказов', 'Средний СПП']
                    analysis_df['Средний чек'] = analysis_df['Выручка'] / analysis_df['Количество заказов']
                    analysis_df['Доля выручки'] = (analysis_df['Выручка'] / analysis_df['Выручка'].sum()) * 100
                    
                    # Сортировка
                    analysis_df = analysis_df.sort_values('Выручка', ascending=False)
                    
                    # Отображение таблицы
                    st.dataframe(
                        analysis_df.style.format({
                            'Выручка': '{:,.0f} ₽',
                            'Средний чек': '{:,.0f} ₽',
                            'Доля выручки': '{:.1f}%',
                            'Средний СПП': '{:.1f}%'
                        }),
                        height=600,
                        use_container_width=True
                    )
                    
                    # Визуализация
                    fig = px.bar(
                        analysis_df.head(20),
                        x=analysis_type,
                        y='Выручка',
                        hover_data=['Количество заказов', 'Средний чек', 'Доля выручки'],
                        title=f"Топ 20 {analysis_type.lower()} по выручке"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Дополнительная информация по артикулам для категорий и подкатегорий
                    if analysis_type in ['Категория', 'Подкатегория']:
                        st.subheader(f"Детализация по артикулам ({analysis_type})")
                        
                        # Выбор конкретной категории/подкатегории
                        selected_item = st.selectbox(
                            f"Выберите {analysis_type.lower()} для детализации",
                            options=analysis_df[analysis_type].unique(),
                            index=0
                        )
                        
                        # Фильтрация данных по выбранной категории/подкатегории
                        item_data = filtered_df[filtered_df[analysis_type] == selected_item]
                        
                        if not item_data.empty:
                            # Анализ по артикулам
                            articles_df = item_data.groupby(['Артикул', 'Наименование товара']).agg({
                                'Выручка': 'sum',
                                'srid': 'nunique',
                                'СПП': 'mean'
                            }).reset_index()
                            
                            articles_df.columns = ['Артикул', 'Наименование товара', 'Выручка', 'Количество заказов', 'Средний СПП']
                            articles_df['Средний чек'] = articles_df['Выручка'] / articles_df['Количество заказов']
                            articles_df['Доля выручки'] = (articles_df['Выручка'] / articles_df['Выручка'].sum()) * 100
                            
                            # Сортировка
                            articles_df = articles_df.sort_values('Выручка', ascending=False)
                            
                            # Отображение таблицы
                            st.dataframe(
                                articles_df.style.format({
                                    'Выручка': '{:,.0f} ₽',
                                    'Средний чек': '{:,.0f} ₽',
                                    'Доля выручки': '{:.1f}%',
                                    'Средний СПП': '{:.1f}%'
                                }),
                                height=600,
                                use_container_width=True
                            )
                            
                            # Визуализация
                            fig = px.bar(
                                articles_df.head(20),
                                x='Наименование товара',
                                y='Выручка',
                                hover_data=['Артикул', 'Количество заказов', 'Средний чек', 'Доля выручки'],
                                title=f"Топ 20 товаров по выручке ({selected_item})"
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.warning(f"Нет данных по выбранной {analysis_type.lower()}")
                else:
                    st.warning(f"В данных отсутствует колонка {analysis_type}")
                
        except Exception as e:
            log_error(f"Ошибка детального анализа: {str(e)}")
            st.warning("Ошибка при выполнении анализа. Проверьте вкладку 'Логи и ошибки'.")

    # Экспорт данных
    with st.expander("📁 Экспорт данных"):
        st.subheader("Отфильтрованные данные")
        
        if filtered_df.empty:
            st.warning("Нет данных для экспорта")
        else:
            st.dataframe(filtered_df.head(1000), height=400)
            
            # Кнопки экспорта
            col1, col2 = st.columns(2)
            
            with col1:
                st.download_button(
                    label="Скачать Excel",
                    data=to_excel(filtered_df),
                    file_name="wb_data.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            
            with col2:
                st.download_button(
                    label="Скачать CSV",
                    data=filtered_df.to_csv(index=False).encode('utf-8'),
                    file_name="wb_data.csv",
                    mime="text/csv"
                )

if __name__ == "__main__":
    main()
