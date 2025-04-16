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
import gc
import logging
from typing import Optional, Tuple
import tempfile
import ijson  # Для потоковой обработки JSON

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация страницы
st.set_page_config(
    layout="wide",
    page_title="WB Analytics Pro (Optimized)",
    page_icon="📈",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://example.com',
        'Report a bug': "https://example.com",
        'About': "# Оптимизированная аналитика Wildberries с поддержкой больших файлов"
    }
)

# Константы
MAX_JSON_SIZE_MB = 500  # Лимит для предупреждения
JSON_LOAD_TIMEOUT = 600  # 10 минут
CHUNK_SIZE = 1024 * 1024  # 1MB для потоковой обработки

# Глобальные переменные с аннотацией типов
global_df: Optional[pd.DataFrame] = None
global_excel_df: Optional[pd.DataFrame] = None

# Оптимизированная загрузка больших JSON файлов
@st.cache_data(ttl=3600, max_entries=3, show_spinner="Загрузка данных...")
def load_large_json(url: str) -> pd.DataFrame:
    """Загружает и обрабатывает большие JSON файлы с потоковой обработкой"""
    try:
        logger.info(f"Начало загрузки большого JSON файла из {url}")
        
        # Проверка размера файла
        with requests.head(url, timeout=10) as r:
            size_mb = int(r.headers.get('content-length', 0)) / (1024 * 1024)
            if size_mb > MAX_JSON_SIZE_MB:
                st.warning(f"⚠️ Файл очень большой ({size_mb:.1f} МБ). Загрузка может занять несколько минут...")

        # Прогресс-бар
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Создаем временный файл для потоковой обработки
        with tempfile.NamedTemporaryFile(mode='w+b') as tmp_file:
            # Загрузка файла с прогрессом
            with requests.get(url, stream=True, timeout=(30, JSON_LOAD_TIMEOUT)) as response:
                response.raise_for_status()
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    tmp_file.write(chunk)
                    downloaded += len(chunk)
                    progress = min(downloaded / total_size, 1.0) if total_size > 0 else 0
                    progress_bar.progress(progress)
                    status_text.text(f"Загружено: {downloaded/(1024*1024):.1f} МБ / {total_size/(1024*1024):.1f} МБ")

            # Потоковый парсинг JSON с помощью ijson
            tmp_file.seek(0)
            items = []
            status_text.text("Обработка JSON...")
            
            # Адаптируйте 'item' под структуру вашего JSON
            for item in ijson.items(tmp_file, 'item'):
                items.append({
                    'date': item.get('date'),
                    'warehouseType': item.get('warehouseType'),
                    'regionName': item.get('regionName'),
                    'category': item.get('category'),
                    'brand': item.get('brand', '').lower(),
                    'subject': item.get('subject'),
                    'totalPrice': float(item.get('totalPrice', 0)),
                    'spp': float(item.get('spp', 0)),
                    'supplierArticle': str(item.get('supplierArticle', '')),
                    'is_return': str(item.get('srid', '')).startswith('R'),
                    'isCancel': bool(item.get('isCancel', False)),
                    'srid': item.get('srid')
                })
                
                # Лимит для демонстрации (можно убрать в продакшене)
                if len(items) % 100000 == 0:
                    status_text.text(f"Обработано записей: {len(items)}")
            
            # Создаем DataFrame
            df = pd.DataFrame(items)
            
            # Оптимизация типов данных
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date']).dt.tz_localize('Europe/Moscow')
            
            # Русские названия столбцов
            column_mapping = {
                'date': 'Дата',
                'warehouseType': 'Склад',
                'regionName': 'Регион',
                'category': 'Категория',
                'brand': 'Бренд',
                'subject': 'Подкатегория',
                'totalPrice': 'Цена',
                'spp': 'СПП',
                'supplierArticle': 'Артикул'
            }
            df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
            
            # Оптимизация артикулов
            if 'Артикул' in df.columns:
                df['Артикул'] = df['Артикул'].apply(
                    lambda x: x[:len(x)//2] if len(x) == 20 and x[:10] == x[10:] else x
                )
            
            logger.info(f"Успешно загружено {len(df)} записей")
            return df
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка запроса: {str(e)}", exc_info=True)
        st.error(f"Ошибка при загрузке данных: {str(e)}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {str(e)}", exc_info=True)
        st.error(f"Произошла ошибка при обработке данных: {str(e)}")
        return pd.DataFrame()
    finally:
        progress_bar.empty()
        status_text.empty()

# Оптимизированная загрузка Excel
@st.cache_data(ttl=3600, max_entries=2)
def load_excel_data(url: str) -> pd.DataFrame:
    """Загружает и обрабатывает данные из Excel с оптимизацией памяти."""
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
        logger.error(f"Ошибка загрузки Excel: {str(e)}", exc_info=True)
        st.error(f"Ошибка при обработке Excel файла: {str(e)}")
        return pd.DataFrame()

# Оптимизированный экспорт в Excel
def to_excel(df: pd.DataFrame) -> bytes:
    """Конвертирует DataFrame в Excel с оптимизацией памяти."""
    try:
        df_copy = df.copy()
        
        # Очистка datetime объектов
        datetime_cols = ['Дата', 'lastChangeDate']
        for col in datetime_cols:
            if col in df_copy.columns:
                df_copy[col] = df_copy[col].dt.tz_localize(None)
        
        # Используем буфер
        output = io.BytesIO()
        with pd.ExcelWriter(
            output,
            engine='openpyxl',
            mode='w',
            engine_kwargs={'options': {'strings_to_urls': False}}
        ) as writer:
            df_copy.to_excel(
                writer,
                index=False,
                sheet_name='SalesData',
                freeze_panes=(1, 0)
            )
        
        return output.getvalue()
    except Exception as e:
        logger.error(f"Ошибка экспорта в Excel: {str(e)}", exc_info=True)
        st.error(f"Ошибка при создании Excel файла: {str(e)}")
        raise

# Функция для фильтрации данных
def apply_filters(df: pd.DataFrame, date_range: Tuple[datetime.date, datetime.date], 
                 include_cancelled: bool, warehouse_type: list) -> pd.DataFrame:
    """Применяет фильтры к данным с оптимизацией."""
    try:
        filtered = df[
            (df['Дата'].dt.date >= date_range[0]) &
            (df['Дата'].dt.date <= date_range[1]) &
            (~df['is_return'])
        ].copy()
        
        if not include_cancelled:
            filtered = filtered[~filtered['isCancel']]
            
        if warehouse_type:
            filtered = filtered[filtered['Склад'].isin(warehouse_type)]
        
        # Оптимизация памяти
        for col in filtered.select_dtypes(include=['object']):
            filtered[col] = filtered[col].astype('string')
            
        return filtered
    except Exception as e:
        logger.error(f"Ошибка фильтрации: {str(e)}", exc_info=True)
        st.error(f"Ошибка при фильтрации данных: {str(e)}")
        raise

# Основная функция
def main():
    global global_df, global_excel_df
    
    st.title("🔍 Wildberries Analytics Pro (Optimized for Large Files)")
    
    # URL данных
    json_url = "https://storage.yandexcloud.net/my-json-bucket-chat-wb/wb_dashboard/all_sales_data.json"
    excel_url = "https://storage.yandexcloud.net/my-json-bucket-chat-wb/14_04_2025_07_26_%D0%9E%D0%B1%D1%89%D0%B8%D0%B5_%D1%85%D0%B0%D1%80%D0%B0%D0%BA%D1%82%D0%B5%D1%80%D0%B8%D1%81%D1%82%D0%B8%D0%BA%D0%B8_%D0%BE%D0%B4%D0%BD%D0%B8%D0%BC_%D1%84%D0%B0%D0%B9%D0%BB%D0%BE%D0%BC.xlsx"
    
    # Загрузка данных с обработкой больших файлов
    if 'data_loaded' not in st.session_state:
        with st.spinner("Загрузка и обработка данных (это может занять несколько минут для больших файлов)..."):
            try:
                # Загрузка JSON с прогресс-баром
                global_df = load_large_json(json_url)
                
                # Загрузка Excel
                if global_df is not None and not global_df.empty:
                    global_excel_df = load_excel_data(excel_url)
                    
                    # Объединение данных
                    if global_excel_df is not None and not global_excel_df.empty:
                        global_df = pd.merge(
                            global_df,
                            global_excel_df,
                            on='Артикул',
                            how='left',
                            validate='many_to_one'
                        )
                        st.session_state.data_loaded = True
            except Exception as e:
                st.error(f"Ошибка при загрузке данных: {str(e)}")
                return
    
    # Проверка загрузки данных
    if global_df is None or global_df.empty:
        st.warning("Не удалось загрузить данные. Пожалуйста, попробуйте позже.")
        return
    
    # Кнопка сброса кэша
    if st.button("🔄 Сбросить кэш и перезагрузить данные"):
        st.cache_data.clear()
        st.session_state.clear()
        global_df = None
        global_excel_df = None
        st.experimental_rerun()
    
    # Определение диапазона дат
    min_date = global_df['Дата'].min().date()
    max_date = global_df['Дата'].max().date()
    
    # Сайдбар с фильтрами
    with st.sidebar:
        st.header("⏱ Период анализа")
        try:
            date_range = st.date_input(
                "Выберите даты",
                [min_date, max_date],
                min_value=min_date,
                max_value=max_date,
                format="DD.MM.YYYY"
            )
            if len(date_range) != 2:
                st.error("Пожалуйста, выберите диапазон дат")
                st.stop()
        except Exception as e:
            logger.error(f"Ошибка выбора даты: {str(e)}", exc_info=True)
            st.error(f"Ошибка при выборе даты: {str(e)}")
            st.stop()
        
        include_cancelled = st.checkbox("Учитывать отмены", value=False)
        st.header("🗂 Фильтры")
        warehouse_type = st.multiselect(
            "Тип склада",
            options=global_df['Склад'].unique(),
            default=global_df['Склад'].unique()[0] if len(global_df['Склад'].unique()) > 0 else []
        )
    
    # Применение фильтров
    if 'filtered_df' not in st.session_state or st.button("Применить фильтры"):
        with st.spinner("Применение фильтров..."):
            try:
                st.session_state.filtered_df = apply_filters(
                    global_df,
                    date_range,
                    include_cancelled,
                    warehouse_type
                )
            except Exception as e:
                st.error(f"Ошибка при фильтрации: {str(e)}")
                st.stop()
    
    filtered_df = st.session_state.get('filtered_df', pd.DataFrame())
    
    # Проверка данных
    if filtered_df.empty:
        st.warning("Нет данных по выбранным фильтрам")
        st.stop()
    
    # Диагностика данных
    st.subheader("🔍 Диагностика данных")
    cols = st.columns(3)
    cols[0].metric("Всего записей", len(filtered_df))
    cols[1].metric("Уникальных заказов", filtered_df['srid'].nunique())
    cols[2].metric("Дубликатов srid", filtered_df.duplicated(subset=['srid']).sum())
    
    # Ключевые показатели
    st.header("📊 Ключевые показатели")
    
    with st.spinner("Расчет показателей..."):
        try:
            revenue = filtered_df['Цена'].sum()
            order_count = filtered_df['srid'].nunique()
            avg_check = revenue / order_count if order_count > 0 else 0
            avg_spp = filtered_df['СПП'].mean()
            
            cols = st.columns(4)
            cols[0].metric("Выручка", f"{revenue:,.0f} ₽")
            cols[1].metric("Средний чек", f"{avg_check:,.0f} ₽")
            cols[2].metric("Количество заказов", order_count)
            cols[3].metric("Средний СПП", 
                          f"{avg_spp:.2f}%" if not pd.isna(avg_spp) else "N/A",
                          help="Средний процент скидки по продажам")
        except Exception as e:
            logger.error(f"Ошибка расчета метрик: {str(e)}", exc_info=True)
            st.error("Ошибка при расчете показателей")
    
    # Вкладки анализа
    tab1, tab2 = st.tabs(["📈 Динамика продаж", "💰 Детализация выручки"])
    
    with tab1:
        st.subheader("Динамика продаж")
        try:
            freq = st.radio("Группировка", ["День", "Неделя", "Месяц"], 
                          horizontal=True, key="freq_selector")
            freq_map = {"День": "D", "Неделя": "W", "Месяц": "ME"}
            
            with st.spinner("Построение графика..."):
                dynamic_df = filtered_df.groupby(
                    pd.Grouper(key='Дата', freq=freq_map[freq])
                ).agg({
                    'Цена': 'sum',
                    'srid': 'nunique'
                }).reset_index()
                
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=dynamic_df['Дата'],
                    y=dynamic_df['Цена'],
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
                
        except Exception as e:
            logger.error(f"Ошибка построения графика: {str(e)}", exc_info=True)
            st.error("Ошибка при отображении динамики продаж")
    
    with tab2:
        st.subheader("Детализация выручки")
        
        if st.checkbox("Показать детализацию", True, key="show_details"):
            with st.spinner("Анализ выручки..."):
                try:
                    total_revenue = filtered_df['Цена'].sum()
                    
                    # Функция для отображения данных
                    def display_revenue_analysis(df, group_col, title):
                        analysis_df = df.groupby(group_col).agg({
                            'Цена': ['sum', 'count'],
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
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                        return analysis_df
                    
                    # Анализ по категориям
                    cat_df = display_revenue_analysis(filtered_df, 'Категория', "Выручка по категориям")
                    selected_cat = st.selectbox("Выберите категорию", cat_df['Категория'].unique())
                    
                    # Детализация по выбранной категории
                    cat_details = filtered_df[filtered_df['Категория'] == selected_cat]
                    subcat_df = display_revenue_analysis(cat_details, 'Подкатегория', 
                                                       f"Выручка по подкатегориям ({selected_cat})")
                    
                    # Анализ по брендам
                    brand_df = display_revenue_analysis(filtered_df, 'Бренд', "Выручка по брендам")
                    
                    # Почасовая аналитика для одного дня
                    if date_range[0] == date_range[1]:
                        st.subheader("Почасовая аналитика")
                        hourly_df = filtered_df.groupby(filtered_df['Дата'].dt.hour).agg({
                            'Цена': 'sum',
                            'srid': 'nunique'
                        }).reset_index().rename(columns={'Дата': 'Час'})
                        
                        fig = px.bar(
                            hourly_df,
                            x='Час',
                            y='Цена',
                            hover_data=['srid'],
                            labels={'srid': 'Количество заказов'},
                            title='Выручка по часам'
                        )
                        st.plotly_chart(fig, use_container_width=True)
                        
                except Exception as e:
                    logger.error(f"Ошибка детализации: {str(e)}", exc_info=True)
                    st.error("Ошибка при анализе выручки")
    
    # Экспорт данных
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
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        cols[1].download_button(
            label="📥 CSV (сжатый)",
            data=filtered_df.to_csv(index=False, encoding='utf-8').encode('utf-8'),
            file_name="wb_analytics.csv",
            mime="text/csv"
        )

if __name__ == "__main__":
    try:
        main()
    finally:
        # Гарантированная очистка памяти
        gc.collect()
