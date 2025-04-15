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


# Глобальные переменные для хранения DataFrame
# Это поможет избежать повторной загрузки и обработки данных,
# если они уже были загружены и обработаны ранее.
global_df = None
global_excel_df = None


# Загрузка данных из JSON файла по ссылке
@st.cache_data(ttl=3600, max_entries=5)
def load_data(url):
    """Загружает данные из JSON URL."""
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
            'warehouseType': 'Склад', # Используем warehouseType вместо warehouseName
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
    except requests.exceptions.RequestException as e:
        st.error(f"Ошибка при загрузке данных из URL: {str(e)}")
        return pd.DataFrame()
    except json.JSONDecodeError as e:
        st.error(f"Ошибка при декодировании JSON: {str(e)}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Ошибка при обработке данных: {str(e)}")
        return pd.DataFrame()


# Загрузка данных из Excel файла
@st.cache_data(ttl=3600, max_entries=5)
def load_excel_data(url):
    """Загружает данные из Excel URL."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        excel_file = io.BytesIO(response.content)
        df = pd.read_excel(excel_file)

        # Проверка наличия необходимых столбцов
        if 'Артикул продавца' not in df.columns or 'Наименование' not in df.columns:
            st.error("В Excel файле отсутствуют столбцы 'Артикул продавца' или 'Наименование'.")
            return pd.DataFrame()

        # Переименование столбцов
        df = df.rename(columns={'Артикул продавца': 'Артикул', 'Наименование': 'Наименование товара'})

        # Преобразование артикула в строковый тип
        df['Артикул'] = df['Артикул'].astype(str)

        return df[['Артикул', 'Наименование товара']]
    except requests.exceptions.RequestException as e:
        st.error(f"Ошибка при загрузке данных из URL: {str(e)}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Ошибка при обработке Excel файла: {str(e)}")
        return pd.DataFrame()


# Функция для создания Excel-файла из DataFrame (с удалением временных зон)
def to_excel(df):
    """Преобразует DataFrame в Excel файл."""
    df_copy = df.copy()
    if 'Дата' in df_copy.columns:
        df_copy['Дата'] = df_copy['Дата'].dt.tz_localize(None)
    if 'lastChangeDate' in df_copy.columns:
        df_copy['lastChangeDate'] = df_copy['lastChangeDate'].dt.tz_localize(None)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_copy.to_excel(writer, index=False, sheet_name='SalesData')
    processed_data = output.getvalue()
    return processed_data


# Глобальные переменные для хранения DataFrame
global_df = None
global_excel_df = None

# Основной интерфейс
def main():
    """Основная функция Streamlit приложения."""
    global global_df, global_excel_df  # Объявляем глобальные переменные в начале функции

    st.title("🔍 Wildberries Analytics Pro")
    json_url = "https://storage.yandexcloud.net/my-json-bucket-chat-wb/wb_dashboard/all_sales_data.json"
    excel_url = "https://storage.yandexcloud.net/my-json-bucket-chat-wb/14_04_2025_07_26_%D0%9E%D0%B1%D1%89%D0%B8%D0%B5_%D1%85%D0%B0%D1%80%D0%B0%D0%BA%D1%82%D0%B5%D1%80%D0%B8%D1%81%D1%82%D0%B8%D0%BA%D0%B8_%D0%BE%D0%B4%D0%BD%D0%B8%D0%BC_%D1%84%D0%B0%D0%B9%D0%BB%D0%BE%D0%BC.xlsx"

    # Загрузка данных
    with st.spinner("Загрузка данных..."):
        if global_df is None:
            global_df = load_data(json_url)
        if global_excel_df is None:
            global_excel_df = load_excel_data(excel_url)

    # Проверка на ошибки загрузки
    if global_df is None or global_df.empty:
        st.warning("Не удалось загрузить данные из JSON. Пожалуйста, попробуйте позже.")
        return
    if global_excel_df is None or global_excel_df.empty:
        st.warning("Не удалось загрузить данные из Excel. Пожалуйста, попробуйте позже.")
        return

    # Объединение данных
    df = pd.merge(global_df, global_excel_df, on='Артикул', how='left')

    # Кнопка сброса кэша
    if st.button("Сбросить кэш"):
        st.cache_data.clear()
        global_df = None
        global_excel_df = None
        st.experimental_rerun()

    # Определяем минимальную и максимальную даты в данных
    min_date = df['Дата'].min().date()
    max_date = df['Дата'].max().date()

    # Боковая панель с фильтрами
    with st.sidebar:
        st.header("⏱ Период анализа")
        date_range = st.date_input(
            "Выберите даты",
            [min_date, max_date], # Устанавливаем диапазон по умолчанию на основе данных
            min_value=min_date, # Минимальная дата
            max_value=max_date, # Максимальная дата
            format="DD.MM.YYYY"
        )

        include_cancelled = st.checkbox("Учитывать отмены", value=False)
        st.header("🗂 Фильтры")
        warehouse_type = st.multiselect(
            "Тип склада",
            options=df['Склад'].unique(), # Используем 'Склад'
            default=df['Склад'].unique()[0] if len(df['Склад'].unique()) > 0 else []
        )

    # Фильтруем данные
    filtered_df = df[
        (df['Дата'].dt.date >= date_range[0]) &
        (df['Дата'].dt.date <= date_range[1]) &
        (~df['is_return'])
    ]

    # Обработка отмененных заказов
    if not include_cancelled:
        filtered_df = filtered_df[filtered_df['isCancel'] == False]

    # Фильтрация по типу склада
    if warehouse_type:
        filtered_df = filtered_df[filtered_df['Склад'].isin(warehouse_type)]

    # Вывод количества дубликатов
    duplicates = filtered_df.duplicated(subset=['srid']).sum()
    st.write(f"Количество дубликатов по srid: {duplicates}")

    # Диагностика данных
    st.subheader("🔍 Диагностика данных")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Всего записей", len(filtered_df))
    with col2:
        st.metric("Уникальных srid", filtered_df['srid'].nunique())
    with col3:
        st.metric("Записей с возвратами", filtered_df['is_return'].sum())

    # Ключевые показатели
    st.header("📊 Ключевые показатели")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        revenue = filtered_df['Выручка'].sum()
        st.metric("Выручка", f"{revenue:,.0f} ₽")
    with col2:
        sales_df = filtered_df # Используем уже отфильтрованный DataFrame
        avg_check = revenue / sales_df['srid'].nunique() if sales_df['srid'].nunique() > 0 else 0
        st.metric("Средний чек", f"{avg_check:,.0f} ₽")
    with col3:
        st.metric("Количество заказов", sales_df['srid'].nunique())
    with col4:
        avg_spp = filtered_df['СПП'].mean()
        if not pd.isna(avg_spp): # Проверяем, что avg_spp не NaN
            avg_spp_rounded = np.ceil(avg_spp * 100) / 100
            st.metric("Средний СПП", f"{avg_spp_rounded:.2f}%")
        else:
            st.metric("Средний СПП", "Данные отсутствуют")

    # Вкладки
    tab1, tab4 = st.tabs(["📈 Динамика", "💰 Детализация выручки"])

    # Динамика продаж
    with tab1:
        st.subheader("Динамика продаж")
        freq = st.radio("Группировка", ["День", "Неделя", "Месяц"], horizontal=True)
        freq_map = {"День": "D", "Неделя": "W", "Месяц": "ME"}
        dynamic_df = filtered_df.groupby(pd.Grouper(key='Дата', freq=freq_map[freq])).agg({
            'Выручка': 'sum',
            'is_return': 'mean'
        }).reset_index()

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=dynamic_df['Дата'],
            y=dynamic_df['Выручка'],
            name="Выручка",
            line=dict(color='#1f77b4', width=2)
        ))

        fig.update_layout(
            title=f"Динамика по {freq.lower()}м",
            yaxis_title="Сумма (₽)",
            hovermode="x unified",
            legend=dict(orientation="h", y=1.1)
        )

        st.plotly_chart(fig, use_container_width=True)

    # Детализация выручки
    with tab4:
        st.subheader("Детализация выручки")
        total_revenue = filtered_df['Выручка'].sum()

        # Функция для отображения деталей
        def show_details(df, level, value):
            st.write(f"Детали для {level}: {value}")
            if level == 'Бренд':
                details = df[df['Бренд'] == value].groupby(['Артикул', 'Наименование товара']).agg({
                    'Выручка': 'sum',
                    'Цена': 'count',
                    'СПП': 'mean'
                }).reset_index()
                details = details.rename(columns={
                    'Артикул': 'Артикул',
                    'Наименование товара': 'Наименование товара',
                    'Выручка': 'Общая выручка',
                    'Цена': 'Количество',
                    'СПП': 'Средний СПП'
                })
            elif level == 'Категория':
                details = df[df['Категория'] == value].groupby(['Артикул', 'Наименование товара']).agg({
                    'Выручка': 'sum',
                    'Цена': 'count',
                    'СПП': 'mean'
                }).reset_index()
                details = details.rename(columns={
                    'Артикул': 'Артикул',
                    'Наименование товара': 'Наименование товара',
                    'Выручка': 'Общая выручка',
                    'Цена': 'Количество',
                    'СПП': 'Средний СПП'
                })
            elif level == 'Подкатегория':
                details = df[df['Подкатегория'] == value].groupby(['Артикул', 'Наименование товара']).agg({
                    'Выручка': 'sum',
                    'Цена': 'count',
                    'СПП': 'mean'
                }).reset_index()
                details = details.rename(columns={
                    'Артикул': 'Артикул',
                    'Наименование товара': 'Наименование товара',
                    'Выручка': 'Общая выручка',
                    'Цена': 'Количество',
                    'СПП': 'Средний СПП'
                })
            else:
                st.error("Неизвестный уровень детализации")
                return

            # Округление СПП и форматирование значений
            details['Средний СПП'] = np.ceil(details['Средний СПП'] * 100) / 100
            st.dataframe(details)
            st.download_button(
                label=f"Скачать детали для {level} {value} в Excel",
                data=to_excel(details),
                file_name=f"details_{level}_{value}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        # Функция для вывода процентов и СПП
        def display_revenue_data(df, group_column, title):
            revenue_data = df.groupby(group_column).agg({
                'Выручка': 'sum',
                'СПП': 'mean' # Добавляем расчет среднего СПП
            }).reset_index()
            revenue_data['percent'] = (revenue_data['Выручка'] / total_revenue) * 100
            revenue_data = revenue_data.rename(columns={'СПП': 'Средний СПП'}) # Переименовываем столбец
            st.subheader(title)
            fig = px.bar(revenue_data, x=group_column, y='Выручка',
                         hover_data=['percent', 'Средний СПП'], # Добавляем СПП в hover data
                         labels={'percent': '% от общей выручки', 'Средний СПП': 'Средний СПП'},
                         title=title)
            st.plotly_chart(fig)
            st.dataframe(revenue_data.sort_values('Выручка', ascending=False))
            st.download_button(
                label=f"Скачать {title.lower()} в Excel",
                data=to_excel(revenue_data),
                file_name=f"{title.lower().replace(' ', '_')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            return revenue_data # Возвращаем DataFrame

        # Выручка по категориям
        category_revenue = display_revenue_data(filtered_df, 'Категория', "Выручка по категориям")
        selected_category = st.selectbox("Выберите категорию для просмотра деталей", category_revenue['Категория'].unique())
        show_details(filtered_df, 'Категория', selected_category)

        # Выручка по подкатегориям
        subcategory_revenue = display_revenue_data(filtered_df, 'Подкатегория', "Выручка по подкатегориям")
        selected_subcategory = st.selectbox("Выберите подкатегорию для просмотра деталей", subcategory_revenue['Подкатегория'].unique())
        show_details(filtered_df, 'Подкатегория', selected_subcategory)

        # Выручка по брендам
        brand_revenue = display_revenue_data(filtered_df, 'Бренд', "Выручка по брендам")
        selected_brand = st.selectbox("Выберите бренд для просмотра деталей", brand_revenue['Бренд'].unique())
        show_details(filtered_df, 'Бренд', selected_brand)

        # Если выбран только один день, показываем выручку по часам
        if date_range[0] == date_range[1]:
            hourly_revenue = filtered_df.groupby(filtered_df['Дата'].dt.hour)['Выручка'].sum().reset_index()
            hourly_revenue = hourly_revenue.rename(columns={'Дата': 'Час'})
            st.subheader("Выручка по часам")
            fig = px.bar(hourly_revenue, x='Дата', y='Выручка',
                         labels={'Выручка': 'Выручка, ₽', 'Дата': 'Час'},
                         title='Выручка по часам')
            st.plotly_chart(fig)
            st.dataframe(hourly_revenue.sort_values('Выручка', ascending=False))
            st.download_button(
                label="Скачать выручку по часам в Excel",
                data=to_excel(hourly_revenue),
                file_name="revenue_by_hour.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    # Детализированные данные
    with st.expander("📌 Детализированные данные"):
        st.subheader("Исходные данные с фильтрами")
        filtered_df_display = filtered_df.copy()
        st.dataframe(filtered_df_display.sort_values('Дата', ascending=False), height=300)
        st.download_button(
            label="Экспорт в Excel",
            data=to_excel(filtered_df),
            file_name="wb_data.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key='download-excel'
        )
        st.download_button(
            label="Экспорт в CSV",
            data=filtered_df.to_csv(index=False).encode('utf-8'),
            file_name="wb_data.csv",
            mime="text/csv",
            key='download-csv'
        )


if __name__ == "__main__":
    main()
