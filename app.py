# app.py
import streamlit as st
import pandas as pd
import plotly.express as px

# Импортируем функции из ваших модулей
from services.calculate import (
    get_cars_without_significant_sales,
    get_unprofitable_old_cars,
    get_best_purchases,
    calculate_summary_statistics,
    get_monthly_income,
    get_monthly_car_counts,
    get_profit_dynamics_bulk
)
from services.table_service import create_aggrid_table

# Импортируем сессию и модели
from database.db import SessionLocal
from database.models import Cars, Profits

st.set_page_config(layout="wide", initial_sidebar_state="collapsed")

@st.cache_data
def load_data():
    session = SessionLocal()

    # Загружаем данные из таблицы Cars
    cars_query = session.query(Cars).all()
    cars_df = pd.DataFrame([{
        'id': car.id,
        'stockn': car.stockn,
        'make': car.make,
        'model': car.model,
        'year': car.year,
        'color': car.color,
        'milage': car.milage,
        'engine': car.engine,
        'location': car.location,
        'cost': car.cost,
        'inventoried': car.inventoried,
        'breakevendate': car.breakevendate,
        'dismantled': car.dismantled,
        'purchesdate': car.purchesdate,
        'age': car.age,
        'payback': car.payback,
        'profit': car.profit,
        'xs': car.xs,
        'status': car.status,
        'import_id': car.import_id,
        'age_last_updated': car.age_last_updated
    } for car in cars_query])

    # Загружаем данные из таблицы Profits
    profits_query = session.query(Profits).all()
    profits_df = pd.DataFrame([{
        'id': profit.id,
        'stockn': profit.stockn,
        'date': profit.date,
        'cumulative_amount': profit.cumulative_amount,
        'change_amount': profit.change_amount,
        'import_id': profit.import_id
    } for profit in profits_query])

    session.close()

    return cars_df, profits_df

cars_df, profits_df = load_data()

# Обработка NaN значений в cars_df
cars_df.fillna({
    'stockn': 0,
    'make': '',
    'model': '',
    'year': 0,
    'color': '',
    'milage': 0.0,
    'engine': '',
    'location': '',
    'cost': 0.0,
    'inventoried': pd.NaT,
    'breakevendate': pd.NaT,
    'dismantled': pd.NaT,
    'purchesdate': pd.NaT,
    'age': 0,
    'payback': 0,
    'profit': 0.0,
    'xs': 0.0,
    'status': '',
    'import_id': '',
    'age_last_updated': pd.NaT
}, inplace=True)

# Преобразуем столбцы дат в datetime
date_columns = ['inventoried', 'breakevendate', 'dismantled', 'purchesdate', 'age_last_updated']
for col in date_columns:
    cars_df[col] = pd.to_datetime(cars_df[col], errors='coerce')

# Обработка NaN значений в profits_df
profits_df.fillna({
    'stockn': 0,
    'date': pd.NaT,
    'cumulative_amount': 0.0,
    'change_amount': 0.0,
    'import_id': ''
}, inplace=True)

profits_df['date'] = pd.to_datetime(profits_df['date'], errors='coerce')

columns_to_display = [
    'stockn', 'make', 'model', 'year', 'color',
    'cost', 'profit', 'xs', 'dinamic', 'age'
]

exclude_stocks = []

# Получаем данные для таблицы 2
table2_df = get_unprofitable_old_cars(cars_df, exclude_stocks)

# Получаем список Stockn из Таблицы №2 для исключения из Таблицы №1
exclude_stocks = table2_df['stockn'].tolist() if not table2_df.empty else []

tab1, tab2, tab3 = st.tabs([
    "Без значимых продаж за последний месяц",
    "Неокупившиеся машины старше 60 дней",
    "Лучшие покупки"
])

with tab1:
    st.header("Без значимых продаж за последний месяц")

    # Получаем данные для таблицы
    table1_df = get_cars_without_significant_sales(profits_df, cars_df, exclude_stocks=exclude_stocks)

    if table1_df.empty:
        st.write("Нет машин, удовлетворяющих условиям.")
    else:
        # Выбираем необходимые колонки
        with SessionLocal() as session:
            stockn_list = table1_df['stockn'].tolist()
            dynamics_dict = get_profit_dynamics_bulk(session, stockn_list)

        table1_df['dinamic'] = table1_df['stockn'].map(dynamics_dict).fillna('Нет данных')
        table1_df = table1_df[columns_to_display]

        # Отображаем таблицу
        create_aggrid_table(table1_df)

        # Сводная информация
        summary_stats = calculate_summary_statistics(table1_df)

        # Создаем 4 колонки
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Количество машин", summary_stats['Количество машин'])
        with col2:
            st.metric("Общий расход", summary_stats['Общий расход'])
        with col3:
            st.metric("Общий доход", summary_stats['Общий доход'])
        with col4:
            st.metric("Общая прибыль", summary_stats['Общая прибыль'])

with tab2:
    st.header("Неокупившиеся машины старше 60 дней")
    if table2_df.empty:
        st.write("Нет машин, удовлетворяющих условиям.")
    else:
        with SessionLocal() as session:
            stockn_list = table2_df['stockn'].tolist()
            dynamics_dict = get_profit_dynamics_bulk(session, stockn_list)

        table2_df['dinamic'] = table2_df['stockn'].map(dynamics_dict).fillna('Нет данных')
        table2_df = table2_df[columns_to_display]

        # Отображаем таблицу
        create_aggrid_table(table2_df, fit_columns_on_grid_load=True)

        # Сводная информация
        summary_stats = calculate_summary_statistics(table2_df)

        # Создаем 4 колонки
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Количество машин", summary_stats['Количество машин'])
        with col2:
            st.metric("Общий расход", summary_stats['Общий расход'])
        with col3:
            st.metric("Общий доход", summary_stats['Общий доход'])
        with col4:
            st.metric("Общая прибыль", summary_stats['Общая прибыль'])

with tab3:
    st.header("Лучшие покупки")

    # Получаем данные для таблицы
    table3_df = get_best_purchases(cars_df)

    if table3_df.empty:
        st.write("Нет машин, удовлетворяющих условиям.")
    else:
        with SessionLocal() as session:
            stockn_list = table3_df['stockn'].tolist()
            dynamics_dict = get_profit_dynamics_bulk(session, stockn_list)

        table3_df['dinamic'] = table3_df['stockn'].map(dynamics_dict).fillna('Нет данных')
        table3_df = table3_df[columns_to_display]

        # Отображаем таблицу
        create_aggrid_table(table3_df, fit_columns_on_grid_load=True)

        # Сводная информация
        summary_stats = calculate_summary_statistics(table3_df)

        # Создаем 4 колонки
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Количество машин", summary_stats['Количество машин'])
        with col2:
            st.metric("Общий расход", summary_stats['Общий расход'])
        with col3:
            st.metric("Общий доход", summary_stats['Общий доход'])
        with col4:
            st.metric("Общая прибыль", summary_stats['Общая прибыль'])

st.header("Доходы по месяцам")

monthly_income = get_monthly_income(profits_df)

if monthly_income.empty:
    st.write("Нет данных для отображения графика доходов по месяцам.")
else:
    fig = px.bar(
        monthly_income,
        x='month_str',
        y='change_amount',
        labels={'month_str': 'Month', 'change_amount': 'Profit'},
        title='Доходы по месяцам'
    )

    # Устанавливаем порядок категорий на оси X
    fig.update_layout(xaxis={'categoryorder':'array', 'categoryarray':monthly_income['month_str']})

    st.plotly_chart(fig)

st.header("Покупки машин по месяцам")

monthly_counts = get_monthly_car_counts(cars_df)

if monthly_counts.empty:
    st.write("Нет данных для отображения графика покупок машин по месяцам.")
else:
    fig = px.line(
        monthly_counts,
        x='month_str',
        y=['Количество покупок', 'Количество инвентаризаций'],
        labels={'variable': 'Показатель', 'value': 'Количество машин', 'month_str': 'Дата (мм/гг)'},
        title='Покупки и инвентаризация машин по месяцам'
    )

    st.plotly_chart(fig)
