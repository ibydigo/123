import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode
from sqlalchemy.orm import Session
from database.db import SessionLocal
from services.table_service import fetch_cars_data
from services.calculate import (
    calculate_stock_count, calculate_total_cost, calculate_total_profit,
    calculate_average_xs, calculate_average_until_payback, get_profit_dynamics_bulk
)
import pandas as pd


def render_filters(df: pd.DataFrame):
    """Отображение фильтров над таблицей."""

    # Получаем уникальные значения для фильтров
    unique_makes = [""] + sorted(df['make'].dropna().unique())
    unique_models = [""]
    unique_colors = [""] + sorted(df['color'].dropna().unique())
    unique_years = [""] + sorted(df['year'].dropna().unique())
    unique_statuses = ["active", "inactive", "scrap"]

    # Строка 1: Make, Model, Year, Color, Status
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        selected_make = st.selectbox("Make", options=unique_makes, index=0)

    # Обновляем список моделей на основе выбранного make
    if selected_make:
        unique_models = [""] + sorted(df[df['make'] == selected_make]['model'].dropna().unique())

    with col2:
        selected_model = st.selectbox("Model", options=unique_models, index=0)

    with col3:
        selected_year = st.selectbox("Year", options=unique_years, index=0)

    with col4:
        selected_color = st.selectbox("Color", options=unique_colors, index=0)

    with col5:
        selected_statuses = st.multiselect("Status", unique_statuses, default=["active"])

    # Строка 2: Possession, Cost, XS
    col1, col2, col3 = st.columns(3)

    with col1:
        possession_min, possession_max = int(df['age'].min()), int(df['age'].max())
        possession_range = st.slider("Possession (days)", min_value=possession_min, max_value=possession_max,
                                     value=(possession_min, possession_max))

    with col2:
        cost_min, cost_max = int(df['cost'].min()), int(df['cost'].max())
        cost_range = st.slider("Cost Range", min_value=cost_min, max_value=cost_max, value=(cost_min, cost_max))

    with col3:
        xs_min, xs_max = float(df['xs'].min()), float(df['xs'].max())
        xs_range = st.slider("XS", min_value=xs_min, max_value=xs_max, value=(xs_min, xs_max))

    st.markdown("---")

    return {
        'make': selected_make,
        'model': selected_model,
        'year': selected_year,
        'color': selected_color,
        'statuses': selected_statuses,
        'possession_range': possession_range,
        'cost_range': cost_range,
        'xs_range': xs_range
    }


def render_stock_table():
    """Отображение таблицы Stock № с настроенными колонками и скрытыми полями."""
    # Создаем сессию базы данных
    session = SessionLocal()

    # Получаем данные из таблицы Cars
    df = fetch_cars_data(session)

    # Сортировка по 'stockn' в порядке убывания
    df = df.sort_values(by='stockn', ascending=False)

    # Вызов фильтров перед таблицей
    filters = render_filters(df)

    # Применение фильтров
    if filters['statuses']:
        df = df[df['status'].isin(filters['statuses'])]
    if filters['make']:
        df = df[df['make'] == filters['make']]
    if filters['model']:
        df = df[df['model'] == filters['model']]
    if filters['year']:
        df = df[df['year'] == filters['year']]
    if filters['color']:
        df = df[df['color'] == filters['color']]
    df = df[(df['age'] >= filters['possession_range'][0]) & (df['age'] <= filters['possession_range'][1])]
    df = df[(df['cost'] >= filters['cost_range'][0]) & (df['cost'] <= filters['cost_range'][1])]
    df = df[(df['xs'] >= filters['xs_range'][0]) & (df['xs'] <= filters['xs_range'][1])]

    # Создаем копию DataFrame перед изменениями
    df = df.copy()

    # Добавляем столбец с динамикой прибыли
    stockn_list = df['stockn'].tolist()
    dynamics_dict = get_profit_dynamics_bulk(session, stockn_list)
    df['dinamic'] = df['stockn'].map(dynamics_dict).fillna('Нет данных')

    # Упорядочиваем столбцы в нужном порядке
    column_order = [
        "stockn", "make", "model", "year", "color", "cost", "profit", "xs",
        "dinamic", "payback", "age", "milage", "engine", "location", "purchesdate"
    ]
    df = df[column_order]

    # Настройка параметров AgGrid
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(
        editable=False,
        filterable=True,
        sortable=True,
        resizable=True
    )

    # JavaScript-код для автоматической подстройки ширины колонок
    auto_size_js = JsCode("""
    function(params) {
        let allColumnIds = [];
        params.columnApi.getAllColumns().forEach(function(column) {
            allColumnIds.push(column.colId);
        });
        params.columnApi.autoSizeColumns(allColumnIds, false);
    };
    """)

    gb.configure_grid_options(
        enableRangeSelection=True,
        suppressColumnVirtualisation=True,
        onGridReady=auto_size_js  # Добавляем событие onGridReady
    )

    grid_options = gb.build()

    # Вывод таблицы с AgGrid
    grid_response = AgGrid(
        df,
        gridOptions=grid_options,
        update_mode=GridUpdateMode.MODEL_CHANGED,
        allow_unsafe_jscode=True,
        editable=False,
        fit_columns_on_grid_load=False
    )

    # Отфильтрованные данные
    filtered_df = pd.DataFrame(grid_response['data'])

    # Выводим сводные данные
    render_summary(filtered_df)

    # Закрываем сессию
    session.close()

def render_summary(filtered_df):
    """Отображение сводных данных под таблицей с учетом фильтрации."""
    st.subheader("Сводные данные")

    # Используем отфильтрованные данные для динамических расчетов
    stock_count = calculate_stock_count(filtered_df)
    total_cost = calculate_total_cost(filtered_df)
    total_profit = calculate_total_profit(filtered_df)
    total_revenue = total_cost + total_profit
    avg_xs = calculate_average_xs(filtered_df)
    avg_until_payback = calculate_average_until_payback(filtered_df)

    # Создаем колонки для вывода сводных данных в одном ряду
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        st.metric("Всего машин", stock_count)

    with col2:
        st.metric("Сумма стоимости", f"{total_cost:.0f}")

    with col3:
        st.metric("Сумма дохода", f"{total_revenue:.0f}")

    with col4:
        st.metric("Сумма прибыли", f"{total_profit:.0f}")

    with col5:
        st.metric("Средние Иксы", f"{avg_xs:.2f}")

    with col6:
        st.metric("Среднее время окупаемости", f"{avg_until_payback:.0f}")


def main():
    render_stock_table()


if __name__ == "__main__":
    main()
