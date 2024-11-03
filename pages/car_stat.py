# car_stat.py

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import func
from database.db import SessionLocal
from database.models import Cars, Profits
import plotly.express as px

# Настройка страницы
st.set_page_config(layout="wide", initial_sidebar_state="collapsed")

st.title("Статистика по конкретной машине")

# Создаем 5 колонок для ввода Stock N и кнопки поиска
col1, col2, col3, col4, col5 = st.columns(5)

with col4:
    stockn_input = st.text_input("Введите StockN", label_visibility='collapsed', max_chars=5, placeholder='Введите StockN')
with col5:
    search_button = st.button("Поиск")

# Функция для создания индикатора (Gauge)
def create_gauge(value, min_val, max_val, title, color):
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value if value else 0,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': title},
        gauge={
            'axis': {'range': [min_val, max_val]},
            'bar': {'color': color},
        }
    ))
    fig.update_layout(height=250, margin={'t': 50, 'b': 0, 'l': 0, 'r': 0})
    return fig

if search_button and stockn_input:
    try:
        stockn = int(stockn_input)
    except ValueError:
        st.error("Пожалуйста, введите корректный номер Stock N.")
    else:
        session = SessionLocal()
        try:
            # Получаем данные о машине
            car = session.query(Cars).filter(Cars.stockn == stockn).first()
            if car:
                # Получаем данные по всем активным машинам
                active_cars_query = session.query(Cars).filter(Cars.status == 'active')
                active_cars_df = pd.read_sql(active_cars_query.statement, session.bind)

                # Формируем заголовок
                make_model_year = f"{car.make} {car.model} {car.year}"
                color = f"({car.color})" if car.color else ""
                title_text = f"**Stock N {car.stockn} | {make_model_year} {color}**"

                # Проверяем, окупилась ли машина
                if car.payback and car.payback > 0 and car.breakevendate:
                    payback_status = f":green[Окупилась] (Дата окупаемости: {car.breakevendate.strftime('%d.%m.%Y')})"
                else:
                    payback_status = ":red[Не окупилась]"

                # Отображаем заголовок
                st.markdown(f"{title_text} {payback_status}")

                # Шаг 6: Индикаторы (Gauges)
                metrics = ['cost', 'profit', 'xs', 'age', 'payback']
                metric_ranges = {}

                for metric in metrics:
                    values = active_cars_df[metric].dropna()
                    if not values.empty:
                        min_val = values.min()
                        max_val = values.max()
                    else:
                        min_val = 0
                        max_val = 1  # Чтобы избежать деления на ноль
                    metric_ranges[metric] = (min_val, max_val)

                # Создаем 5 колонок для индикаторов
                gauge_cols = st.columns(5)

                with gauge_cols[0]:
                    fig = create_gauge(car.cost, *metric_ranges['cost'], "Cost", "blue")
                    st.plotly_chart(fig, use_container_width=True, key=f"gauge_cost_{car.stockn}")

                with gauge_cols[1]:
                    fig = create_gauge(car.profit, *metric_ranges['profit'], "Profit", "green")
                    st.plotly_chart(fig, use_container_width=True, key=f"gauge_profit_{car.stockn}")

                with gauge_cols[2]:
                    fig = create_gauge(car.xs, *metric_ranges['xs'], "Xs", "orange")
                    st.plotly_chart(fig, use_container_width=True, key=f"gauge_xs_{car.stockn}")

                with gauge_cols[3]:
                    fig = create_gauge(car.age, *metric_ranges['age'], "Age", "purple")
                    st.plotly_chart(fig, use_container_width=True, key=f"gauge_age_{car.stockn}")

                with gauge_cols[4]:
                    payback_value = car.payback if car.payback else 0
                    fig = create_gauge(payback_value, *metric_ranges['payback'], "Payback", "red")
                    st.plotly_chart(fig, use_container_width=True, key=f"gauge_payback_{car.stockn}")

                # Шаг 7: График изменения прибыли
                profits_data = (
                    session.query(Profits.date, Profits.change_amount)
                    .filter(Profits.stockn == car.stockn)
                    .order_by(Profits.date)
                    .all()
                )

                profits_df = pd.DataFrame(profits_data, columns=['date', 'change_amount'])

                # Преобразуем 'date' в формат datetime
                profits_df['date'] = pd.to_datetime(profits_df['date'], errors='coerce')
                # Убираем строки с некорректными датами
                profits_df = profits_df.dropna(subset=['date'])

                if profits_df.empty:
                    st.warning("Нет корректных данных для отображения графика изменения прибыли.")
                else:
                    # Создаем столбец с отформатированной датой (дата без агрегации)
                    profits_df['date_str'] = profits_df['date'].dt.strftime('%Y-%m-%d')

                    # Шаг 8: Добавление средних значений за каждую дату
                    # 1. Среднее по всем машинам за каждую дату
                    avg_change_all_df = pd.read_sql(
                        session.query(
                            Profits.date,
                            func.avg(Profits.change_amount).label('avg_change_all')
                        )
                        .group_by(Profits.date)
                        .statement,
                        session.bind
                    )
                    # Преобразуем 'date' в формат datetime
                    avg_change_all_df['date'] = pd.to_datetime(avg_change_all_df['date'], errors='coerce')
                    # Убираем строки с некорректными датами
                    avg_change_all_df = avg_change_all_df.dropna(subset=['date'])
                    avg_change_all_df['date_str'] = avg_change_all_df['date'].dt.strftime('%Y-%m-%d')

                    # 2. Среднее по марке (make) за каждую дату
                    avg_change_make_df = pd.read_sql(
                        session.query(
                            Profits.date,
                            func.avg(Profits.change_amount).label('avg_change_make')
                        )
                        .join(Cars, Profits.stockn == Cars.stockn)
                        .filter(Cars.make == car.make)
                        .group_by(Profits.date)
                        .statement,
                        session.bind
                    )
                    avg_change_make_df['date'] = pd.to_datetime(avg_change_make_df['date'], errors='coerce')
                    avg_change_make_df = avg_change_make_df.dropna(subset=['date'])
                    avg_change_make_df['date_str'] = avg_change_make_df['date'].dt.strftime('%Y-%m-%d')

                    # 3. Среднее по модели (model) за каждую дату
                    avg_change_model_df = pd.read_sql(
                        session.query(
                            Profits.date,
                            func.avg(Profits.change_amount).label('avg_change_model')
                        )
                        .join(Cars, Profits.stockn == Cars.stockn)
                        .filter(Cars.model == car.model)
                        .group_by(Profits.date)
                        .statement,
                        session.bind
                    )
                    avg_change_model_df['date'] = pd.to_datetime(avg_change_model_df['date'], errors='coerce')
                    avg_change_model_df = avg_change_model_df.dropna(subset=['date'])
                    avg_change_model_df['date_str'] = avg_change_model_df['date'].dt.strftime('%Y-%m-%d')

                    # Объединяем все средние значения с основным DataFrame по 'date_str'
                    combined_df = profits_df.merge(avg_change_all_df[['date_str', 'avg_change_all']],
                                                   on='date_str',
                                                   how='left')
                    combined_df = combined_df.merge(avg_change_make_df[['date_str', 'avg_change_make']],
                                                    on='date_str',
                                                    how='left')
                    combined_df = combined_df.merge(avg_change_model_df[['date_str', 'avg_change_model']],
                                                    on='date_str',
                                                    how='left')

                    # Сортируем по дате в порядке возрастания
                    combined_df = combined_df.sort_values('date')

                    # Шаг 9: Построение графика изменения прибыли с средними значениями
                    fig = go.Figure()

                    # Добавляем линию изменения прибыли выбранной машины
                    fig.add_trace(go.Scatter(
                        x=combined_df['date'],
                        y=combined_df['change_amount'],
                        mode='lines+markers',
                        name='Изменение прибыли',
                        line=dict(color='blue'),
                        marker=dict(size=6)
                    ))

                    # Добавляем линию среднего по всем машинам
                    fig.add_trace(go.Scatter(
                        x=combined_df['date'],
                        y=combined_df['avg_change_all'],
                        mode='lines',
                        name='Среднее по всем машинам',
                        line=dict(color='green', dash='dash')
                    ))

                    # Добавляем линию среднего по марке
                    fig.add_trace(go.Scatter(
                        x=combined_df['date'],
                        y=combined_df['avg_change_make'],
                        mode='lines',
                        name=f'Среднее по марке {car.make}',
                        line=dict(color='orange', dash='dash')
                    ))

                    # Добавляем линию среднего по модели
                    fig.add_trace(go.Scatter(
                        x=combined_df['date'],
                        y=combined_df['avg_change_model'],
                        mode='lines',
                        name=f'Среднее по модели {car.model}',
                        line=dict(color='red', dash='dash')
                    ))

                    # Настройка макета графика с развёрнутой осью X
                    fig.update_layout(
                        title='Изменение прибыли по датам',
                        xaxis_title='Дата',
                        yaxis_title='Изменение прибыли',
                        legend=dict(
                            x=1,  # Легенда будет начинаться справа
                            y=-0.4,  # Размещение под графиком
                            xanchor="right",
                            yanchor="bottom",
                            orientation="h"  # Горизонтальная ориентация легенды
                        ),
                        hovermode='x unified',
                        margin=dict(t=50, b=100),  # Увеличиваем нижний отступ для легенды
                        xaxis=dict(autorange='reversed')  # Разворот оси X
                    )

                    # Поворачиваем метки оси X для лучшей читаемости
                    fig.update_xaxes(tickangle=-45)

                    # Отображаем график с уникальным ключом
                    st.plotly_chart(fig, use_container_width=True, key=f"profit_graph_{car.stockn}")

                    # Шаг 10: Вкладки с графиками
                    tabs = st.tabs(["Средние Xs", "Средние дни окупаемости"])

                    with tabs[0]:
                        st.subheader("Средние Xs")
                        # Данные для графика Xs
                        xs_car = car.xs if car.xs else 0
                        avg_xs_all = active_cars_df['xs'].dropna().mean()
                        avg_xs_make = active_cars_df[active_cars_df['make'] == car.make]['xs'].dropna().mean()
                        avg_xs_model = active_cars_df[active_cars_df['model'] == car.model]['xs'].dropna().mean()

                        xs_data = {
                            'Категория': ['Данная машина', 'Среднее по всем', f'Среднее по {car.make}', f'Среднее по {car.model}'],
                            'Xs': [xs_car, avg_xs_all, avg_xs_make, avg_xs_model]
                        }
                        xs_df = pd.DataFrame(xs_data)

                        fig = px.bar(
                            xs_df,
                            x='Xs',
                            y='Категория',
                            orientation='h',
                            labels={'Xs': 'Xs', 'Категория': ''},
                            title='Сравнение Xs'
                        )

                        # Настройка цветов баров
                        colors = ['blue', 'green', 'orange', 'red']
                        fig.update_traces(marker_color=colors)

                        # Отображаем график с уникальным ключом
                        st.plotly_chart(fig, use_container_width=True, key=f"xs_graph_{car.stockn}")

                    with tabs[1]:
                        st.subheader("Средние дни окупаемости")
                        # Данные для графика Payback
                        payback_car = car.payback if car.payback else 0
                        avg_payback_all = active_cars_df['payback'].dropna().mean()
                        avg_payback_make = active_cars_df[active_cars_df['make'] == car.make]['payback'].dropna().mean()
                        avg_payback_model = active_cars_df[active_cars_df['model'] == car.model]['payback'].dropna().mean()

                        payback_data = {
                            'Категория': ['Данная машина', 'Среднее по всем', f'Среднее по {car.make}', f'Среднее по {car.model}'],
                            'Payback': [payback_car, avg_payback_all, avg_payback_make, avg_payback_model]
                        }
                        payback_df = pd.DataFrame(payback_data)

                        fig = px.bar(
                            payback_df,
                            x='Payback',
                            y='Категория',
                            orientation='h',
                            labels={'Payback': 'Дни окупаемости', 'Категория': ''},
                            title='Сравнение дней окупаемости'
                        )

                        # Настройка цветов баров
                        colors = ['blue', 'green', 'orange', 'red']
                        fig.update_traces(marker_color=colors)

                        # Отображаем график с уникальным ключом
                        st.plotly_chart(fig, use_container_width=True, key=f"payback_graph_{car.stockn}")
            else:
                st.error(f"Машина с Stock N {stockn} не найдена.")
        except Exception as e:
            st.error(f"Ошибка при получении данных: {e}")
        finally:
            session.close()
