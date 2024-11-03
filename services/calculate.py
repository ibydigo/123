from datetime import date
from sqlalchemy.orm import Session
from sqlalchemy import func
from database.models import Cars, Profits
import pandas as pd

# Функции расчета для одной машины
def calculate_age(inventoried_date):
    return (date.today() - inventoried_date).days if inventoried_date else None

def calculate_payback(breakevendate, inventoried_date):
    return (breakevendate - inventoried_date).days if breakevendate and inventoried_date else None

def calculate_profit(session, stockn, cost):
    if cost is None or pd.isna(cost):
        print(f"Cost is missing for stockn: {stockn}")
        return None

    last_cumulative_amount = (
        session.query(Profits.cumulative_amount)
        .filter(Profits.stockn == stockn)
        .order_by(Profits.date.desc())
        .first()
    )

    if last_cumulative_amount is None or pd.isna(last_cumulative_amount[0]):
        print(f"Cumulative amount is missing for stockn: {stockn}")
        return None

    profit = int(last_cumulative_amount[0] - cost)
    print(f"Calculated profit for stockn {stockn}: {profit}")
    return profit

def calculate_xs(session, stockn, cost):
    if cost is None or pd.isna(cost):
        print(f"Cost is missing for stockn: {stockn}")
        return None

    last_cumulative_amount = (
        session.query(Profits.cumulative_amount)
        .filter(Profits.stockn == stockn)
        .order_by(Profits.date.desc())
        .first()
    )

    if last_cumulative_amount is None or pd.isna(last_cumulative_amount[0]):
        print(f"Cumulative amount is missing for stockn: {stockn}")
        return None

    xs = round(last_cumulative_amount[0] / cost, 2)
    print(f"Calculated xs for stockn {stockn}: {xs}")
    return xs

# Агрегационные функции
def get_min_max_avg_sum(session, field, make=None, model=None, status=["active"]):
    query = session.query(
        func.min(getattr(Cars, field)),
        func.max(getattr(Cars, field)),
        func.avg(getattr(Cars, field)),
        func.sum(getattr(Cars, field))
    ).filter(Cars.status.in_(status))

    if make:
        query = query.filter(Cars.make == make)
    if model:
        query = query.filter(Cars.model == model)

    return query.first()

# Пример агрегации для конкретных полей
def get_aggregated_data(session, make=None, model=None, include_scrap=False):
    status_filter = ["active"]
    if include_scrap:
        status_filter.append("scrap")

    results = {
        "age": get_min_max_avg_sum(session, "age", make, model, status_filter),
        "payback": get_min_max_avg_sum(session, "payback", make, model, status_filter),
        "profit": get_min_max_avg_sum(session, "profit", make, model, status_filter),
        "xs": get_min_max_avg_sum(session, "xs", make, model, status_filter),
        "cost_sum": get_min_max_avg_sum(session, "cost", make, model, status_filter)[3],
        "profit_sum": get_min_max_avg_sum(session, "profit", make, model, status_filter)[3],
    }
    return results

# Функция для подсчета количества машин
def calculate_stock_count(filtered_df):
    return len(filtered_df)

# Функция для подсчета общей стоимости
def calculate_total_cost(filtered_df):
    """Вычисление общей стоимости автомобилей, игнорируя NaN значения."""
    return filtered_df['cost'].dropna().sum() or 0

# Функция для подсчета общей прибыли
def calculate_total_profit(filtered_df):
    return filtered_df['profit'].dropna().sum() or 0

# Функция для подсчета среднего значения xs
def calculate_average_xs(filtered_df):
    return filtered_df['xs'].dropna().mean() or 0

# Функция для подсчета среднего значения payback, игнорируя отрицательные значения
def calculate_average_until_payback(filtered_df):
    positive_payback = filtered_df['payback'].dropna()  # Убираем NaN значения
    positive_payback = positive_payback[positive_payback > 0]  # Оставляем только положительные значения
    return positive_payback.mean() if not positive_payback.empty else 0

# Функция для получения динамики прибыли
def get_profit_dynamics_bulk(session, stockn_list):
    profits = (
        session.query(Profits.stockn, Profits.change_amount)
        .filter(Profits.stockn.in_(stockn_list))
        .order_by(Profits.stockn, Profits.date.desc())
        .all()
    )

    # Создаем словарь для хранения динамики
    dynamics_dict = {}
    for stockn in stockn_list:
        dynamics_dict[stockn] = []

    # Группируем данные по stockn
    for stockn, change_amount in profits:
        formatted_change = (
            f"⬆️ (+{int(change_amount)})" if change_amount > 0 else
            f"⬇️ ({int(change_amount)})" if change_amount < 0 else
            "0"
        )
        dynamics_dict[stockn].append(formatted_change)

    # Объединяем динамику в строки
    for stockn in dynamics_dict:
        dynamics_dict[stockn] = " / ".join(dynamics_dict[stockn])

    return dynamics_dict

def calculate_change_amount(session, profit_id, stockn, new_cumulative_amount, new_date):
    previous_profit = (
        session.query(Profits.cumulative_amount)
        .filter(Profits.stockn == stockn, Profits.date < new_date, Profits.id != profit_id)
        .order_by(Profits.date.desc())
        .first()
    )

    if previous_profit:
        return new_cumulative_amount - previous_profit[0]
    else:
        return new_cumulative_amount

# Без значимых продаж
def get_cars_without_significant_sales(profits_df, cars_df, exclude_stocks=None, threshold=200):
    # Убираем NaN из 'date' и 'change_amount'
    profits_df = profits_df.dropna(subset=['date', 'change_amount'])

    # Преобразуем 'change_amount' в числовой формат
    profits_df['change_amount'] = pd.to_numeric(profits_df['change_amount'], errors='coerce').fillna(0.0)

    # Получаем последние 4 уникальные даты
    last_dates = profits_df['date'].drop_duplicates().nlargest(4)

    # Фильтруем продажи за последние 4 даты
    recent_sales = profits_df[profits_df['date'].isin(last_dates)]

    # Суммируем продажи по машинам
    sales_sum = recent_sales.groupby('stockn')['change_amount'].sum().reset_index()

    # Выбираем машины с суммой продаж менее или равной threshold
    low_sales_stocks = sales_sum[sales_sum['change_amount'] <= threshold]['stockn']

    # Исключаем машины из exclude_stocks, если они указаны
    if exclude_stocks is not None:
        low_sales_stocks = low_sales_stocks[~low_sales_stocks.isin(exclude_stocks)]

    # Фильтруем данные по машинам
    result_df = cars_df[cars_df['stockn'].isin(low_sales_stocks)]

    return result_df

def get_unprofitable_old_cars(cars_df, exclude_stocks, days_threshold=60, xs_threshold=1.5):
    # Убираем NaN из 'inventoried'
    cars_df = cars_df.dropna(subset=['inventoried'])

    # Вычисляем возраст машины в днях
    cars_df['age_days'] = (pd.Timestamp('today') - cars_df['inventoried']).dt.days

    # Заменяем NaN в 'xs' на 0
    cars_df['xs'] = cars_df['xs'].fillna(0.0)

    # Фильтруем машины по условиям
    filtered_cars = cars_df[
        (cars_df['age_days'] > days_threshold) &
        (cars_df['xs'] < xs_threshold) &
        (~cars_df['stockn'].isin(exclude_stocks))
    ]

    return filtered_cars

# Лучшие покупки
def get_best_purchases(cars_df, xs_threshold=2, profit_threshold=5000):
    # Заменяем NaN в 'xs' и 'profit' на 0
    cars_df['xs'] = cars_df['xs'].fillna(0.0)
    cars_df['profit'] = cars_df['profit'].fillna(0.0)

    filtered_cars = cars_df[
        (cars_df['xs'] > xs_threshold) |
        (cars_df['profit'] > profit_threshold)
        ]
    return filtered_cars

# Общие сведения на App.py
def calculate_summary_statistics(df):
    total_cars = len(df)
    total_cost = df['cost'].sum()
    total_profit = df['profit'].sum()
    total_income = total_profit + total_cost  # Если доход = прибыль + расход

    # Округляем значения до целых чисел
    total_cost = round(total_cost)
    total_profit = round(total_profit)
    total_income = round(total_income)

    summary = {
        'Количество машин': total_cars,
        'Общий расход': f"${total_cost:,}",
        'Общий доход': f"${total_income:,}",
        'Общая прибыль': f"${total_profit:,}"
    }
    return summary

# Доходы по месяцам
def get_monthly_income(profits_df, start_date='2024-09-01'):
    # Убираем NaN из 'date' и 'change_amount'
    profits_df = profits_df.dropna(subset=['date', 'change_amount'])

    # Фильтруем данные по дате
    profits_df = profits_df[profits_df['date'] >= pd.to_datetime(start_date)]

    # Добавляем столбец с месяцем
    profits_df['month'] = profits_df['date'].dt.to_period('M')

    # Суммируем 'change_amount' по месяцам
    monthly_income = profits_df.groupby('month')['change_amount'].sum().reset_index()

    # Преобразуем месяц в строку для отображения
    monthly_income['month_str'] = monthly_income['month'].dt.strftime('%m/%y')

    # Сортируем данные по месяцам в обратном порядке
    monthly_income = monthly_income.sort_values('month', ascending=False)

    return monthly_income[['month_str', 'change_amount']]

# Покупки по месяцам
def get_monthly_car_counts(cars_df, start_date='2022-05-01'):
    # Преобразуем столбцы дат в datetime, если это еще не сделано
    cars_df['purchesdate'] = pd.to_datetime(cars_df['purchesdate'], errors='coerce')
    cars_df['inventoried'] = pd.to_datetime(cars_df['inventoried'], errors='coerce')

    # Фильтруем даты после start_date
    start_date = pd.to_datetime(start_date)
    purch_mask = cars_df['purchesdate'] >= start_date
    inv_mask = cars_df['inventoried'] >= start_date

    # Считаем количество покупок по месяцам
    purchase_df = cars_df.loc[purch_mask].copy()
    purchase_df['purchase_month'] = purchase_df['purchesdate'].dt.to_period('M')
    purchase_counts = purchase_df.groupby('purchase_month').size().reset_index(name='Количество покупок')

    # Считаем количество инвентаризаций по месяцам
    inventory_df = cars_df.loc[inv_mask].copy()
    inventory_df['inventory_month'] = inventory_df['inventoried'].dt.to_period('M')
    inventory_counts = inventory_df.groupby('inventory_month').size().reset_index(name='Количество инвентаризаций')

    # Объединяем данные по месяцам
    monthly_counts = pd.merge(
        purchase_counts,
        inventory_counts,
        left_on='purchase_month',
        right_on='inventory_month',
        how='outer'
    )

    # Обрабатываем NaN значения
    monthly_counts['month'] = monthly_counts['purchase_month'].combine_first(monthly_counts['inventory_month'])
    monthly_counts = monthly_counts.drop(columns=['purchase_month', 'inventory_month'])

    # Заполняем NaN нулями
    monthly_counts['Количество покупок'] = monthly_counts['Количество покупок'].fillna(0)
    monthly_counts['Количество инвентаризаций'] = monthly_counts['Количество инвентаризаций'].fillna(0)

    # Преобразуем период в строку для оси X
    monthly_counts['month_str'] = monthly_counts['month'].dt.strftime('%m/%y')

    # Сортируем по месяцам в обратном порядке
    monthly_counts = monthly_counts.sort_values('month', ascending=False)

    return monthly_counts[['month_str', 'Количество покупок', 'Количество инвентаризаций']]
