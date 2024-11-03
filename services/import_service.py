import pandas as pd
import streamlit as st
import re
from sqlalchemy.orm import Session
from database.models import Cars, Profits
from database.db import SessionLocal
from datetime import datetime
from services.calculate import calculate_age, calculate_payback, calculate_profit, calculate_xs
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)

def clean_milage(milage_value: str) -> str:
    """Удаляет все нецифровые символы из пробега."""
    return re.sub(r'\D', '', milage_value) if milage_value else None

def calculate_change_amount_for_import(session, stockn, new_import_date, new_cumulative_amount, import_id):
    """
    Рассчитывает и добавляет новую запись в таблицу Profits для заданного stockn и даты импорта.
    Если запись уже существует для stockn и даты, она не будет обновляться.
    Если cumulative_amount отсутствует, change_amount устанавливается в 0.
    """
    from sqlalchemy import func

    # Проверяем, существует ли запись для данного stockn и даты
    existing_record = (
        session.query(Profits)
        .filter(
            Profits.stockn == stockn,
            func.date(Profits.date) == new_import_date
        )
        .first()
    )

    if existing_record:
        logging.info(f"Запись уже существует для stockn {stockn} на дату {new_import_date}. Пропускаем.")
        return False  # Новая запись не добавлена

    # Рассчитываем change_amount
    if new_cumulative_amount is None:
        change_amount = 0
    else:
        # Находим предыдущий cumulative_amount для данного stockn
        previous_profit = (
            session.query(Profits.cumulative_amount)
            .filter(
                Profits.stockn == stockn,
                Profits.date < new_import_date
            )
            .order_by(Profits.date.desc())
            .first()
        )
        if previous_profit:
            previous_cumulative_amount = previous_profit[0]
            change_amount = new_cumulative_amount - previous_cumulative_amount
        else:
            change_amount = new_cumulative_amount

    # Создаем новую запись и устанавливаем import_id
    new_profit = Profits(
        stockn=stockn,
        date=new_import_date,
        cumulative_amount=new_cumulative_amount,
        change_amount=change_amount,
        import_id=import_id  # Устанавливаем import_id
    )
    session.add(new_profit)
    logging.info(f"Добавлена новая запись для stockn {stockn} на дату {new_import_date}: change_amount {change_amount}")
    return True  # Новая запись добавлена

import pandas as pd
import re
from sqlalchemy.orm import Session
from database.models import Cars, Profits
from database.db import SessionLocal
from datetime import datetime
from services.calculate import calculate_age, calculate_payback, calculate_profit, calculate_xs
import logging
import streamlit as st  # Добавляем импорт streamlit для очистки кеша

# Настройка логирования
logging.basicConfig(level=logging.INFO)

def import_data_from_excel(file, selected_date: str, color_mileage_engine: bool = False) -> dict:
    session: Session = SessionLocal()
    cars_added = 0
    cars_updated = 0
    profits_added = 0

    try:
        import_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df = pd.read_excel(file).where(pd.notnull, None)

        # Проверяем названия столбцов
        print("Названия столбцов в DataFrame:", df.columns.tolist())

        if not color_mileage_engine:
            # Обработка первого типа файла
            # Приводим названия столбцов к нижнему регистру и удаляем пробелы
            df.columns = df.columns.str.strip().str.lower()

            # Переименовываем столбец 'purchasedate' в 'purchesdate' для консистентности
            if 'purchasedate' in df.columns:
                df.rename(columns={'purchasedate': 'purchesdate'}, inplace=True)
        else:
            # Обработка второго типа файла
            # Оставляем названия столбцов без изменений
            pass

        # Преобразуем selected_date в объект date, если это строка
        if isinstance(selected_date, str):
            selected_date = datetime.strptime(selected_date, '%Y-%m-%d').date()

        # Преобразуем значения столбцов с датами и заменяем NaT на None
        if not color_mileage_engine:
            date_columns = ['inventoried', 'breakevendate', 'dismantled', 'purchesdate']
        else:
            date_columns = []

        for column in date_columns:
            if column in df.columns:
                df[column] = pd.to_datetime(df[column], errors='coerce', dayfirst=True).dt.date
                df[column] = df[column].apply(lambda x: x if pd.notnull(x) else None)

        if df.empty:
            logging.error("Файл Excel пустой или содержит некорректные данные.")
            return {"cars_added": 0, "cars_updated": 0, "profits_added": 0}

        # Словарь для хранения обновленных автомобилей
        updated_cars = {}

        for _, row in df.iterrows():
            if not color_mileage_engine:
                # Первый тип файла
                stockn = int(row.get('vstockno', 0))
            else:
                # Второй тип файла
                stockn = int(row.get('Stock #', 0))

            if stockn < 10300:
                logging.info(f"Пропущен stockn={stockn}, так как он меньше 10300.")
                continue

            car = session.query(Cars).filter(Cars.stockn == stockn).first()
            if not car:
                if not color_mileage_engine:
                    # Добавление нового автомобиля в таблицу Cars, если его нет и это первый тип файла
                    car = Cars(
                        stockn=stockn,
                        color=row.get('color'),
                        milage=clean_milage(row.get('odo reading')),
                        engine=row.get('engine'),
                        import_id=import_id,
                    )
                    # Заполнение дополнительных полей для первого типа файла
                    car.make = row.get('manufacturer')
                    car.model = row.get('modelname')
                    car.year = row.get('modelyear')
                    car.location = create_location(row.get('bin'), row.get('xcoord'))
                    car.cost = row.get('cost')
                    car.inventoried = row.get('inventoried')
                    car.purchesdate = row.get('purchesdate')
                    car.breakevendate = row.get('breakevendate')
                    car.dismantled = row.get('dismantled')
                    car.status = 'scrap' if car.dismantled else 'active'
                    car.age = calculate_age(car.inventoried) if car.inventoried else None
                    car.payback = calculate_payback(car.breakevendate, car.inventoried) if car.breakevendate and car.inventoried else None

                    session.add(car)
                    cars_added += 1
                else:
                    # Если автомобиль не найден и это второй тип файла, пропускаем обработку
                    continue
            else:
                # Обновление существующего автомобиля
                if not color_mileage_engine:
                    # Обновление всех полей для первого типа файла
                    if row.get('manufacturer') is not None:
                        car.make = row.get('manufacturer')
                    if row.get('modelname') is not None:
                        car.model = row.get('modelname')
                    if row.get('modelyear') is not None:
                        car.year = row.get('modelyear')
                    if row.get('cost') is not None:
                        car.cost = row.get('cost')
                    if row.get('inventoried') is not None:
                        car.inventoried = row.get('inventoried')
                    if row.get('purchesdate') is not None:
                        car.purchesdate = row.get('purchesdate')
                    if row.get('breakevendate') is not None:
                        car.breakevendate = row.get('breakevendate')
                    if row.get('dismantled') is not None:
                        car.dismantled = row.get('dismantled')
                        car.status = 'scrap' if car.dismantled else 'active'
                    # Пересчитываем age и payback после обновления дат
                    car.age = calculate_age(car.inventoried) if car.inventoried else None
                    car.payback = calculate_payback(car.breakevendate, car.inventoried) if car.breakevendate and car.inventoried else None
                    cars_updated += 1
                else:
                    # Обновление только полей color, milage, engine для второго типа файла
                    if row.get('Color') is not None:
                        car.color = row.get('Color')
                    if row.get('Odo Reading') is not None:
                        car.milage = clean_milage(row.get('Odo Reading'))
                    if row.get('Engine') is not None:
                        car.engine = row.get('Engine')
                    cars_updated += 1

            # Сохраняем обновленный автомобиль для последующего расчета
            if not color_mileage_engine:
                updated_cars[stockn] = car

        # Обработка данных для Profits (только если color_mileage_engine=False)
        if not color_mileage_engine:
            for _, row in df.iterrows():
                stockn = int(row.get('vstockno', 0))
                if stockn < 10400:
                    continue
                cumulative_amount = row.get('sales')
                new_profit_added = calculate_change_amount_for_import(
                    session,
                    stockn,
                    selected_date,
                    cumulative_amount,
                    import_id  # Передаем import_id в функцию
                )
                if new_profit_added:
                    profits_added += 1

            # Сбрасываем изменения в сессии, чтобы новые записи Profits были видны
            session.flush()

            # Рассчитываем profit и xs для обновленных автомобилей
            for stockn, car in updated_cars.items():
                car.profit = calculate_profit(session, stockn, car.cost)
                car.xs = calculate_xs(session, stockn, car.cost)

        # Сохранение всех изменений
        session.commit()

        # Очистка кеша после успешного импорта
        st.cache_data.clear()

        return {"cars_added": cars_added, "cars_updated": cars_updated, "profits_added": profits_added}

    except Exception as e:
        session.rollback()
        logging.error(f"Ошибка при импорте данных: {e}")
        return {"cars_added": 0, "cars_updated": 0, "profits_added": 0}
    finally:
        session.close()

def clean_milage(milage_value: str) -> str:
    """Удаляет все нецифровые символы из пробега."""
    return re.sub(r'\D', '', milage_value) if milage_value else None

def create_location(bin_value, xcoord_value) -> str:
    """Создает строку location на основе bin и xcoord, убирая .0, если значение целое."""

    # Если значение bin или xcoord является числом с десятичной частью ".0", удаляем её
    if pd.notna(bin_value):
        bin_value = str(bin_value).rstrip('.0')
    else:
        bin_value = ''

    if pd.notna(xcoord_value):
        xcoord_value = str(xcoord_value).rstrip('.0')
    else:
        xcoord_value = ''

    # Собираем location
    if bin_value and xcoord_value:
        return f"{bin_value}.{xcoord_value}"
    return bin_value or xcoord_value

def create_location(bin_value, xcoord_value) -> str:
    """Создает строку location на основе bin и xcoord, убирая .0, если значение целое."""

    # Если значение bin или xcoord является числом с десятичной частью ".0", удаляем её
    if pd.notna(bin_value):
        bin_value = str(bin_value).rstrip('.0')
    else:
        bin_value = ''

    if pd.notna(xcoord_value):
        xcoord_value = str(xcoord_value).rstrip('.0')
    else:
        xcoord_value = ''

    # Собираем location
    if bin_value and xcoord_value:
        return f"{bin_value}.{xcoord_value}"
    return bin_value or xcoord_value
