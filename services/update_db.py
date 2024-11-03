from datetime import date, datetime
from sqlalchemy.orm import Session
from database.models import Cars, Profits
from services.calculate import calculate_age, calculate_profit, calculate_xs
from database.db import SessionLocal

# Функция для обновления значений profit и xs для всех автомобилей
def update_profit_and_xs():
    session: Session = SessionLocal()
    try:
        cars = session.query(Cars).all()
        for car in cars:
            # Получаем последний cumulative_amount для stockn
            last_cumulative_amount = (
                session.query(Profits.cumulative_amount)
                .filter(Profits.stockn == car.stockn)
                .order_by(Profits.date.desc())
                .first()
            )
            if last_cumulative_amount:
                car.profit = calculate_profit(session, car.stockn, car.cost)
                car.xs = calculate_xs(session, car.stockn, car.cost)
        session.commit()
        print("Profit и Xs обновлены для всех автомобилей.")
    except Exception as e:
        session.rollback()
        print(f"Ошибка при обновлении profit и xs: {e}")
    finally:
        session.close()

# Функция для обновления profit и xs при каждом новом импорте
def update_profit_history():
    session: Session = SessionLocal()
    try:
        profits = session.query(Profits).all()
        for profit in profits:
            car = session.query(Cars).filter_by(stockn=profit.stockn).first()
            if car:
                # Обновляем profit и xs для каждой новой записи Profits
                profit.change_amount = profit.cumulative_amount - car.profit if car.profit else None
                car.profit = calculate_profit(session, car.stockn, car.cost)
                car.xs = calculate_xs(session, car.stockn, car.cost)
        session.commit()
        print("ProfitHistory обновлен.")
    except Exception as e:
        session.rollback()
        print(f"Ошибка при обновлении ProfitHistory: {e}")
    finally:
        session.close()

# Функция для обновления age один раз в день при запуске
def update_age_daily():
    session: Session = SessionLocal()
    today = date.today()
    # Проверяем последнюю дату обновления в базе данных
    last_update = session.query(Cars).filter(Cars.age.isnot(None)).first()
    if last_update and last_update.age_last_updated == today:
        # Если age уже обновлен сегодня, пропускаем
        print("Age уже обновлен сегодня.")
        session.close()
        return

    # Обновляем age для всех автомобилей
    try:
        cars = session.query(Cars).all()
        for car in cars:
            car.age = calculate_age(car.inventoried)
            car.age_last_updated = today  # Сохраняем дату обновления
        session.commit()
        print("Age обновлен для всех автомобилей.")
    except Exception as e:
        session.rollback()
        print(f"Ошибка при обновлении age: {e}")
    finally:
        session.close()
