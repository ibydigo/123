from sqlalchemy.orm import Session
from sqlalchemy import func
from database.models import Cars, Profits
from database.db import SessionLocal
from services.calculate import calculate_profit, calculate_xs, calculate_payback

def get_all_import_ids() -> list:
    """
    Возвращает список всех уникальных import_id из таблиц Cars и Profits.

    :return: Список уникальных import_id
    """
    session: Session = SessionLocal()

    try:
        # Получаем уникальные import_id из всех таблиц
        cars_import_ids = session.query(Cars.import_id).distinct().all()
        profits_import_ids = session.query(Profits.import_id).distinct().all()

        # Объединяем результаты в один список и удаляем дубликаты
        all_import_ids = set(
            [id[0] for id in cars_import_ids] +
            [id[0] for id in profits_import_ids]
        )

        return sorted(list(all_import_ids))

    except Exception as e:
        print(f"Ошибка при получении import_id: {e}")
        return []

    finally:
        session.close()

def delete_data_by_import_id(import_id: str) -> dict:
    """
    Удаляет все данные, связанные с заданным import_id, из таблиц Cars и Profits.
    Если удаляемый import_id является последним, пересчитывает значения в Cars.

    :param import_id: Идентификатор импорта
    :return: Словарь с количеством удалённых строк из каждой таблицы
    """
    session: Session = SessionLocal()

    try:
        # Определяем, является ли удаляемый import_id последним
        latest_import_id = session.query(Profits.import_id).order_by(Profits.date.desc()).first()
        is_latest_import = (latest_import_id and latest_import_id[0] == import_id)

        # Удаляем записи из таблицы Cars
        deleted_cars = session.query(Cars).filter(Cars.import_id == import_id).delete()

        # Удаляем записи из таблицы Profits
        deleted_profits = session.query(Profits).filter(Profits.import_id == import_id).delete()

        # Применяем изменения
        session.commit()

        # Если удалён последний импорт, пересчитываем значения в таблице Cars
        if is_latest_import:
            recalculate_cars_data(session)

        return {
            "cars_deleted": deleted_cars,
            "profits_deleted": deleted_profits
        }

    except Exception as e:
        session.rollback()
        print(f"Ошибка при удалении данных: {e}")
        return {"cars_deleted": 0, "profits_deleted": 0}
    finally:
        session.close()

def recalculate_cars_data(session: Session):
    """
    Пересчитывает значения profit, xs и payback в таблице Cars на основе оставшихся данных в Profits.
    """
    cars = session.query(Cars).all()
    for car in cars:
        # Рассчитываем значения на основе оставшихся записей в Profits
        car.profit = calculate_profit(session, car.stockn, car.cost) if car.cost else None
        car.xs = calculate_xs(session, car.stockn, car.cost) if car.cost else None
        car.payback = calculate_payback(car.breakevendate, car.inventoried) if car.inventoried and car.breakevendate else None
    session.commit()
