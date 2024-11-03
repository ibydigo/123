from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database.models import Base  # Импортируем Base из models.py
import config  # Файл, где указана строка подключения к БД

# Создаем движок для PostgreSQL
engine = create_engine(config.DATABASE_URL)

# Создаем сессию
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Создание таблиц
def create_database():
    Base.metadata.create_all(bind=engine)
    print("База данных и таблицы созданы успешно.")

if __name__ == "__main__":
    create_database()