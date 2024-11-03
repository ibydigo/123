from sqlalchemy import Column, Integer, String, Date, Float, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# Модель для таблицы Cars
class Cars(Base):
    __tablename__ = 'cars'

    id = Column(Integer, primary_key=True, index=True)
    stockn = Column(Integer, index=True)
    make = Column(String)
    model = Column(String)
    year = Column(Integer)
    color = Column(String)
    milage = Column(Float)
    engine = Column(String)
    location = Column(String)
    cost = Column(Float)
    inventoried = Column(Date)  # Изменен на Date
    breakevendate = Column(Date)  # Изменен на Date
    dismantled = Column(Date)  # Изменен на Date
    purchesdate = Column(Date)  # Новый столбец с датой покупки
    age = Column(Integer)  # Возраст (владение)
    payback = Column(Integer)  # Время до окупаемости
    profit = Column(Float)  # Прибыль
    xs = Column(Float)
    status = Column(String)
    import_id = Column(String)
    age_last_updated = Column(Date)

# Модель для таблицы Profits
class Profits(Base):
    __tablename__ = 'profits'
    __table_args__ = (
        UniqueConstraint('stockn', 'date', name='_stockn_date_uc'),
    )

    id = Column(Integer, primary_key=True, index=True)
    stockn = Column(Integer, index=True)  # stockn теперь Integer
    date = Column(Date)  # Изменен на Date
    cumulative_amount = Column(Float)
    change_amount = Column(Float)  # Новый столбец для хранения разницы
    import_id = Column(String)
