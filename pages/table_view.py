import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode
from sqlalchemy.orm import Session
from database.db import SessionLocal
from database.models import Cars, Profits
import pandas as pd

# Функции для работы с таблицами

def fetch_data(table_model):
    """Получение всех данных из таблицы с сортировкой по убыванию stockn."""
    session: Session = SessionLocal()
    try:
        data = session.query(table_model).order_by(table_model.stockn.desc()).all()
        return data
    except Exception as e:
        st.error(f"Ошибка при получении данных: {e}")
    finally:
        session.close()

def sanitize_value(value):
    """Преобразование NaN в None для сохранения в базе данных."""
    return None if pd.isna(value) else value

def has_changes(updated_row, original_row):
    """Проверка, были ли реальные изменения в строке."""
    for key in updated_row:
        if key != 'id':
            if updated_row[key] != original_row[key]:
                return True
    return False

def update_data(table_model, updated_rows, original_rows):
    """Обновление данных в базе для измененных строк."""
    session: Session = SessionLocal()
    try:
        # Создаем словарь исходных строк по 'id'
        original_rows_by_id = {row['id']: row for row in original_rows}

        for updated_row in updated_rows:
            row_id = updated_row.get('id')
            original_row = original_rows_by_id.get(row_id)
            if original_row and has_changes(updated_row, original_row):
                record = session.query(table_model).filter_by(id=row_id).first()
                if record:
                    for key, value in updated_row.items():
                        if key != 'id':  # Не изменяем 'id'
                            setattr(record, key, sanitize_value(value))
        session.commit()
    except Exception as e:
        session.rollback()
        st.error(f"Ошибка при обновлении данных: {e}")
    finally:
        session.close()

def render_table(table_model, table_name, column_order):
    """Отображение таблицы с возможностью редактирования и сохранения изменений в базу данных."""
    data = fetch_data(table_model)

    if not data:
        st.warning(f"Таблица {table_name} пуста или данные не найдены.")
        return

    rows = [record.__dict__ for record in data]
    for row in rows:
        row.pop('_sa_instance_state', None)

    df = pd.DataFrame(rows)
    df = df[column_order]
    original_rows = df.to_dict(orient='records')

    # Создание GridOptionsBuilder из DataFrame
    gb = GridOptionsBuilder.from_dataframe(df)

    # Настройка свойств по умолчанию для колонок
    gb.configure_default_column(
        editable=True,
        filterable=True,
        sortable=True,
        resizable=True
    )

    # Для таблицы Profits делаем колонки 'id', 'stockn' и 'date' нередактируемыми
    if table_name == "Profits":
        gb.configure_column('id', editable=False)
        gb.configure_column('stockn', editable=False)
        gb.configure_column('date', editable=False)

    # JavaScript-код для автоматической подстройки ширины колонок
    auto_size_js = JsCode("""
    function(params) {
        params.api.gridOptionsWrapper.gridOptions.suppressColumnVirtualisation = true;
        let allColumnIds = [];
        params.columnApi.getAllColumns().forEach(function(column) {
            allColumnIds.push(column.colId);
        });
        params.columnApi.autoSizeColumns(allColumnIds, false);
    }
    """)

    # Настройка опций грида
    gb.configure_grid_options(
        suppressColumnVirtualisation=True,
        onFirstDataRendered=auto_size_js
    )

    grid_options = gb.build()

    # Отображение таблицы с AgGrid
    grid_response = AgGrid(
        df,
        gridOptions=grid_options,
        update_mode=GridUpdateMode.MODEL_CHANGED,
        editable=True,
        allow_unsafe_jscode=True,
        fit_columns_on_grid_load=False
    )

    updated_df = grid_response['data']
    updated_rows = updated_df.to_dict(orient='records')

    # Проверяем, есть ли изменения
    if not df.equals(updated_df):
        update_data(table_model, updated_rows, original_rows)

# Главная функция
def main():
    # Отображаем таблицу Cars
    st.subheader("Таблица Cars")
    render_table(
        Cars,
        "Cars",
        column_order=[
            "stockn", "make", "model", "year", "color", "milage", "engine",
            "location", "cost", "inventoried", "breakevendate", "status", "dismantled", "import_id", "age", "payback", "profit", "xs"
        ]
    )

    # Отображаем таблицу Profits
    st.subheader("Таблица Profits")
    render_table(
        Profits,
        "Profits",
        column_order=["id", "stockn", "date", "cumulative_amount", "change_amount", "import_id"]
    )

if __name__ == "__main__":
    main()
