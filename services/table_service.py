
from sqlalchemy.orm import Session
from database.models import Cars
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode
import pandas as pd

def fetch_cars_data(session: Session) -> pd.DataFrame:
    """
    Извлекает все данные из таблицы Cars и возвращает их в формате DataFrame для дальнейшей обработки.
    """
    query = session.query(
        Cars.stockn,
        Cars.make,
        Cars.model,
        Cars.year,
        Cars.color,
        Cars.milage,
        Cars.engine,
        Cars.location,
        Cars.cost,
        Cars.inventoried,
        Cars.breakevendate,
        Cars.dismantled,
        Cars.purchesdate,
        Cars.age,
        Cars.payback,
        Cars.profit,
        Cars.xs,
        Cars.status,
        Cars.import_id
    ).all()

    # Преобразуем результат запроса в DataFrame
    columns = [
        "stockn", "make", "model", "year", "color", "milage", "engine", "location",
        "cost", "inventoried", "breakevendate", "dismantled", "purchesdate", "age",
        "payback", "profit", "xs", "status", "import_id"
    ]
    df = pd.DataFrame(query, columns=columns)
    return df

# Вывод таблиц в app.py
def create_aggrid_table(df, editable=False, height=400, fit_columns_on_grid_load=False):
    # Заменяем NaN в DataFrame
    df = df.fillna('')

    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(
        editable=editable,
        filterable=True,
        sortable=True,
        resizable=True
    )

    # JavaScript для автоматической подстройки ширины колонок
    auto_size_js = JsCode("""
    function onGridReady(params) {
        params.api.sizeColumnsToFit();
    };
    """)

    gb.configure_grid_options(
        onGridReady=auto_size_js,
        suppressColumnVirtualisation=True
    )

    grid_options = gb.build()

    AgGrid(
        df,
        gridOptions=grid_options,
        allow_unsafe_jscode=True,
        fit_columns_on_grid_load=fit_columns_on_grid_load,
        height=height
    )
