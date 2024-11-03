import streamlit as st
from datetime import date
from services.import_service import import_data_from_excel

def main():
    st.title("Импорт данных")

    # Поле для загрузки файла Excel
    uploaded_file = st.file_uploader("Загрузите файл Excel", type=["xlsx"])

    # Поле для выбора даты
    selected_date = st.date_input("Выберите дату для записи в таблицу Profits", value=date.today())

    # Поле для галочки, если импортируем только color, mileage, engine
    color_mileage_engine = st.checkbox("Импортировать только color, mileage, engine")

    # Кнопка для запуска импорта
    if st.button("Импортировать данные"):
        if uploaded_file:
            # Выполняем импорт данных, передавая все параметры
            result = import_data_from_excel(uploaded_file, selected_date.strftime("%Y-%m-%d"), color_mileage_engine)

            # Выводим детализированную информацию по каждой таблице
            st.success(
                f"Импорт завершен успешно!\n"
                f"Добавлено в Cars: {result['cars_added']} строк, Обновлено в Cars: {result['cars_updated']} строк.\n"
                f"Добавлено в Profits: {result['profits_added']} строк."
            )

        else:
            st.error("Пожалуйста, загрузите файл для импорта.")

if __name__ == "__main__":
    main()
