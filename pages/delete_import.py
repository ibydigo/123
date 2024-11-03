import streamlit as st
from services.delete_service import delete_data_by_import_id, get_all_import_ids

def main():
    st.title("Удаление данных по import_id")

    # Получаем все доступные import_id
    import_ids = get_all_import_ids()

    if not import_ids:
        st.warning("Доступных import_id для удаления не найдено.")
        return

    # Выводим выпадающий список для выбора import_id
    selected_import_id = st.selectbox("Выберите import_id для удаления данных", import_ids)

    # Кнопка для удаления данных
    if st.button("Удалить данные"):
        if selected_import_id:
            result = delete_data_by_import_id(selected_import_id)
            st.success(f"Удалено записей: Cars - {result['cars_deleted']}, Profits - {result['profits_deleted']}")
        else:
            st.error("Пожалуйста, выберите import_id")

if __name__ == "__main__":
    main()