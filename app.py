import streamlit as st
import requests
import gspread
from google.oauth2.service_account import Credentials
from gspread import Cell

# ====== КОНСТАНТЫ ======

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
# Подставь сюда свой ID таблицы или используй input в интерфейсе
DEFAULT_SPREADSHEET_ID = "1b3XnnXIoGMaQz2V0ADYii83GkxRVgmZ0B1wgGYT2UyY"

URL_COLUMN_NAME = "Source"        # фиксированное имя колонки с URL
STATUS_COLUMN_NAME = "Response code"  # фиксированное имя колонки с кодом ответа


# ====== АВТОРИЗАЦИЯ В GOOGLE SHEETS ======

@st.cache_resource
def get_gspread_client():
    """
    Создаём gspread-клиента из данных сервисного аккаунта в st.secrets.
    """
    creds_info = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client


def open_spreadsheet(spreadsheet_id: str):
    client = get_gspread_client()
    return client.open_by_key(spreadsheet_id)


@st.cache_data(show_spinner=False)
def list_sheet_names(spreadsheet_id: str):
    """
    Возвращает список названий всех листов в таблице.
    """
    sh = open_spreadsheet(spreadsheet_id)
    return [ws.title for ws in sh.worksheets()]


def ensure_status_column(ws, headers_row):
    """
    Гарантирует, что в листе есть колонка STATUS_COLUMN_NAME.
    Если колонки нет — добавляет её в конец первой строки.
    Возвращает индекс колонки (1-based для gspread).
    """
    if STATUS_COLUMN_NAME in headers_row:
        return headers_row.index(STATUS_COLUMN_NAME) + 1

    # Добавляем новый заголовок в конец
    headers_row.append(STATUS_COLUMN_NAME)
    ws.update("1:1", [headers_row])
    return len(headers_row)


# ====== ЛОГИКА HTTP-ПРОВЕРКИ ======

def check_url_status(url: str) -> str:
    """
    Возвращает HTTP статус-код как строку,
    либо 'Site Not Found', если запрос не удался.
    """
    if not url:
        return ""

    # При желании можно автоматически добавлять https://, если схемы нет
    # но пока используем URL как есть
    try:
        resp = requests.get(
            url,
            allow_redirects=True,
            timeout=10,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            },
        )
        return str(resp.status_code)
    except Exception:
        return "Site Not Found"


# ====== ОБРАБОТКА ЛИСТОВ ======

def preload_sheets_data(spreadsheet_id: str, sheet_names):
    """
    Загружает данные по всем выбранным листам:
    - сами объекты листов
    - все значения
    - индекс колонки Source
    - индекс/создание колонки Response code

    Возвращает:
    {
      sheet_name: {
         "ws": worksheet,
         "values": values,
         "url_col": int,
         "status_col": int,
      }, ...
    }
    а также obщее число URL для прогресса.
    """
    sh = open_spreadsheet(spreadsheet_id)
    sheets_data = {}
    total_urls = 0

    for sheet_name in sheet_names:
        ws = sh.worksheet(sheet_name)
        values = ws.get_all_values()

        if not values:
            sheets_data[sheet_name] = {
                "ws": ws,
                "values": values,
                "url_col": None,
                "status_col": None,
            }
            continue

        headers = values[0]

        if URL_COLUMN_NAME not in headers:
            sheets_data[sheet_name] = {
                "ws": ws,
                "values": values,
                "url_col": None,
                "status_col": None,
            }
            continue

        url_col_index = headers.index(URL_COLUMN_NAME) + 1
        status_col_index = ensure_status_column(ws, headers)

        # Подсчитываем количество непустых URL
        sheet_url_count = 0
        for row in values[1:]:
            if len(row) >= url_col_index:
                url = (row[url_col_index - 1] or "").strip()
            else:
                url = ""
            if url:
                sheet_url_count += 1
        total_urls += sheet_url_count

        sheets_data[sheet_name] = {
            "ws": ws,
            "values": values,
            "url_col": url_col_index,
            "status_col": status_col_index,
        }

    return sheets_data, total_urls


def process_sheets(spreadsheet_id: str, sheet_names, progress, status_placeholder):
    """
    Основная функция обработки:
    - идём по всем выбранным листам
    - для каждой строки с URL делаем запрос
    - записываем код ответа в колонку Response code
    - обновляем прогресс
    """
    sheets_data, total_urls = preload_sheets_data(spreadsheet_id, sheet_names)

    if total_urls == 0:
        st.warning("Не найдено ни одного URL в колонке 'Source' на выбранных листах.")
        return []

    processed = 0
    results_summary = []

    for sheet_name in sheet_names:
        data = sheets_data[sheet_name]
        ws = data["ws"]
        values = data["values"]
        url_col = data["url_col"]
        status_col = data["status_col"]

        if not values:
            results_summary.append(
                {
                    "sheet": sheet_name,
                    "total_urls": 0,
                    "processed_urls": 0,
                }
            )
            continue

        if url_col is None:
            st.warning(f"В листе '{sheet_name}' не найдена колонка '{URL_COLUMN_NAME}'. Пропускаю его.")
            results_summary.append(
                {
                    "sheet": sheet_name,
                    "total_urls": 0,
                    "processed_urls": 0,
                }
            )
            continue

        # Собираем ячейки для обновления
        cells_to_update = []
        sheet_total_urls = 0
        sheet_processed_urls = 0

        for row_idx, row in enumerate(values[1:], start=2):  # начиная со строки 2 (после заголовков)
            # Берём URL из колонки Source
            if len(row) >= url_col:
                url = (row[url_col - 1] or "").strip()
            else:
                url = ""

            if not url:
                continue

            sheet_total_urls += 1

            status = check_url_status(url)
            sheet_processed_urls += 1
            processed += 1

            # Формируем объект ячейки для gspread.update_cells
            cells_to_update.append(Cell(row=row_idx, col=status_col, value=status))

            # Обновляем прогресс и статус
            progress.progress(processed / total_urls)
            status_placeholder.write(
                f"Лист: **{sheet_name}** — обработано {sheet_processed_urls} из {sheet_total_urls} "
                f"(всего по всем листам: {processed} / {total_urls})"
            )

        # Пакетное обновление гугл-таблицы для этого листа
        if cells_to_update:
            ws.update_cells(cells_to_update)

        results_summary.append(
            {
                "sheet": sheet_name,
                "total_urls": sheet_total_urls,
                "processed_urls": sheet_processed_urls,
            }
        )

    return results_summary


# ====== STREAMLIT UI ======

def main():
    st.set_page_config(page_title="URL Response Code Checker", layout="wide")
    st.title("🔎 URL Response Code Checker (Google Sheets → Streamlit)")
    st.write(
        "Приложение читает URL из колонки **'Source'** в выбранных листах Google Sheets, "
        "проверяет HTTP-код ответа и записывает его в колонку **'Response code'**."
    )

    st.markdown("### 1. Настройки таблицы")

    spreadsheet_id = st.text_input(
        "ID Google таблицы",
        help="Можно взять из URL вида https://docs.google.com/spreadsheets/d/ИД_ТАБЛИЦЫ/edit",
        value=DEFAULT_SPREADSHEET_ID,
    )

    if not spreadsheet_id:
        st.stop()

    # Загружаем список листов
    if st.button("Загрузить список листов"):
        try:
            sheet_names = list_sheet_names(spreadsheet_id)
            st.session_state["sheet_names"] = sheet_names
            st.success(f"Найдено листов: {len(sheet_names)}")
        except Exception as e:
            st.error(f"Не удалось загрузить таблицу: {e}")

    sheet_names = st.session_state.get("sheet_names", None)

    if sheet_names:
        st.markdown("### 2. Выбор листов для обработки")
        selected_sheets = st.multiselect(
            "Выбери один или несколько листов",
            options=sheet_names,
            default=sheet_names,  # по умолчанию все
        )

        if not selected_sheets:
            st.info("Выбери хотя бы один лист.")
            st.stop()

        st.markdown("### 3. Запуск проверки URL")

        run_button = st.button("🚀 Запустить проверку")

        if run_button:
            progress = st.progress(0)
            status_placeholder = st.empty()

            with st.spinner("Идёт обработка URL..."):
                summary = process_sheets(
                    spreadsheet_id=spreadsheet_id,
                    sheet_names=selected_sheets,
                    progress=progress,
                    status_placeholder=status_placeholder,
                )

            st.success("Обработка завершена ✅")

            # Итоговая статистика
            st.markdown("### 4. Итоги обработки")

            total_urls = sum(item["total_urls"] for item in summary)
            total_processed = sum(item["processed_urls"] for item in summary)

            st.write(f"Всего URL найдено: **{total_urls}**, обработано: **{total_processed}**")

            st.table(summary)
    else:
        st.info("Сначала нажми кнопку **«Загрузить список листов»** после ввода ID таблицы.")


if __name__ == "__main__":
    main()
