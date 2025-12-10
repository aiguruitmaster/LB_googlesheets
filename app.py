import streamlit as st
import requests
import gspread
import urllib3
import pandas as pd
from urllib.parse import urlparse
from google.oauth2.service_account import Credentials
from gspread import Cell

# Disable warnings for requests with verify=False (invalid/old SSL certificates)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ====== CONSTANTS ======

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Your spreadsheet ID – stored only in code, not visible to users
SPREADSHEET_ID = "1b3XnnXIoGMaQz2V0ADYii83GkxRVgmZ0B1wgGYT2UyY"

URL_COLUMN_NAME = "Source"            # URL column name
STATUS_COLUMN_NAME = "Response code"  # HTTP status column name


# ====== GOOGLE SHEETS AUTH ======

@st.cache_resource
def get_gspread_client():
    """
    Build a gspread client from service account info stored in st.secrets['gcp_service_account'].
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
    Return a list of sheet names for the given spreadsheet.
    """
    sh = open_spreadsheet(spreadsheet_id)
    return [ws.title for ws in sh.worksheets()]


def ensure_status_column(ws, headers_row):
    """
    Make sure the sheet has a STATUS_COLUMN_NAME column.
    If not – append it to the header row.
    Return its 1-based column index.
    """
    if STATUS_COLUMN_NAME in headers_row:
        return headers_row.index(STATUS_COLUMN_NAME) + 1

    # Append new header at the end
    headers_row.append(STATUS_COLUMN_NAME)
    ws.update("1:1", [headers_row])
    return len(headers_row)


# ====== HTTP CHECK LOGIC ======

def normalize_url(url: str) -> str:
    """
    Normalize URL:
    - trim spaces
    - if scheme is missing, default to http://
    - if starts with //domain, also add http://
    """
    if not url:
        return ""

    url = url.strip()

    # //example.com/path -> http://example.com/path
    if url.startswith("//"):
        return "http:" + url

    parsed = urlparse(url)

    if not parsed.scheme:
        # no scheme at all -> assume http
        return "http://" + url

    return url


def _do_request(url: str) -> str:
    """
    Single HTTP request with our common options.
    Returns status code as string.
    """
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
        verify=False,  # ignore invalid / self-signed SSL certs
    )
    return str(resp.status_code)


def check_url_status(url: str) -> str:
    """
    Try:
    1) given URL (normalized)
    2) if it was https:// and raised an error –
       retry with http:// (no SSL).
    """
    url = normalize_url(url)
    if not url:
        return ""

    # 1st attempt — as is
    try:
        return _do_request(url)
    except Exception:
        # If original URL was https:// – try http://
        if url.startswith("https://"):
            http_url = "http://" + url[len("https://"):]
            try:
                return _do_request(http_url)
            except Exception:
                return "Site Not Found"
        else:
            return "Site Not Found"


# ====== SHEETS PROCESSING ======

def preload_sheets_data(spreadsheet_id: str, sheet_names):
    """
    Load data for all selected sheets:
    - worksheet objects
    - all values
    - index of URL column
    - index (create if needed) of status column

    Returns:
    sheets_data: {
        sheet_name: {
            "ws": worksheet,
            "values": values,
            "url_col": int or None,
            "status_col": int or None,
        }, ...
    }
    total_urls: total count of URL rows across all sheets.
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

        # Count non-empty URLs
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


def process_sheets(sheet_names, progress, status_placeholder):
    """
    Main processing function:
    - go through all selected sheets
    - for each row with a URL make HTTP request
    - write status code into the 'Response code' column
    - update Streamlit progress

    Returns:
      summary: per-sheet stats
      detailed_results: list of {sheet, row, url, status}
    """
    sheets_data, total_urls = preload_sheets_data(SPREADSHEET_ID, sheet_names)

    if total_urls == 0:
        st.warning("No URLs found in 'Source' column on selected sheets.")
        return [], []

    processed = 0
    results_summary = []
    detailed_results = []

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
            st.warning(f"Sheet '{sheet_name}' does not contain column '{URL_COLUMN_NAME}'. Skipping.")
            results_summary.append(
                {
                    "sheet": sheet_name,
                    "total_urls": 0,
                    "processed_urls": 0,
                }
            )
            continue

        cells_to_update = []
        sheet_total_urls = 0
        sheet_processed_urls = 0

        # Start from row 2 (row 1 is header)
        for row_idx, row in enumerate(values[1:], start=2):
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

            cells_to_update.append(Cell(row=row_idx, col=status_col, value=status))

            # Save detailed info
            detailed_results.append(
                {
                    "sheet": sheet_name,
                    "row": row_idx,
                    "url": url,
                    "status": status,
                }
            )

            # Update progress & status text
            progress.progress(processed / total_urls)
            status_placeholder.write(
                f"Sheet: **{sheet_name}** — processed {sheet_processed_urls} of {sheet_total_urls} "
                f"(total: {processed} / {total_urls})"
            )

        # Batch update for this sheet
        if cells_to_update:
            ws.update_cells(cells_to_update)

        results_summary.append(
            {
                "sheet": sheet_name,
                "total_urls": sheet_total_urls,
                "processed_urls": sheet_processed_urls,
            }
        )

    return results_summary, detailed_results


# ====== STREAMLIT UI ======

def main():
    st.set_page_config(page_title="URL Response Code Checker", layout="wide")

    st.title("🔎 URL Response Code Checker")
    st.write(
        "This app reads URLs from the **'Source'** column in selected Google Sheets tabs, "
        "checks the HTTP response code, and writes it back to the **'Response code'** column."
    )

    # Load sheet names automatically (spreadsheet id is hidden in code)
    try:
        sheet_names = list_sheet_names(SPREADSHEET_ID)
    except Exception as e:
        st.error(
            "Unable to load the spreadsheet. "
            "Please check your service account permissions and the spreadsheet ID in the code."
        )
        st.exception(e)
        st.stop()

    if not sheet_names:
        st.warning("No sheets found in the spreadsheet.")
        st.stop()

    st.markdown("### 1. Select sheets to process")
    selected_sheets = st.multiselect(
        "Sheets",
        options=sheet_names,
        default=sheet_names,  # by default – all sheets
    )

    if not selected_sheets:
        st.info("Please select at least one sheet to process.")
        # Still may want to show previous results if they exist
    run_button = st.button("🚀 Run check")

    if run_button and selected_sheets:
        progress = st.progress(0)
        status_placeholder = st.empty()

        with st.spinner("Processing URLs..."):
            summary, details = process_sheets(
                sheet_names=selected_sheets,
                progress=progress,
                status_placeholder=status_placeholder,
            )

        st.success("Processing finished ✅")

        # Save results in session_state so filters work without re-processing
        st.session_state["last_summary"] = summary
        st.session_state["last_details"] = details

    # ====== SHOW RESULTS (if any) ======
    if "last_summary" in st.session_state:
        summary = st.session_state["last_summary"]
        details = st.session_state["last_details"]

        st.markdown("### 2. Summary")

        total_urls = sum(item["total_urls"] for item in summary)
        total_processed = sum(item["processed_urls"] for item in summary)

        st.write(f"Total URLs found: **{total_urls}**, processed: **{total_processed}**")
        st.table(summary)

        # Detailed results + filter
        st.markdown("### 3. Detailed results")

        if details:
            status_values = sorted(set(item["status"] for item in details))
            selected_statuses = st.multiselect(
                "Filter by status code",
                options=status_values,
                default=status_values,
            )

            filtered = [r for r in details if r["status"] in selected_statuses]

            if filtered:
                df = pd.DataFrame(filtered)[["sheet", "row", "url", "status"]]
                st.dataframe(df, use_container_width=True)
            else:
                st.info("No rows match the selected status codes.")
        else:
            st.info("No detailed results available yet.")


if __name__ == "__main__":
    main()
