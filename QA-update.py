#!/usr/bin/env python3
import os
import json
import logging
import time
import io

import pandas as pd
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
from gspread.exceptions import APIError, WorksheetNotFound
from gspread.utils import rowcol_to_a1
from requests.exceptions import RequestException, ReadTimeout

# —————————————————————————————
SOURCE_SS_ID      = "1gV9STzFPKMeIkVO6MFILzC-v2O6cO3XZyi4sSstgd8A"
SOURCE_SHEET_NAME = "All lesson reviews"
DEST_SS_ID        = "16QrbLtzLTV6GqyT8HYwzcwYIsXewzjUbM0Jy5i1fENE"
DEST_SHEET_NAME   = "QA - Lesson evaluation"
# —————————————————————————————

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def api_retry_open(client, key, max_attempts=5, backoff=1.0):
    for i in range(1, max_attempts+1):
        try:
            logging.info(f"open_by_key attempt {i}/{max_attempts}")
            return client.open_by_key(key)
        except APIError as e:
            code = getattr(e.response, "status_code", None) or getattr(e.response, "status", None)
            if code and 500 <= int(code) < 600 and i < max_attempts:
                logging.warning(f"open_by_key got {code}, retrying in {backoff:.1f}s")
                time.sleep(backoff); backoff *= 2
                continue
            raise


def api_retry_worksheet(sh, title, max_attempts=5, backoff=1.0):
    for i in range(1, max_attempts+1):
        try:
            logging.info(f"worksheet('{title}') attempt {i}/{max_attempts}")
            return sh.worksheet(title)
        except APIError as e:
            code = getattr(e.response, "status_code", None) or getattr(e.response, "status", None)
            if code and 500 <= int(code) < 600 and i < max_attempts:
                logging.warning(f"worksheet got {code}, retrying in {backoff:.1f}s")
                time.sleep(backoff); backoff *= 2
                continue
            raise
        except WorksheetNotFound:
            logging.error(f"Worksheet '{title}' not found")
            raise


def fetch_csv_with_retries(url: str, max_attempts=5, backoff=1.0) -> bytes:
    delay = backoff
    for i in range(1, max_attempts+1):
        try:
            logging.info(f"CSV fetch attempt {i}/{max_attempts}")
            r = requests.get(url, timeout=(10, 120))
            if r.status_code >= 500:
                raise RequestException(f"{r.status_code} Server Error")
            r.raise_for_status()
            return r.content
        except (RequestException, ReadTimeout) as e:
            if i == max_attempts:
                logging.error(f"CSV fetch failed after {i} attempts: {e}")
                raise
            logging.warning(f"CSV fetch error ({e}), retrying in {delay:.1f}s…")
            time.sleep(delay); delay *= 2


def fetch_all_values_with_retries(ws, max_attempts=5, backoff=1.0):
    for i in range(1, max_attempts+1):
        try:
            logging.info(f"get_all_values() attempt {i}/{max_attempts}")
            return ws.get_all_values()
        except APIError as e:
            code = getattr(e.response, "status_code", None) or getattr(e.response, "status", None)
            if code and 500 <= int(code) < 600 and i < max_attempts:
                logging.warning(f"get_all_values got {code}, retrying in {backoff:.1f}s")
                time.sleep(backoff); backoff *= 2
                continue
            logging.error(f"get_all_values failed: {e}")
            raise


def fetch_columns(ws, cols_idx, max_attempts=3, backoff=1.0):
    """
    Пытаемся batch_get нужных колонок cols_idx (0-based).
    Если и он всё равно падает, выйдем с APIError и дадим main() обработать.
    """
    backoff_local = backoff
    for i in range(1, max_attempts+1):
        try:
            ranges = []
            for idx in cols_idx:
                a1 = rowcol_to_a1(1, idx+1)
                col = ''.join(filter(str.isalpha, a1))
                ranges.append(f"{col}1:{col}")
            logging.info(f"batch_get ranges {ranges} (attempt {i}/{max_attempts})")
            batch = ws.batch_get(ranges)
            cols = [[r[0] if r else "" for r in colblock] for colblock in batch]
            headers = [c[0] for c in cols]
            data = list(zip(*(c[1:] for c in cols)))
            return pd.DataFrame(data, columns=headers)
        except APIError as e:
            if i < max_attempts:
                logging.warning(f"batch_get got {e}, retrying in {backoff_local:.1f}s")
                time.sleep(backoff_local); backoff_local *= 2
                continue
            logging.error(f"batch_get failed after {i} attempts: {e}")
            raise


def main():
    # 1) Авторизация
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    sa_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(sa_info, scope)
    client = gspread.authorize(creds)
    logging.info("✔ Авторизованы в Google Sheets")

    # 2) Открываем исходный лист
    sh_src = api_retry_open(client, SOURCE_SS_ID)
    ws_src = api_retry_worksheet(sh_src, SOURCE_SHEET_NAME)

    # 3) Пробуем batch_get по колонкам C,D,O,M,F (2,3,14,12,5)
    cols_to_take = [2, 3, 14, 12, 5]
    try:
        df = fetch_columns(ws_src, cols_to_take)
        logging.info(f"→ batch_get succeeded, shape={df.shape}")
    except APIError:
        # 4a) fallback: CSV-экспорт
        logging.warning("batch_get не прошел, пробуем CSV-экспорт…")
        gid = ws_src.id
        token = creds.get_access_token().access_token
        export_url = (
            f"https://docs.google.com/spreadsheets/d/{SOURCE_SS_ID}/export"
            f"?format=csv&gid={gid}&access_token={token}"
        )
        try:
            csv_bytes = fetch_csv_with_retries(export_url)
            df_all = pd.read_csv(io.StringIO(csv_bytes.decode("utf-8")))
            logging.info(f"→ CSV-экспорт удался, shape={df_all.shape}")
        except Exception as e:
            # 4b) последняя надежда: get_all_values()
            logging.warning(f"CSV-экспорт упал ({e}), пробуем get_all_values()…")
            all_vals = fetch_all_values_with_retries(ws_src)
            if not all_vals or len(all_vals) < 2:
                logging.error("Нет данных ни одним способом – выхожу.")
                return
            df_all = pd.DataFrame(all_vals[1:], columns=all_vals[0])
            logging.info(f"→ get_all_values() удался, shape={df_all.shape}")
        # отбираем нужные колонки из df_all
        df = df_all.iloc[:, cols_to_take]
        logging.info(f"→ После fallback-выборки shape={df.shape}")

    # 5) Запись в целевой лист
    sh_dst = api_retry_open(client, DEST_SS_ID)
    ws_dst = api_retry_worksheet(sh_dst, DEST_SHEET_NAME)
    ws_dst.batch_clear(["A:E"])
    set_with_dataframe(ws_dst, df, row=1, col=1,
                       include_index=False, include_column_header=True)
    logging.info(f"✔ Данные записаны в «{DEST_SHEET_NAME}» — {df.shape[0]} строк")


if __name__ == "__main__":
    main()
