#!/usr/bin/env python3
import os
import json
import logging
import time

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
from gspread.exceptions import APIError, WorksheetNotFound
from gspread.utils import rowcol_to_a1

# —————————————————————————————
# Ваши константы
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
                logging.warning(f"Received {code} — retrying in {backoff:.1f}s")
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
                logging.warning(f"Received {code} — retrying in {backoff:.1f}s")
                time.sleep(backoff); backoff *= 2
                continue
            raise
        except WorksheetNotFound:
            logging.error(f"Worksheet '{title}' not found")
            raise

def fetch_columns(ws, cols_idx, max_attempts=5, backoff=1.0):
    """
    Скачиваем только нужные колонки (0-based indices) через batch_get().
    cols_idx — список индексов, напр. [2,3,14,12,5]
    """
    for attempt in range(1, max_attempts+1):
        try:
            # строим диапазоны C1:C, D1:D, O1:O, M1:M, F1:F
            ranges = []
            for idx in cols_idx:
                a1   = rowcol_to_a1(1, idx+1)                # "C1", "D1", ...
                col  = ''.join(filter(str.isalpha, a1))     # "C", "D", ...
                ranges.append(f"{col}1:{col}")
            batch = ws.batch_get(ranges)
            # batch → список колонок; каждая колонка — список строк [ [hdr],[val1],... ]
            cols = [[row[0] if row else "" for row in col] for col in batch]
            headers = [c[0] for c in cols]
            data    = list(zip(*(c[1:] for c in cols)))
            return pd.DataFrame(data, columns=headers)
        except Exception as e:
            if attempt < max_attempts:
                logging.warning(f"batch_get error (attempt {attempt}): {e} — retrying in {backoff:.1f}s")
                time.sleep(backoff); backoff *= 2
                continue
            logging.error(f"batch_get failed after {attempt} attempts: {e}")
            raise

def main():
    # 1) Авторизация
    scope   = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds   = ServiceAccountCredentials.from_json_keyfile_dict(
                  json.loads(os.environ["GCP_SERVICE_ACCOUNT"]), scope
              )
    client  = gspread.authorize(creds)
    logging.info("✔ Авторизованы в Google Sheets")

    # 2) Открываем исходный лист
    sh_src = api_retry_open(client, SOURCE_SS_ID)
    ws_src = api_retry_worksheet(sh_src, SOURCE_SHEET_NAME)

    # 3) Получаем только C, D, O, M, F (2,3,14,12,5)
    cols_to_take = [2, 3, 14, 12, 5]
    df = fetch_columns(ws_src, cols_to_take)
    logging.info(f"→ Скачали колонки {cols_to_take}, результат shape={df.shape}")

    # 4) Записываем в целевой лист
    sh_dst = api_retry_open(client, DEST_SS_ID)
    ws_dst = api_retry_worksheet(sh_dst, DEST_SHEET_NAME)

    # очищаем A:E
    ws_dst.batch_clear(["A:E"])
    # вставляем DataFrame с заголовками
    set_with_dataframe(ws_dst, df, row=1, col=1,
                       include_index=False, include_column_header=True)
    logging.info(f"✔ Данные записаны в «{DEST_SHEET_NAME}» — {df.shape[0]} строк")

if __name__ == "__main__":
    main()
