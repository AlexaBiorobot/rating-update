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
SOURCE_SS_ID      = "1xqGCXsebSmYL4bqAwvTmD9lOentI45CTMxhea-ZDFls"
SOURCE_SHEET_NAME = "Tutors"

DEST_SS_ID        = "1SudB1YkPD0Tt7xkEiNJypRv0vb62BSdsCLrcrGqALAI"
DEST_SHEET_NAME   = "rates"
# —————————————————————————————

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def api_retry_open(client, key, max_attempts=5, backoff=1.0):
    for i in range(1, max_attempts + 1):
        try:
            return client.open_by_key(key)
        except APIError as e:
            code = getattr(e.response, "status_code", None) or getattr(e.response, "status", None)
            if code and 500 <= int(code) < 600 and i < max_attempts:
                time.sleep(backoff); backoff *= 2
                continue
            raise

def api_retry_worksheet(sh, title, max_attempts=5, backoff=1.0):
    for i in range(1, max_attempts + 1):
        try:
            return sh.worksheet(title)
        except APIError as e:
            code = getattr(e.response, "status_code", None) or getattr(e.response, "status", None)
            if code and 500 <= int(code) < 600 and i < max_attempts:
                time.sleep(backoff); backoff *= 2
                continue
            raise
        except WorksheetNotFound:
            raise

def fetch_columns(ws, cols_idx, max_attempts=5, backoff=1.0):
    """
    batch_get только нужных колонок (по индексам cols_idx).
    cols_idx — список 0-based индексов (например [0,1,22,...])
    """
    for attempt in range(1, max_attempts+1):
        try:
            # Собираем диапазоны вида "A1:A", "B1:B", "W1:W" и т.п.
            ranges = []
            for idx in cols_idx:
                a1 = rowcol_to_a1(1, idx+1)            # "A1", "B1", "W1"... 
                letter = ''.join(filter(str.isalpha, a1))  # "A", "B", "W"...
                ranges.append(f"{letter}1:{letter}")
            batch = ws.batch_get(ranges)
            # Преобразуем: [[['H1'],['r1'],...], ...] → колонки
            cols = []
            for col in batch:
                cols.append([row[0] if row else "" for row in col])
            headers = [col[0] for col in cols]
            data    = list(zip(*(col[1:] for col in cols)))
            return pd.DataFrame(data, columns=headers)
        except Exception as e:
            if attempt < max_attempts:
                time.sleep(backoff); backoff *= 2
                continue
            raise

def main():
    # 1) Авторизация
    scope   = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds   = ServiceAccountCredentials.from_json_keyfile_dict(
                  json.loads(os.environ["GCP_SERVICE_ACCOUNT"]), scope
              )
    client  = gspread.authorize(creds)
    logging.info("✔ Authenticated")

    # 2) Открываем исходный лист
    sh_src = api_retry_open(client, SOURCE_SS_ID)
    ws_src = api_retry_worksheet(sh_src, SOURCE_SHEET_NAME)

    # 3) Тянем только нужные колонки
    cols_to_take = [0, 1, 22, 23, 24, 18]
    df = fetch_columns(ws_src, cols_to_take)
    logging.info(f"→ Fetched columns, shape={df.shape}")

    # 4) Запись в целевой лист
    sh_dst = api_retry_open(client, DEST_SS_ID)
    ws_dst = api_retry_worksheet(sh_dst, DEST_SHEET_NAME)
    ws_dst.clear()
    set_with_dataframe(ws_dst, df)
    logging.info(f"✔ Written to '{DEST_SHEET_NAME}' — {df.shape[0]} rows")

if __name__ == "__main__":
    main()
