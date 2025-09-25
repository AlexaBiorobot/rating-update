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
SOURCE_SHEET_NAME = "Groups & Teachers"

EXTRA_SHEET_NAME  = "Students & Teachers"   # добавлено
# A,B,J => индексы 0,1,9
EXTRA_COLS_IDX    = [0, 1, 9]

DEST_SS_ID        = "16QrbLtzLTV6GqyT8HYwzcwYIsXewzjUbM0Jy5i1fENE"
DEST_SHEET_NAME   = "Groups"
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
                time.sleep(backoff)
                backoff *= 2
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
                time.sleep(backoff)
                backoff *= 2
                continue
            raise
        except WorksheetNotFound:
            logging.error(f"Worksheet '{title}' not found")
            raise


def fetch_columns(ws, cols_idx, max_attempts=5, backoff=1.0):
    """
    Скачиваем только нужные колонки (0-based indices) через batch_get().
    Нормализуем длину колонок (паддинг пустыми строками), чтобы не терять строки.
    """
    for attempt in range(1, max_attempts+1):
        try:
            # строим диапазоны A1:A, B1:B, ...
            ranges = []
            for idx in cols_idx:
                a1 = rowcol_to_a1(1, idx+1)                # "A1", "B1", ...
                col = ''.join(filter(str.isalpha, a1))     # "A", "B", ...
                ranges.append(f"{col}1:{col}")

            batch = ws.batch_get(ranges)

            # список колонок (каждая — список строк); гарантируем хотя бы header + одна пустая строка
            cols = []
            for col in batch:
                if not col:
                    cols.append(["", ""])  # header + одна пустая строка
                else:
                    cols.append([row[0] if row else "" for row in col])

            headers = [c[0] for c in cols]
            # длина данных без заголовка
            max_len = max(len(c) - 1 for c in cols) if cols else 0

            # берём значения (без заголовка) и паддим до max_len
            values = []
            for c in cols:
                body = c[1:]
                if len(body) < max_len:
                    body = body + [""] * (max_len - len(body))
                values.append(body)

            # транспонируем в строки
            data = list(zip(*values)) if values else []
            return pd.DataFrame(data, columns=headers)
        except Exception as e:
            if attempt < max_attempts:
                logging.warning(f"batch_get error (attempt {attempt}): {e} — retrying in {backoff:.1f}s")
                time.sleep(backoff)
                backoff *= 2
                continue
            logging.error(f"batch_get failed after {attempt} attempts: {e}")
            raise


def main():
    # 1) Авторизация
    scope   = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds   = ServiceAccountCredentials.from_json_keyfile_dict(
        json.loads(os.environ["GCP_SERVICE_ACCOUNT"]), scope
    )
    client  = gspread.authorize(creds)
    logging.info("✔ Authenticated to Google Sheets")

    # 2) Открываем исходную таблицу
    sh_src = api_retry_open(client, SOURCE_SS_ID)

    # 3) Лист "Groups & Teachers": тянем только нужные колонки
    ws_src_main = api_retry_worksheet(sh_src, SOURCE_SHEET_NAME)
    cols_main = [0, 1, 10, 3]  # A, B, K, D(возраст)
    df_main = fetch_columns(ws_src_main, cols_main)
    logging.info(f"→ Main fetched {SOURCE_SHEET_NAME} {cols_main}, shape={df_main.shape}")

    # 4) Лист "Students & Teachers": A,B,J и добавляем справа
    ws_src_extra = api_retry_worksheet(sh_src, EXTRA_SHEET_NAME)
    df_extra = fetch_columns(ws_src_extra, EXTRA_COLS_IDX)
    logging.info(f"→ Extra fetched {EXTRA_SHEET_NAME} {EXTRA_COLS_IDX}, shape={df_extra.shape}")

    # Выравниваем по количеству строк (по индексу), недостающее -> ""
    n = max(len(df_main), len(df_extra))
    df_main  = df_main.reindex(range(n))
    df_extra = df_extra.reindex(range(n))
    df = pd.concat([df_main.fillna(""), df_extra.fillna("")], axis=1)
    logging.info(f"→ Combined shape={df.shape}")

    # 5) Запись в целевой лист
    sh_dst = api_retry_open(client, DEST_SS_ID)
    ws_dst = api_retry_worksheet(sh_dst, DEST_SHEET_NAME)
    ws_dst.clear()
    set_with_dataframe(ws_dst, df)
    logging.info(f"✔ Written to '{DEST_SHEET_NAME}' — {df.shape[0]} rows, {df.shape[1]} columns")


if __name__ == "__main__":
    main()
