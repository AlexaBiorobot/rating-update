#!/usr/bin/env python3
import os
import json
import logging
import time

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
from gspread.exceptions import APIError, WorksheetNotFound, SpreadsheetNotFound
from gspread.utils import rowcol_to_a1

# —————————————————————————————
SOURCE_SS_ID      = "1hyK1UPn0bJYx67my12Ytbsh3uThag0v28TvY9T4-81"
SOURCE_SHEET_NAME = "Students&Groups"

DEST_SS_ID        = "1XwyahhHC7uVzwfoErrvwrcruEjwewqIUp2u-6nvdSR0"
DEST_SHEET_NAME   = "0-students"
# —————————————————————————————

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def api_retry_open(client, key, max_attempts=5, backoff=1.0):
    for i in range(1, max_attempts + 1):
        try:
            logging.info(f"open_by_key({key}) attempt {i}/{max_attempts}")
            return client.open_by_key(key)
        except APIError as e:
            code = getattr(e.response, "status_code", None) or getattr(e.response, "status", None)
            if code and 500 <= int(code) < 600 and i < max_attempts:
                logging.warning(f"Received {code} — retrying in {backoff:.1f}s")
                time.sleep(backoff)
                backoff *= 2
                continue
            raise
        except SpreadsheetNotFound:
            # не ретраим 404 — сразу кидаем выше
            raise


def api_retry_worksheet(sh, title, max_attempts=5, backoff=1.0):
    for i in range(1, max_attempts + 1):
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
    Скачиваем только нужные колонки (0-based) через batch_get().
    cols_idx — список индексов, напр. [0,1,2,3,...,9] для A:J.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            ranges = []
            for idx in cols_idx:
                a1 = rowcol_to_a1(1, idx + 1)            # "A1", "B1", ...
                col = ''.join(filter(str.isalpha, a1))   # "A", "B", ...
                ranges.append(f"{col}1:{col}")

            batch = ws.batch_get(ranges)

            # Превращаем batch (список колонок) в одинаковой длины списки строк
            # (если колонка короче — дополним пустыми строками)
            max_len = max((len(col) for col in batch), default=0)
            norm_cols = []
            for col in batch:
                col = col or []
                # каждая "строка" в ответе — это список ячеек одной строки; берём первую ячейку
                flat = [row[0] if row else "" for row in col]
                if len(flat) < max_len:
                    flat += [""] * (max_len - len(flat))
                norm_cols.append(flat)

            if not norm_cols:
                return pd.DataFrame()

            headers = [c[0] for c in norm_cols]
            data = list(zip(*(c[1:] for c in norm_cols)))
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
    # 1) Авторизация (исправлены scopes)
    sa_json = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
    sa_email = sa_json.get("client_email", "unknown-sa@unknown")
    logging.info(f"Service Account: {sa_email}")

    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(sa_json, scope)
    client = gspread.authorize(creds)
    logging.info("✔ Authenticated to Google Sheets")

    # 2) Открываем исходный файл
    try:
        sh_src = api_retry_open(client, SOURCE_SS_ID)
    except SpreadsheetNotFound:
        raise SystemExit(
            f"❌ SpreadsheetNotFound (SOURCE). Проверь ID и дай доступ на {sa_email} (Editor). ID={SOURCE_SS_ID}"
        )
    ws_src = api_retry_worksheet(sh_src, SOURCE_SHEET_NAME)

    # 3) Тянем A..J
    cols_to_take = list(range(0, 10))  # A..J
    df = fetch_columns(ws_src, cols_to_take)
    logging.info(f"→ Fetched columns {cols_to_take}, resulting shape={df.shape}")

    # 4) Открываем целевой файл
    try:
        sh_dst = api_retry_open(client, DEST_SS_ID)
    except SpreadsheetNotFound:
        raise SystemExit(
            f"❌ SpreadsheetNotFound (DEST). Проверь ID и дай доступ на {sa_email} (Editor). ID={DEST_SS_ID}"
        )
    ws_dst = api_retry_worksheet(sh_dst, DEST_SHEET_NAME)

    # 5) Очистка целевой области и запись
    ws_dst.batch_clear(["A:J"])  # чистим A:J, т.к. пишем 10 колонок
    set_with_dataframe(ws_dst, df, row=1, col=1, include_index=False, include_column_header=True)
    logging.info(f"✔ Written to '{DEST_SHEET_NAME}' — {df.shape[0]} rows")


if __name__ == "__main__":
    main()
