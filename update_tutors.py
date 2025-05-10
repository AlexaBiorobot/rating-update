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

# —————————————————————————————
# Ваши константы
SOURCE_SS_ID      = "1xqGCXsebSmYL4bqAwvTmD9lOentI45CTMxhea-ZDFls"
SOURCE_SHEET_NAME = "Tutors"

DEST_SS_ID        = "16QrbLtzLTV6GqyT8HYwzcwYIsXewzjUbM0Jy5i1fENE"
DEST_SHEET_NAME   = "Tutors"
# —————————————————————————————

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def api_retry_open(client, key, max_attempts=5, backoff=1.0):
    for attempt in range(1, max_attempts + 1):
        try:
            logging.info(f"open_by_key attempt {attempt}/{max_attempts}")
            return client.open_by_key(key)
        except APIError as e:
            code = getattr(e.response, "status_code", None) or getattr(e.response, "status", None)
            if code and 500 <= int(code) < 600 and attempt < max_attempts:
                logging.warning(f"Received {code} on open_by_key — retrying in {backoff}s")
                time.sleep(backoff)
                backoff *= 2
                continue
            raise


def api_retry_worksheet(sh, title, max_attempts=5, backoff=1.0):
    for attempt in range(1, max_attempts + 1):
        try:
            logging.info(f"worksheet('{title}') attempt {attempt}/{max_attempts}")
            return sh.worksheet(title)
        except APIError as e:
            code = getattr(e.response, "status_code", None) or getattr(e.response, "status", None)
            if code and 500 <= int(code) < 600 and attempt < max_attempts:
                logging.warning(f"Received {code} on worksheet — retrying in {backoff}s")
                time.sleep(backoff)
                backoff *= 2
                continue
            raise
        except WorksheetNotFound:
            logging.error(f"Worksheet '{title}' not found")
            raise


def fetch_with_retries(ws, max_attempts=5, initial_backoff=1.0):
    """
    Пытаемся ws.get_all_values(), при любой APIError — ждем и повторяем до max_attempts.
    """
    backoff = initial_backoff
    for attempt in range(1, max_attempts + 1):
        try:
            logging.info(f"get_all_values() attempt {attempt}/{max_attempts}")
            return ws.get_all_values()
        except APIError as e:
            if attempt < max_attempts:
                logging.warning(
                    f"APIError on get_all_values (attempt {attempt}): {e}. "
                    f"Retrying in {backoff}s…"
                )
                time.sleep(backoff)
                backoff *= 2
                continue
            logging.error(f"get_all_values() failed after {attempt} attempts: {e}")
            raise
    # На практике сюда не дойдём
    raise RuntimeError("fetch_with_retries: retry loop exited unexpectedly")


def main():
    # 1) Авторизация
    scope   = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    sa_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
    creds   = ServiceAccountCredentials.from_json_keyfile_dict(sa_info, scope)
    client  = gspread.authorize(creds)
    logging.info("✔ Authenticated to Google Sheets")

    # 2) Чтение исходной таблицы с retry
    sh_src   = api_retry_open(client, SOURCE_SS_ID)
    ws_src   = api_retry_worksheet(sh_src, SOURCE_SHEET_NAME)
    all_vals = fetch_with_retries(ws_src)

    if not all_vals or len(all_vals) < 2:
        logging.error("Source sheet empty or no data")
        return

    # 3) Формируем DataFrame и выбираем колонки A,B,C,V,E (0,1,2,21,4)
    df_src = pd.DataFrame(all_vals[1:], columns=all_vals[0])
    df      = df_src.iloc[:, [0,1,2,21,4]]
    logging.info(f"→ Kept A,B,C,V,E: {df.shape[0]} rows")

    # 4) Запись в целевой лист с retry
    sh_dst = api_retry_open(client, DEST_SS_ID)
    ws_dst = api_retry_worksheet(sh_dst, DEST_SHEET_NAME)
    ws_dst.clear()
    set_with_dataframe(ws_dst, df)
    logging.info(f"✔ Written to '{DEST_SHEET_NAME}' — {df.shape[0]} rows")


if __name__ == "__main__":
    main()
