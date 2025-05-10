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
from requests.exceptions import RequestException, ReadTimeout

# —————————————————————————————
# Ваши константы
SOURCE_SS_ID      = "1xqGCXsebSmYL4bqAwvTmD9lOentI45CTMxhea-ZDFls"
SOURCE_SHEET_NAME = "Tutors"

DEST_SS_ID        = "1SudB1YkPD0Tt7xkEiNJypRv0vb62BSdsCLrcrGqALAI"
DEST_SHEET_NAME   = "rates"
# —————————————————————————————

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def api_retry_open(client, key, max_attempts=8, backoff=1.0):
    for attempt in range(1, max_attempts + 1):
        try:
            logging.info(f"open_by_key attempt {attempt}/{max_attempts}")
            return client.open_by_key(key)
        except APIError as e:
            code = getattr(e.response, "status_code", None) or getattr(e.response, "status", None)
            if code and 500 <= int(code) < 600 and attempt < max_attempts:
                logging.warning(f"Received {code} — retrying in {backoff:.1f}s")
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
                logging.warning(f"Received {code} — retrying in {backoff:.1f}s")
                time.sleep(backoff)
                backoff *= 2
                continue
            raise
        except WorksheetNotFound:
            logging.error(f"Worksheet '{title}' not found")
            raise


def fetch_csv_with_retries(url: str, max_attempts: int = 8, backoff_sec: float = 1.0) -> bytes:
    """Пытаемся скачать CSV-экспорт с экспоненциальным бэкоффом."""
    delay = backoff_sec
    for attempt in range(1, max_attempts + 1):
        try:
            logging.info(f"CSV fetch attempt {attempt}/{max_attempts}")
            resp = requests.get(url, timeout=(10, 120))
            if resp.status_code >= 500:
                raise RequestException(f"{resp.status_code} Server Error")
            resp.raise_for_status()
            return resp.content
        except (RequestException, ReadTimeout) as e:
            if attempt == max_attempts:
                logging.error(f"CSV fetch failed after {attempt} attempts: {e}")
                raise
            logging.warning(f"Error fetching CSV ({e}) — retrying in {delay:.1f}s")
            time.sleep(delay)
            delay *= 2


def fetch_values_with_retries(ws, max_attempts: int = 5, backoff_sec: float = 1.0):
    """Пытаемся ws.get_all_values() с retry на любой APIError."""
    backoff = backoff_sec
    for attempt in range(1, max_attempts + 1):
        try:
            logging.info(f"get_all_values() attempt {attempt}/{max_attempts}")
            return ws.get_all_values()
        except APIError as e:
            if attempt < max_attempts:
                logging.warning(f"APIError on get_all_values (attempt {attempt}): {e} — retrying in {backoff:.1f}s")
                time.sleep(backoff)
                backoff *= 2
                continue
            logging.error(f"get_all_values() failed after {attempt} attempts: {e}")
            raise
    # не должно дойти сюда
    raise RuntimeError("fetch_values_with_retries exhausted")


def main():
    # 1) Авторизация
    scope   = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    sa_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
    creds   = ServiceAccountCredentials.from_json_keyfile_dict(sa_info, scope)
    client  = gspread.authorize(creds)
    logging.info("✔ Authenticated to Google Sheets")

    # 2) Открываем исходный лист
    sh_src = api_retry_open(client, SOURCE_SS_ID)
    ws_src = api_retry_worksheet(sh_src, SOURCE_SHEET_NAME)

    # 3) Пытаемся экспортировать CSV
    gid       = ws_src.id
    token     = creds.get_access_token().access_token
    export_url = (
        f"https://docs.google.com/spreadsheets/d/{SOURCE_SS_ID}/export"
        f"?format=csv&gid={gid}&access_token={token}"
    )

    try:
        csv_bytes = fetch_csv_with_retries(export_url)
        text      = csv_bytes.decode("utf-8")
        df_all    = pd.read_csv(io.StringIO(text))
        logging.info(f"→ CSV export succeeded, shape={df_all.shape}")
    except Exception:
        logging.warning("CSV export failed — falling back to get_all_values()")
        vals   = fetch_values_with_retries(ws_src)
        if not vals or len(vals) < 2:
            logging.error("Нет данных в исходном листе, выхожу.")
            return
        df_all = pd.DataFrame(vals[1:], columns=vals[0])
        logging.info(f"→ get_all_values() succeeded, shape={df_all.shape}")

    # 4) Отбираем нужные колонки [0,1,22,23,24,18]
    df = df_all.iloc[:, [0, 1, 22, 23, 24, 18]]
    logging.info(f"→ Selected columns [0,1,22,23,24,18], resulting shape={df.shape}")

    # 5) Пишем в целевой лист
    sh_dst = api_retry_open(client, DEST_SS_ID)
    ws_dst = api_retry_worksheet(sh_dst, DEST_SHEET_NAME)
    ws_dst.clear()
    set_with_dataframe(ws_dst, df)
    logging.info(f"✔ Written to '{DEST_SHEET_NAME}' — {df.shape[0]} rows")


if __name__ == "__main__":
    main()
