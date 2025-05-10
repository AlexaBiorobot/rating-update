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
from requests.exceptions import RequestException
from gspread.exceptions import APIError, WorksheetNotFound

# Логирование
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ——— ВАШИ КОНСТАНТЫ ———
SRC_SS_ID        = "1xqGCXsebSmYL4bqAwvTmD9lOentI45CTMxhea-ZDFls"
SRC_SHEET_GID    = "1516956819"

DST_SS_ID        = "1SudB1YkPD0Tt7xkEiNJypRv0vb62BSdsCLrcrGqALAI"
DST_SHEET_TITLE  = "Tutors"
# ——————————————————

SERVICE_ACCOUNT_JSON = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])

def api_retry_open(client, key, max_attempts=5, backoff=1.0):
    for attempt in range(1, max_attempts+1):
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
    for attempt in range(1, max_attempts+1):
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
            raise

def fetch_csv_with_retries(url: str, max_attempts: int = 8, initial_backoff: float = 1.0) -> bytes:
    backoff = initial_backoff
    for attempt in range(1, max_attempts + 1):
        try:
            logging.info(f"CSV fetch attempt {attempt}/{max_attempts}")
            resp = requests.get(url, timeout=(10, 120))
            if resp.status_code >= 500:
                raise RequestException(f"{resp.status_code} Server Error")
            resp.raise_for_status()
            return resp.content
        except RequestException as e:
            if attempt == max_attempts:
                logging.error(f"Failed to fetch CSV after {max_attempts} attempts: {e}")
                raise
            logging.warning(f"Error ({e}), retrying in {backoff:.1f}s…")
            time.sleep(backoff)
            backoff *= 2
    raise RuntimeError("CSV fetch retries exhausted")

def main():
    # 1) Авторизация
    scope  = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds  = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_JSON, scope)
    client = gspread.authorize(creds)
    logging.info("✔ Authenticated to Google Sheets")

    # 2) Формируем URL CSV
    token      = creds.get_access_token().access_token
    export_url = (
        f"https://docs.google.com/spreadsheets/d/{SRC_SS_ID}/export"
        f"?format=csv&gid={SRC_SHEET_GID}&access_token={token}"
    )
    logging.info(f"→ Export CSV URL: {export_url[:80]}…")

    # 3) Скачиваем CSV
    csv_bytes = fetch_csv_with_retries(export_url)
    logging.info(f"→ Downloaded {len(csv_bytes)} bytes")

    # 4) Парсим DataFrame
    df = pd.read_csv(io.StringIO(csv_bytes.decode("utf-8")), encoding="utf-8")
    logging.info(f"→ Original DF shape: {df.shape}")

    # 5) Оставляем колонки A–K
    df = df.iloc[:, :11]
    logging.info(f"→ Kept A–K: {df.shape}")

    # 6) Открываем целевую таблицу и лист с retry
    sh_dst = api_retry_open(client, DST_SS_ID)
    ws_dst = api_retry_worksheet(sh_dst, DST_SHEET_TITLE)

    # 7) Чистим и заливаем
    ws_dst.clear()
    set_with_dataframe(ws_dst, df)
    logging.info(f"✔ Written {df.shape[0]} rows to '{DST_SHEET_TITLE}'")

if __name__ == "__main__":
    main()
