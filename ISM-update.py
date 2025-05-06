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

# —————————————————————————————
# Константы для этого конкретного переноса

# источник: ID таблицы и GID листа
SRC_SS_ID     = "1MBVdG-_8Bza_H5elN8rSABxSAdBqUtgpsXyS4BcRhV8"
SRC_SHEET_GID = "2063311651"

# куда заливаем: ID таблицы и название листа
DST_SS_ID       = "1SudB1YkPD0Tt7xkEiNJypRv0vb62BSdsCLrcrGqALAI"
DST_SHEET_TITLE = "ism_communications"  # <-- замените на точное название вкладки с GID=1889297831

# единственный секрет
SERVICE_ACCOUNT_JSON = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
# —————————————————————————————

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def fetch_csv_with_retries(url: str, max_attempts: int = 5, backoff_sec: float = 1.0) -> bytes:
    delay = backoff_sec
    for attempt in range(1, max_attempts+1):
        try:
            logging.info(f"CSV fetch attempt {attempt}")
            r = requests.get(url, timeout=30)
            if r.status_code >= 500:
                raise RequestException(f"{r.status_code} Server Error")
            r.raise_for_status()
            return r.content
        except Exception as e:
            if attempt == max_attempts:
                logging.error(f"Failed to fetch CSV after {max_attempts} attempts: {e}")
                raise
            logging.warning(f"Error fetching CSV ({e}), retrying in {delay}s…")
            time.sleep(delay)
            delay *= 2

def main():
    # 1) Авторизуемся
    scope  = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds  = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_JSON, scope)
    client = gspread.authorize(creds)
    logging.info("✔ Authenticated to Google Sheets")

    # 2) Собираем URL экспорта CSV
    token      = creds.get_access_token().access_token
    export_url = (
        f"https://docs.google.com/spreadsheets/d/{SRC_SS_ID}/export"
        f"?format=csv"
        f"&gid={SRC_SHEET_GID}"
        f"&access_token={token}"
    )
    logging.info(f"→ Export URL: {export_url[:80]}…")

    # 3) Скачиваем и парсим CSV
    csv_bytes = fetch_csv_with_retries(export_url)
    logging.info(f"→ Retrieved {len(csv_bytes)} bytes")
    text = csv_bytes.decode("utf-8")
    df_all = pd.read_csv(io.StringIO(text), encoding="utf-8")
    logging.info(f"→ Parsed DataFrame {df_all.shape}")

    # 4) Выбираем колонки C, E, L, AC (индексы 2,4,11,28)
    df = df_all.iloc[:, [2, 4, 11, 28]]
    logging.info(f"→ Selected columns C,E,L,AC → shape {df.shape}")

    # 5) Заливаем в целевой лист
    sh_dst = client.open_by_key(DST_SS_ID)
    ws_dst = sh_dst.worksheet(DST_SHEET_TITLE)
    ws_dst.clear()
    set_with_dataframe(ws_dst, df)
    logging.info(f"✔ Written to '{DST_SHEET_TITLE}' — {df.shape[0]} rows")

if __name__ == "__main__":
    main()
