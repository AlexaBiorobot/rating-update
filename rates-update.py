#!/usr/bin/env python3
import os
import json
import logging
import time

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
from gspread.exceptions import APIError

# —————————————————————————————
# Ваши константы
SOURCE_SS_ID      = "1xqGCXsebSmYL4bqAwvTmD9lOentI45CTMxhea-ZDFls"
SOURCE_SHEET_NAME = "Tutors"

DEST_SS_ID        = "1SudB1YkPD0Tt7xkEiNJypRv0vb62BSdsCLrcrGqALAI"
DEST_SHEET_NAME   = "rates"
# —————————————————————————————

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def fetch_with_retries(ws, max_attempts=5, initial_backoff=1.0):
    """Пытаемся ws.get_all_values(), при 503 – ждем и повторяем."""
    backoff = initial_backoff
    for attempt in range(1, max_attempts+1):
        try:
            logging.info(f"Попытка #{attempt} чтения листа…")
            return ws.get_all_values()
        except APIError as e:
            if e.response.status_code == 503 and attempt < max_attempts:
                logging.warning(f"503 Service Unavailable – ждем {backoff:.1f}s и повторяем")
                time.sleep(backoff)
                backoff *= 2
                continue
            else:
                logging.error(f"Ошибка при чтении листа: {e}")
                raise
    raise RuntimeError("Не удалось получить данные после нескольких попыток")

def main():
    # 1) Авторизация
    scope   = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    sa_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
    creds   = ServiceAccountCredentials.from_json_keyfile_dict(sa_info, scope)
    client  = gspread.authorize(creds)
    logging.info("✔ Авторизованы в Google Sheets")

    # 2) Открываем исходный лист и читаем данные с retry
    sh_src = client.open_by_key(SOURCE_SS_ID)
    ws_src = sh_src.worksheet(SOURCE_SHEET_NAME)
    all_vals = fetch_with_retries(ws_src)

    if not all_vals or len(all_vals) < 2:
        logging.error("Исходный лист пуст или нет данных")
        return

    # 3) Собираем DataFrame и выбираем колонки
    df_src = pd.DataFrame(all_vals[1:], columns=all_vals[0])
    df      = df_src.iloc[:, [0,1,22,23,24,18]]
    logging.info(f"→ Оставили A,B,C,V,E: {df.shape[0]} строк")

    # 4) Записываем в целевой лист
    sh_dst = client.open_by_key(DEST_SS_ID)
    ws_dst = sh_dst.worksheet(DEST_SHEET_NAME)
    ws_dst.clear()
    set_with_dataframe(ws_dst, df)
    logging.info(f"✔ Данные записаны в «{DEST_SHEET_NAME}» ({DEST_SS_ID}) — {df.shape[0]} строк")

if __name__ == "__main__":
    main()
