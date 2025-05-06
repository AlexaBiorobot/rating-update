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
# Константы
SRC_SS_ID       = "1XwyahhHC7uVzwfoErrvwrcruEjwewqIUp2u-6nvdSR0"
SRC_SHEET_TITLE = "data"

DST_SS_ID       = "1SudB1YkPD0Tt7xkEiNJypRv0vb62BSdsCLrcrGqALAI"
DST_SHEET_TITLE = "Students_in_groups"
# —————————————————————————————

# Логирование
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def fetch_with_retries(ws, attempts=5, backoff=1.0):
    """Чтение get_all_values с retry при APIError(503)."""
    for i in range(1, attempts+1):
        try:
            logging.info(f"Чтение исходных данных, попытка {i}/{attempts}")
            return ws.get_all_values()
        except APIError as e:
            if i == attempts or e.response.status_code != 503:
                logging.error(f"Ошибка при чтении листа: {e}")
                raise
            logging.warning(f"503 — ждем {backoff}s")
            time.sleep(backoff)
            backoff *= 2
    raise RuntimeError("Не удалось прочитать лист после retry")

def main():
    # 1) Авторизация
    sa_json = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        sa_json,
        ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    )
    client = gspread.authorize(creds)
    logging.info("✔ Авторизованы в Google Sheets")

    # 2) Читаем исходный лист
    sh_src = client.open_by_key(SRC_SS_ID)
    ws_src = sh_src.worksheet(SRC_SHEET_TITLE)
    data = fetch_with_retries(ws_src)
    if not data or len(data) < 2:
        logging.error("Исходный лист пуст или нет строк")
        return

    # 3) Формируем DataFrame и фильтруем по B содержит COL|CHI|ESP
    header = data[0]
    df_all = pd.DataFrame(data[1:], columns=header)
    df = df_all.iloc[:, [1, 13]]
    logging.info(f"→ Отобрано {len(df)} строк с колонками B и N")
    
    # 4) Пишем в целевой лист
    sh_dst = client.open_by_key(DST_SS_ID)
    ws_dst = sh_dst.worksheet(DST_SHEET_TITLE)
    ws_dst.clear()
    set_with_dataframe(ws_dst, df)
    logging.info(f"✔ Записано в '{DST_SHEET_TITLE}': {len(df)} строк")

if __name__ == "__main__":
    main()
