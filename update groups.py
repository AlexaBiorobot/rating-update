#!/usr/bin/env python3
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

# Логирование
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ——— ВАШИ КОНСТАНТЫ ———
# источник CSV
SRC_SS_ID        = "1xqGCXsebSmYL4bqAwvTmD9lOentI45CTMxhea-ZDFls"
SRC_SHEET_GID    = "1516956819"
# куда заливаем
DST_SS_ID        = "1SudB1YkPD0Tt7xkEiNJypRv0vb62BSdsCLrcrGqALAI"
DST_SHEET_TITLE  = "Tutors"   # <-- Замените на точный заголовок листа в целевой таблице

# JSON-сервис-аккаунта (можно оставить в env или тоже захардкодить)
SERVICE_ACCOUNT_JSON = json.loads(
    # либо os.environ["GCP_SERVICE_ACCOUNT"],
    """{
      "type": "service_account",
      … ваш JSON ключ …
    }"""
)
# ——————————————————

def fetch_csv_with_retries(url: str, max_attempts: int = 5, initial_backoff: float = 1.0) -> bytes:
    backoff = initial_backoff
    for attempt in range(1, max_attempts + 1):
        try:
            logging.info(f"Попытка #{attempt} скачивания CSV…")
            resp = requests.get(url, timeout=30)
            if resp.status_code >= 500:
                raise RequestException(f"{resp.status_code} Server Error")
            resp.raise_for_status()
            return resp.content
        except RequestException as e:
            if attempt == max_attempts:
                logging.error(f"Не удалось скачать CSV после {max_attempts} попыток: {e}")
                raise
            logging.warning(f"Ошибка ({e}), ждём {backoff:.1f}s и повторяем")
            time.sleep(backoff)
            backoff *= 2
    raise RuntimeError("Неожиданно не сработал ни один запрос")

def main():
    # 1) Авторизация
    scope  = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds  = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_JSON, scope)
    client = gspread.authorize(creds)
    logging.info("✔ Авторизованы в Google Sheets")

    # 2) Формируем URL CSV
    token      = creds.get_access_token().access_token
    export_url = (
        f"https://docs.google.com/spreadsheets/d/{SRC_SS_ID}/export"
        f"?format=csv&gid={SRC_SHEET_GID}&access_token={token}"
    )
    logging.info(f"→ Экспорт CSV URL: {export_url[:80]}…")

    # 3) Скачиваем CSV
    csv_bytes = fetch_csv_with_retries(export_url)
    logging.info(f"→ Получено {len(csv_bytes)} байт CSV")

    # 4) Парсим DataFrame
    text = csv_bytes.decode("utf-8")
    df   = pd.read_csv(io.StringIO(text), encoding="utf-8")
    logging.info(f"→ Исходный DF: {df.shape}")

    # 5) Оставляем колонки A–K (0–10)
    df = df.iloc[:, list(range(11))]
    logging.info(f"→ Оставили A–K: {df.shape}")

    # 6) Открываем целевую таблицу и лист по названию
    sh_dst  = client.open_by_key(DST_SS_ID)
    ws_dst  = sh_dst.worksheet(DST_SHEET_TITLE)

    # 7) Чистим и заливаем
    ws_dst.clear()
    set_with_dataframe(ws_dst, df)
    logging.info(f"✔ Данные записаны в '{DST_SHEET_TITLE}' — {df.shape[0]} строк")

if __name__ == "__main__":
    main()
