#!/usr/bin/env python3
import os
import json
import logging
import io

import pandas as pd
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe

# —————————————————————————————————————————————————————————————————————————
# Здесь прописываем «жёстко» ID и GID/имена листов

# исходная таблица и GID листа «Tutors»
SOURCE_SS_ID      = "1xqGCXsebSmYL4bqAwvTmD9lOentI45CTMxhea-ZDFls"
SOURCE_SHEET_GID  = 1731969866    # ← именно числовой gid из URL

# целевая таблица и имя листа
DEST_SS_ID        = "1SudB1YkPD0Tt7xkEiNJypRv0vb62BSdsCLrcrGqALAI"
DEST_SHEET_NAME   = "Tutors"

# —————————————————————————————————————————————————————————————————————————

# Логирование
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def main():
    # 1) Авторизация сервис‑аккаунтом
    scope    = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    sa_json  = os.environ["GCP_SERVICE_ACCOUNT"]
    creds_info = json.loads(sa_json)
    creds    = ServiceAccountCredentials.from_json_keyfile_dict(creds_info, scope)
    client   = gspread.authorize(creds)
    logging.info("✔ Авторизованы в Google Sheets")

    # 2) Формируем URL экспортa CSV по прямому запросу
    token     = creds.get_access_token().access_token
    export_url = (
        f"https://docs.google.com/spreadsheets/d/{SOURCE_SS_ID}/export"
        f"?format=csv"
        f"&gid={SOURCE_SHEET_GID}"
        f"&access_token={token}"
    )
    logging.info(f"→ Экспорт CSV URL: {export_url[:80]}…")

    # 3) Скачиваем CSV
    resp = requests.get(export_url)
    resp.raise_for_status()
    logging.info(f"→ Получено {len(resp.content)} байт CSV")

    # 4) Парсим DataFrame
    text = resp.content.decode("utf-8")
    df   = pd.read_csv(io.StringIO(text), encoding="utf-8")

    # 5) Берём только колонки A,B,C,V,E (0,1,2,21,4)
    df = df.iloc[:, [0, 1, 2, 21, 4]]
    logging.info(f"→ Оставили колонки A,B,C,V,E — {len(df)} строк")

    # 6) Открываем целевой лист
    sh_dst = client.open_by_key(DEST_SS_ID)
    ws_dst = sh_dst.worksheet(DEST_SHEET_NAME)

    # 7) Заливаем
    ws_dst.clear()
    set_with_dataframe(ws_dst, df)
    logging.info(f"✔ Данные записаны в лист «{DEST_SHEET_NAME}» ({DEST_SS_ID}) — {len(df)} строк")

if __name__ == "__main__":
    main()
