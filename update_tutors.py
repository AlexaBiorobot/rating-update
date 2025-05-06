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

# Логирование в консоль
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def main():
    # 1) Авторизация сервис-аккаунтом
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    sa_json     = os.environ["GCP_SERVICE_ACCOUNT"]
    creds_info  = json.loads(sa_json)
    creds       = ServiceAccountCredentials.from_json_keyfile_dict(creds_info, scope)
    client      = gspread.authorize(creds)
    logging.info("✔ Авторизованы в Google Sheets")

    # 2) Формируем URL CSV-экспорта, включая токен
    src_id = os.environ["SOURCE_SS_ID"]
    gid    = os.environ["SOURCE_SHEET_GID"]
    token  = creds.get_access_token().access_token

    export_url = (
        f"https://docs.google.com/spreadsheets/d/{src_id}/export"
        f"?format=csv"
        f"&gid={gid}"
        f"&access_token={token}"
    )
    logging.info(f"→ Экспорт CSV URL: {export_url[:80]}…")

    # 3) Загружаем CSV одним запросом
    resp = requests.get(export_url)
    resp.raise_for_status()
    logging.info(f"→ Получено {len(resp.content)} байт CSV")

    # 4) Явно декодируем в UTF-8 и парсим DataFrame
    text = resp.content.decode("utf-8")
    df   = pd.read_csv(io.StringIO(text), encoding="utf-8")

    # 5) Оставляем только колонки A,B,C,E,V  (индексы 0,1,2,4,21)
    df = df.iloc[:, [0, 1, 2, 21, 4]]
    logging.info(f"→ Оставили колонки A,B,C,E,V — {len(df)} строк")

    # 6) Открываем целевую таблицу и лист
    dst_id    = os.environ["DEST_SS_ID"]
    dst_sheet = os.environ.get("DEST_SHEET", "Tutors")
    sh_dst    = client.open_by_key(dst_id)
    ws_dst    = sh_dst.worksheet(dst_sheet)

    # 7) Очищаем лист и заливаем обновлённый DataFrame
    ws_dst.clear()
    set_with_dataframe(ws_dst, df)
    logging.info(f"✔ Данные записаны в '{dst_sheet}' ({dst_id}) — {len(df)} строк")


if __name__ == "__main__":
    main()
