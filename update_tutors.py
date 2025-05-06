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
    sa_json = os.environ["GCP_SERVICE_ACCOUNT"]
    creds_info = json.loads(sa_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_info, scope)
    client = gspread.authorize(creds)
    logging.info("✔ Авторизованы в Google Sheets")

    # 2) Формируем URL CSV-экспорта с access_token в запросе
    src_id = os.environ["SOURCE_SS_ID"]
    gid    = os.environ["SOURCE_SHEET_GID"]    # GID листа Tutors
    token  = creds.get_access_token().access_token

    export_url = (
        f"https://docs.google.com/spreadsheets/d/{src_id}/export"
        f"?access_token={token}"
        f"&format=csv"
        f"&gid={gid}"
    )
    logging.info(f"→ Экспорт CSV: {export_url[:80]}…")

    # 3) Делаем простой запрос — токен в URL сохранится на любом хосте
    resp = requests.get(export_url)
    resp.raise_for_status()
    logging.info(f"→ Получено {len(resp.content)} байт CSV")

    # 4) Читаем в pandas и оставляем только колонки A,B,C,E,V
    text = resp.content.decode("utf-8")
    df = pd.read_csv(io.StringIO(text), encoding="utf-8")
    df = df.iloc[:, [0, 1, 2, 4, 21]]  # A=0, B=1, C=2, E=4, V=21
    logging.info(f"→ Оставили колонки A,B,C,E,V — всего {len(df)} строк")

    # 5) Открываем целевую таблицу и лист
    dst_id    = os.environ["DEST_SS_ID"]
    dst_sheet = os.environ.get("DEST_SHEET", "Tutors")
    sh_dst    = client.open_by_key(dst_id)
    ws_dst    = sh_dst.worksheet(dst_sheet)

    # 6) Очищаем и заливаем новый DataFrame
    ws_dst.clear()
    set_with_dataframe(ws_dst, df)
    logging.info(f"✔ Данные записаны в лист '{dst_sheet}' (ID={dst_id}) — {len(df)} строк")

if __name__ == "__main__":
    main()
