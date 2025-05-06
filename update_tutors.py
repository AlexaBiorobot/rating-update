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

# — Логирование в консоль —
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def main():
    # 1) Авторизация сервис-аккаунтом
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    sa_json = os.environ["GCP_SERVICE_ACCOUNT"]
    creds_info = json.loads(sa_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_info, scope)
    client = gspread.authorize(creds)
    logging.info("✔ Авторизовались в Google Sheets")

    # 2) Собираем URL для CSV-экспорта
    src_id     = os.environ["SOURCE_SS_ID"]
    gid        = os.environ["SOURCE_SHEET_GID"]
    export_url = f"https://docs.google.com/spreadsheets/d/{src_id}/export?format=csv&gid={gid}"
    token      = creds.get_access_token().access_token

    # 3) Делаем первый запрос без редиректа
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})

    r = session.get(export_url, allow_redirects=False)
    if r.status_code in (301, 302, 303, 307, 308):
        redirect_url = r.headers.get("Location")
        logging.info(f"→ Редирект на {redirect_url}")
        r = session.get(redirect_url)

    r.raise_for_status()
    logging.info(f"→ CSV экспорт получен: {len(r.content)} байт")

    # 4) Читаем CSV в pandas и фильтруем колонки A,B,C,E,V
    df = pd.read_csv(io.StringIO(r.text))
    # индексы: A=0, B=1, C=2, E=4, V=21
    df = df.iloc[:, [0, 1, 2, 4, 21]]
    logging.info(f"→ Оставили колонки A,B,C,E,V — {len(df)} строк")

    # 5) Открываем целевую таблицу и лист
    dst_id    = os.environ["DEST_SS_ID"]
    dst_sheet = os.environ.get("DEST_SHEET", "Tutors")
    sh_dst    = client.open_by_key(dst_id)
    ws_dst    = sh_dst.worksheet(dst_sheet)

    # 6) Очищаем и заливаем DataFrame
    ws_dst.clear()
    set_with_dataframe(ws_dst, df)
    logging.info(f"✔ Перенесли данные в лист '{dst_sheet}' (ID={dst_id}): {len(df)} строк")


if __name__ == "__main__":
    main()
