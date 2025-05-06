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

    # 2) Собираем export URL (без токена! только format & gid)
    src_id     = os.environ["SOURCE_SS_ID"]
    gid        = os.environ["SOURCE_SHEET_GID"]
    export_url = f"https://docs.google.com/spreadsheets/d/{src_id}/export?format=csv&gid={gid}"
    token      = creds.get_access_token().access_token

    # 3) Делаем запрос и вручную следуем редиректу
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})

    # первый шаг: получить Location
    r = session.get(export_url, allow_redirects=False)
    if r.status_code in (301, 302, 303, 307, 308):
        redirect_url = r.headers["Location"]
        logging.info(f"→ Редирект на: {redirect_url}")
        r = session.get(redirect_url)  # здесь будет сохранён header

    r.raise_for_status()
    logging.info(f"→ CSV экспорт получен: {len(r.content)} байт")

    # 4) Явно декодируем в UTF-8 и парсим
    text = r.content.decode("utf-8")
    df   = pd.read_csv(io.StringIO(text), encoding="utf-8")
    # оставляем только A,B,C,E,V
    df = df.iloc[:, [0, 1, 2, 4, 21]]
    logging.info(f"→ Оставили колонки A,B,C,E,V — {len(df)} строк")

    # 5) Запись в целевой лист
    dst_id    = os.environ["DEST_SS_ID"]
    dst_sheet = os.environ.get("DEST_SHEET", "Tutors")
    sh_dst    = client.open_by_key(dst_id)
    ws_dst    = sh_dst.worksheet(dst_sheet)

    ws_dst.clear()
    set_with_dataframe(ws_dst, df)
    logging.info(f"✔ Данные записаны в лист '{dst_sheet}' — {len(df)} строк")


if __name__ == "__main__":
    main()
