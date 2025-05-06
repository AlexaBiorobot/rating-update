#!/usr/bin/env python3
import os
import json
import logging

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe

# —————————————————————————————————————————————————————————————————————————
# Жёстко прописываем ID таблиц и имя листа (больше не нужны SOURCE_SHEET_GID и HTTP‑token)
SOURCE_SS_ID     = "1xqGCXsebSmYL4bqAwvTmD9lOentI45CTMxhea-ZDFls"
SOURCE_SHEET_NAME= "Tutors"   # точное имя листа

DEST_SS_ID       = "1SudB1YkPD0Tt7xkEiNJypRv0vb62BSdsCLrcrGqALAI"
DEST_SHEET_NAME  = "Tutors"
# —————————————————————————————————————————————————————————————————————————

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def main():
    # 1) Авторизация
    scope    = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    sa_info  = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
    creds    = ServiceAccountCredentials.from_json_keyfile_dict(sa_info, scope)
    client   = gspread.authorize(creds)
    logging.info("✔ Авторизованы в Google Sheets")

    # 2) Читаем исходный лист целиком
    sh_src = client.open_by_key(SOURCE_SS_ID)
    ws_src = sh_src.worksheet(SOURCE_SHEET_NAME)
    all_vals = ws_src.get_all_values()
    if not all_vals:
        logging.error("Исходный лист пуст")
        return

    # 3) Формируем DataFrame, берём только столбцы A,B,C,V,E (0,1,2,21,4)
    df_src = pd.DataFrame(all_vals[1:], columns=all_vals[0])
    df = df_src.iloc[:, [0, 1, 2, 21, 4]]
    logging.info(f"→ Выбрали колонки A,B,C,V,E: {df.shape[0]} строк")

    # 4) Записываем в целевой лист
    sh_dst = client.open_by_key(DEST_SS_ID)
    ws_dst = sh_dst.worksheet(DEST_SHEET_NAME)
    ws_dst.clear()
    set_with_dataframe(ws_dst, df)
    logging.info(f"✔ Данные записаны в лист «{DEST_SHEET_NAME}» ({DEST_SS_ID}) — {df.shape[0]} строк")

if __name__ == "__main__":
    main()
