import os
import json
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
import logging

# — настраиваем логирование в консоль
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def main():
    # 1) Авторизация
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    sa_json = os.environ["GCP_SERVICE_ACCOUNT"]
    creds_info = json.loads(sa_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_info, scope)
    client = gspread.authorize(creds)
    logging.info("✔ Авторизовались в Google Sheets")

    # 2) Скачиваем исходную таблицу и лист
    src_id    = os.environ["SOURCE_SS_ID"]
    src_sheet = os.environ.get("SOURCE_SHEET", "Tutors")
    sh_src    = client.open_by_key(src_id)
    ws_src    = sh_src.worksheet(src_sheet)
    all_vals  = ws_src.get_all_values()
    logging.info(f"→ Открыли исходный лист '{src_sheet}' (ID={src_id}), строк: {len(all_vals)-1}")

    # 3) Превращаем в DataFrame и выбираем столбцы A,B,C,E,V
    header = all_vals[0]
    data   = all_vals[1:]
    df     = pd.DataFrame(data, columns=header)
    # номера столбцов: A→0, B→1, C→2, E→4, V→21
    cols   = [header[i] for i in (0,1,2,4,21)]
    df     = df[cols]
    logging.info(f"→ Выбрали колонки: {cols}")

    # 4) Записываем в целевую таблицу
    dst_id    = os.environ["DEST_SS_ID"]
    dst_sheet = os.environ.get("DEST_SHEET", "Tutors")
    sh_dst    = client.open_by_key(dst_id)
    ws_dst    = sh_dst.worksheet(dst_sheet)
    ws_dst.clear()  # очищаем прежние данные
    set_with_dataframe(ws_dst, df)
    logging.info(f"✔ Перенесли данные в лист '{dst_sheet}' (ID={dst_id}) — строк: {len(df)}")

if __name__ == "__main__":
    main()
