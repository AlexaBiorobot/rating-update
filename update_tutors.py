import os, json, logging, io
import pandas as pd
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def main():
    # 1) Авторизация
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    sa_json = os.environ["GCP_SERVICE_ACCOUNT"]
    creds_info = json.loads(sa_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_info, scope)
    client = gspread.authorize(creds)
    logging.info("✔ Авторизовались в Google Sheets")

    # 2) Экспорт CSV
    src_id  = os.environ["SOURCE_SS_ID"]
    gid     = os.environ["SOURCE_SHEET_GID"]
    export_url = f"https://docs.google.com/spreadsheets/d/{src_id}/export?format=csv&gid={gid}"
    token   = creds.get_access_token().access_token
    resp    = requests.get(export_url, headers={"Authorization": f"Bearer {token}"})
    resp.raise_for_status()
    logging.info(f"→ CSV экспорт получен: {len(resp.content)} байт")

    df = pd.read_csv(io.StringIO(resp.text))
    df = df.iloc[:, [0,1,2,4,21]]  # A,B,C,E,V
    logging.info(f"→ Оставили колонки A,B,C,E,V — всего {len(df)} строк")

    # 3) Запись в целевую таблицу
    dst_id    = os.environ["DEST_SS_ID"]
    dst_sheet = os.environ["DEST_SHEET"]
    sh_dst    = client.open_by_key(dst_id)
    ws_dst    = sh_dst.worksheet(dst_sheet)
    ws_dst.clear()
    set_with_dataframe(ws_dst, df)
    logging.info(f"✔ Записали в {dst_sheet} (ID={dst_id}) {len(df)} строк")

if __name__ == "__main__":
    main()
