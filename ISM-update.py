#!/usr/bin/env python3
import os
import json
import logging
import time

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe

# —————————————————————————————
# Константы

SRC_SS_ID     = "1MBVdG-_8Bza_H5elN8rSABxSAdBqUtgpsXyS4BcRhV8"
SRC_SHEET_GID = 2063311651  # int

DST_SS_ID       = "1SudB1YkPD0Tt7xkEiNJypRv0vb62BSdsCLrcrGqALAI"
DST_SHEET_TITLE = "ism_communications"

SERVICE_ACCOUNT_JSON = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
# —————————————————————————————

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def get_gspread_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_JSON, scope)
    client = gspread.authorize(creds)
    return client


def get_worksheet_by_gid(spreadsheet, gid: int):
    # В разных версиях gspread метод может называться по-разному
    try:
        return spreadsheet.get_worksheet_by_id(gid)
    except AttributeError:
        for ws in spreadsheet.worksheets():
            if ws.id == gid:
                return ws
        raise ValueError(f"Worksheet with gid={gid} not found")


def read_sheet_as_dataframe(client, ss_id: str, gid: int) -> pd.DataFrame:
    logging.info(f"Opening source spreadsheet: {ss_id}")
    sh = client.open_by_key(ss_id)
    ws = get_worksheet_by_gid(sh, gid)

    logging.info(f"Reading worksheet: '{ws.title}' (gid={gid})")
    values = ws.get_all_values()

    if not values:
        logging.warning("Source sheet is empty")
        return pd.DataFrame()

    # Выравниваем строки по длине
    max_len = max(len(r) for r in values)
    values = [r + [""] * (max_len - len(r)) for r in values]

    header = values[0]
    rows = values[1:]
    df = pd.DataFrame(rows, columns=header)

    logging.info(f"Parsed DataFrame shape: {df.shape}")
    return df


def main():
    client = get_gspread_client()
    logging.info("✔ Authenticated to Google Sheets")

    # ВАЖНО: service account должен иметь доступ к ИСТОЧНИКУ и ЦЕЛЕВОЙ таблице
    logging.info(f"Service account email: {SERVICE_ACCOUNT_JSON.get('client_email')}")

    # 1) Читаем источник
    df_all = read_sheet_as_dataframe(client, SRC_SS_ID, SRC_SHEET_GID)

    if df_all.empty:
        raise ValueError("Source dataframe is empty")

    # 2) Выбираем колонки C, E, L, AC (индексы 2, 4, 11, 28)
    need_idx = [2, 4, 11, 28]
    if df_all.shape[1] <= max(need_idx):
        raise ValueError(
            f"Source has only {df_all.shape[1]} columns, but need index {max(need_idx)} (AC)"
        )

    df = df_all.iloc[:, need_idx].copy()
    logging.info(f"→ Selected columns C,E,L,AC → shape {df.shape}")

    # (опционально) можно переименовать колонки
    # df.columns = ["col_C", "col_E", "col_L", "col_AC"]

    # 3) Записываем в целевой лист
    sh_dst = client.open_by_key(DST_SS_ID)
    ws_dst = sh_dst.worksheet(DST_SHEET_TITLE)

    ws_dst.clear()
    set_with_dataframe(ws_dst, df, include_index=False, include_column_header=True)

    logging.info(f"✔ Written to '{DST_SHEET_TITLE}' — {df.shape[0]} rows")


if __name__ == "__main__":
    main()
