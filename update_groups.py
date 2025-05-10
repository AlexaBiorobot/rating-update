#!/usr/bin/env python3
import os
import json
import logging
import time
import io

import pandas as pd
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
from gspread.exceptions import APIError, WorksheetNotFound
from requests.exceptions import RequestException, ReadTimeout

# —————————————————————————————
SOURCE_SS_ID      = "1xqGCXsebSmYL4bqAwvTmD9lOentI45CTMxhea-ZDFls"
SOURCE_SHEET_NAME = "Tutors"

DEST_SS_ID        = "16QrbLtzLTV6GqyT8HYwzcwYIsXewzjUbM0Jy5i1fENE"
DEST_SHEET_NAME   = "Tutors"
# —————————————————————————————

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def api_retry_open(client, key, max_attempts=5, backoff=1.0):
    for i in range(1, max_attempts+1):
        try:
            logging.info(f"open_by_key attempt {i}/{max_attempts}")
            return client.open_by_key(key)
        except APIError as e:
            code = getattr(e.response, "status_code", None) or getattr(e.response, "status", None)
            if code and 500 <= int(code) < 600 and i < max_attempts:
                logging.warning(f"Received {code} — retrying in {backoff:.1f}s")
                time.sleep(backoff); backoff *= 2
                continue
            raise

def api_retry_worksheet(sh, title, max_attempts=5, backoff=1.0):
    for i in range(1, max_attempts+1):
        try:
            logging.info(f"worksheet('{title}') attempt {i}/{max_attempts}")
            return sh.worksheet(title)
        except APIError as e:
            code = getattr(e.response, "status_code", None) or getattr(e.response, "status", None)
            if code and 500 <= int(code) < 600 and i < max_attempts:
                logging.warning(f"Received {code} — retrying in {backoff:.1f}s")
                time.sleep(backoff); backoff *= 2
                continue
            raise
        except WorksheetNotFound:
            logging.error(f"Worksheet '{title}' not found")
            raise

def fetch_csv_with_retries(url, max_attempts=5, backoff=1.0):
    delay = backoff
    for i in range(1, max_attempts+1):
        try:
            logging.info(f"CSV fetch attempt {i}/{max_attempts}")
            # timeout=(connect, read)
            r = requests.get(url, timeout=(10, 120))
            if r.status_code >= 500:
                raise RequestException(f"{r.status_code} Server Error")
            r.raise_for_status()
            return r.content
        except (RequestException, ReadTimeout) as e:
            if i == max_attempts:
                logging.error(f"CSV fetch failed after {i} attempts: {e}")
                raise
            logging.warning(f"Error fetching CSV ({e}), retrying in {delay:.1f}s...")
            time.sleep(delay); delay *= 2

def main():
    # 1) Auth
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    sa_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(sa_info, scope)
    client = gspread.authorize(creds)
    logging.info("✔ Authenticated")

    # 2) Source sheet + gid
    sh_src = api_retry_open(client, SOURCE_SS_ID)
    ws_src = api_retry_worksheet(sh_src, SOURCE_SHEET_NAME)
    gid = ws_src.id
    logging.info(f"Found sheet '{SOURCE_SHEET_NAME}' (GID={gid})")

    # 3) Try CSV export
    token = creds.get_access_token().access_token
    export_url = (
        f"https://docs.google.com/spreadsheets/d/{SOURCE_SS_ID}/export"
        f"?format=csv&gid={gid}&access_token={token}"
    )

    try:
        csv_bytes = fetch_csv_with_retries(export_url)
        text = csv_bytes.decode("utf-8")
        df_all = pd.read_csv(io.StringIO(text))
        logging.info(f"→ CSV export succeeded, shape={df_all.shape}")
    except Exception:
        logging.warning("CSV export failed — falling back to gspread.get_all_values()")
        all_vals = ws_src.get_all_values()
        if not all_vals or len(all_vals)<2:
            logging.error("No data in sheet!")
            return
        df_all = pd.DataFrame(all_vals[1:], columns=all_vals[0])
        logging.info(f"→ get_all_values() succeeded, shape={df_all.shape}")

        # Если даже get_all_values() очень медленно — можно взять только нужные столбцы:
        # ranges = ['A2:A','B2:B','C2:C','V2:V','E2:E']
        # cols = ws_src.batch_get(ranges)
        # data = list(zip(*[col[1:] for col in cols]))  # убрать заголовки
        # df_all = pd.DataFrame(data, columns=['A','B','C','V','E'])
        # logging.info(f"→ batch_get() выбранных столбцов shape={df_all.shape}")

    # 4) Slice нужные поля (A,B,C,V,E = индексы 0,1,2,21,4)
    df = df_all.iloc[:, [0,1,2,21,4]]
    logging.info(f"→ Selected columns [0,1,2,21,4], shape={df.shape}")

    # 5) Запись в dest
    sh_dst = api_retry_open(client, DEST_SS_ID)
    ws_dst = api_retry_worksheet(sh_dst, DEST_SHEET_NAME)
    ws_dst.clear()
    set_with_dataframe(ws_dst, df)
    logging.info(f"✔ Written to '{DEST_SHEET_NAME}' — {df.shape[0]} rows")

if __name__ == "__main__":
    main()
