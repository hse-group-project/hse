import logging
import sys
import requests
import time
import pandas as pd
from datetime import datetime
from requests.exceptions import ConnectTimeout
import warnings

from utils.utils import connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

warnings.simplefilter(action="ignore", category=FutureWarning)


def fetch_index_data(ticker):
    url = f"https://iss.moex.com/iss/securities/{ticker.lower()}/dividends.json"
    try:
        response = requests.get(url, timeout=10)
    except ConnectTimeout:
        time.sleep(60)
        response = requests.get(url, timeout=10)
    columns = list(response.json()["dividends"]["metadata"].keys())
    rows = response.json()["dividends"]["data"]
    df = pd.DataFrame(rows, columns=columns)
    df_filtered = df[["secid", "registryclosedate", "value"]].copy()
    df_filtered["secid"] = ticker.upper()
    df_filtered.rename(
        columns={
            "registryclosedate": "date",
            "secid": "ticker",
            "value": "dividedends_rub_per_share",
        },
        inplace=True,
    )
    df_filtered["updated_date"] = datetime.today().date()
    return df_filtered


def main():
    while True:
        all_data = []
        for ticker in tickers:
            df = fetch_index_data(ticker)
            if not df.empty:
                all_data.append(df)
        if all_data:
            full_df = pd.concat(all_data, ignore_index=True)
            full_df.to_sql(
                "moex_iss_dividends", con=connection(), if_exists="replace", index=False
            )
            logger.info(
                f"Данные  успешно собраны и обновлены в БД {datetime.today().date()}."
            )
        else:
            logger.info("Данные за сегодня пока отсутствуют.")
        logger.info("Ждем 30 дней до следующей итерации.")
        time.sleep(30 * 24 * 60 * 60)


tickers = [
    "SBER",
    "GAZP",
    "T",
    "X5",
    "PIKK",
    "PLZL",
    "LKOH",
    "GMKN",
    "VTBR",
    "NVTK",
    "YDEX",
    "TATN",
    "SMLT",
    "SPBE",
    "ROSN",
    "AFLT",
    "IRAO",
    "VKCO",
    "CHMF",
    "NLMK",
    "MAGN",
    "MGNT",
    "MTLR",
    "AFKS",
    "MOEX",
    "POSI",
    "ALRS",
    "BELU",
    "MTSS",
    "SVCB",
    "SNGS",
    "HEAD",
    "CNRU",
    "PHOR",
    "RTKM",
    "SIBN",
    "UGLD",
    "RUAL",
    "HYDR",
    "BSPB",
    "FEES",
    "EUTR",
    "RNFT",
    "MRKC",
    "MDMG",
    "FLOT",
    "UPRO",
    "ASTR",
    "IVAT",
    "LENT",
    "WUSH",
    "UWGN",
    "ENPG",
    "TRMK",
    "MVID",
    "RASP",
    "RAGR",
    "MRKP",
    "SVAV",
    "AQUA",
    "RENI",
    "SOFL",
    "SGZH",
    "SFIN",
    "OZPH",
    "NMTP",
    "FIXR",
    "OGKB",
    "SELG",
    "LEAS",
    "CBOM",
    "VSMO",
    "IRKT",
    "VSEH",
]

main()
