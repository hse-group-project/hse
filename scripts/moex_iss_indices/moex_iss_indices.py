import logging
import sys
import pandas as pd
import requests
from datetime import datetime
from sqlalchemy import Table, MetaData
from requests.exceptions import ConnectTimeout
import time
import warnings


from utils.utils import connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

warnings.simplefilter(action="ignore", category=FutureWarning)


def fetch_today_index_data(index_code, today_date):
    today_date_str = today_date.strftime("%Y%m%d")
    url = f"https://iss.moex.com/iss/history/engines/stock/markets/index/securities/{index_code.lower()}.json?from={today_date_str}&till={today_date_str}"
    try:
        response = requests.get(url, timeout=10)
    except ConnectTimeout:
        time.sleep(60)
        response = requests.get(url, timeout=10)
    data = response.json()
    history = data.get("history")
    if history is None:
        return pd.DataFrame()
    columns = history["columns"]
    rows = history["data"]
    df = pd.DataFrame(rows, columns=columns)
    df_filtered = df[["TRADEDATE", "OPEN", "CLOSE"]].copy()
    df_filtered["index_code"] = index_code.upper()
    df_filtered.rename(
        columns={"TRADEDATE": "date", "OPEN": "open", "CLOSE": "close"}, inplace=True
    )
    return df_filtered


def update_db(df):
    records = df.to_dict(orient="records")

    with connection().begin() as conn:
        for record in records:
            # Check if record exists
            existing = conn.execute(
                indices_prices.select().where(
                    (indices_prices.c.date == record["date"])
                    & (indices_prices.c.index_code == record["index_code"])
                )
            ).first()

            if existing:
                # Update existing record
                update_stmt = (
                    indices_prices.update()
                    .where(
                        (indices_prices.c.date == record["date"])
                        & (indices_prices.c.index_code == record["index_code"])
                    )
                    .values(open=record["open"], close=record["close"])
                )
                conn.execute(update_stmt)
            else:
                # Insert new record
                insert_stmt = indices_prices.insert().values(**record)
                conn.execute(insert_stmt)


def main():
    while True:
        all_data = []
        for idx in moex_indices_codes:
            df = fetch_today_index_data(idx, datetime.today())
            if not df.empty:
                df["index_code"] = idx
                all_data.append(df)
        if all_data:
            full_df = pd.concat(all_data, ignore_index=True)
            update_db(full_df)
            logger.info("Данные за сегодня успешно собраны и обновлены в БД.")
        else:
            logger.info("Данные за сегодня пока отсутствуют.")
        logger.info("Ждем 3 часа до следующей итерации.")
        time.sleep(3 * 3600)


moex_indices_codes = [
    "IMOEX",  # Индекс МосБиржи
    "IMOEX2",  # Индекс МосБиржи с дополнительными сессиями
    "RTSI",  # Индекс РТС
    "IMOEXCNY",  # Индекс МосБиржи в юанях
    "IMOEXW",  # Индекс МосБиржи – активное управление
    "MOEXBC",  # Индекс МосБиржи голубых фишек
    "MRBC",  # (Вероятно, связанный с голубыми фишками)
    "MOEXBMI",  # Индекс МосБиржи широкого рынка
    "RUBMI",  # Индекс МосБиржи в рублях (возможно малый индекс)
    "MCXSM",  # Индекс МосБиржи средней и малой капитализации
    "RTSSM",  # Индекс РТС средней и малой капитализации
    "MOEXOG",  # Нефть и газ
    "RTSOG",  # РТС нефть и газ
    "MOEXEU",  # Электроэнергетики
    "RTSEU",  # РТС электроэнергетики
    "MOEXTL",  # Телекоммуникации
    "RTSTL",  # РТС телекоммуникации
    "MOEXMM",  # Металлы и добыча
    "RTSMM",  # РТС металлы и добыча
    "MOEXFN",  # Финансовый сектор
    "RTSFN",  # РТС финансовый сектор
    "MOEXCN",  # Потребительский сектор
    "RTSCR",  # РТС потребительский сектор
    "MOEXCH",  # Химия и нефтехимия
    "RTSCH",  # РТС химия и нефтехимия
    "MOEXIT",  # Информационные технологии МосБиржи
    "RTSIT",  # Информационные технологии РТС
    "MOEXRE",  # Недвижимость МосБиржи
    "RTSRE",  # Недвижимость РТС
    "MOEXTN",  # Транспорт МосБиржи
    "RTSTN",  # Транспорт РТС
    "MOEX10",  # Индекс МосБиржи 10 (ТОП-10 компаний)
    "MOEXINN",  # Индекс МосБиржи инноваций
    "MIPO",  # Индекс МосБиржи IPO
    "MXSHAR",  # Индекс МосБиржи исламских инвестиций
    "MESG",  # Индекс МосБиржи-RAEX ESG сбалансированный
    "MRRT",  # Индекс МосБиржи - РСПП Ответственность и открытость
    "MRSV",  # Индекс МосБиржи - РСПП Вектор устойчивого развития
    "MRSVR",  # Индекс МосБиржи - РСПП Вектор устойчивого развития российских эмитентов
    "RGBITR",  # Индекс государственных облигаций
    "RUCBTRNS",  # Индекс корпоративных облигаций
    "RUMBTRNS",  # Индекс муниципальных облигаций
    "RUABITR",  # Композитный индекс облигаций
    "RGBILP",  # Новый индекс ОФЗ (облигаций федерального займа), с 2025 года
    "MOEXALLW",  # Индекс МосБиржи Всепогодный
    "MXTDFI2030",  # Индекс МосБиржи с целевой датой 2030
    "MXTDFI2035",  # Индекс МосБиржи с целевой датой 2035
    "MXTDFI2040",  # Индекс МосБиржи с целевой датой 2040
    "MXTDFI2045",  # Индекс МосБиржи с целевой датой 2045
    "RUPCI",  # Индекс активов пенсионных накоплений - Консервативный
    "RUPMI",  # Индекс активов пенсионных накоплений - Сбалансированный
    "RUPAI",  # Индекс активов пенсионных накоплений - Агрессивный
    "BPSI",  # Субиндекс облигаций активов пенсионных накоплений
    "BPSIG",  # Субиндекс облигаций ОФЗ активов пенсионных накоплений
    "EPSITR",  # Субиндекс акций полной доходности
    "RUGOLD",  # Индекс Золота
    "MREF",  # Индекс МосБиржи фондов недвижимости
    "MREFTR",  # Индекс МосБиржи фондов недвижимости (полная доходность)
    "RUSFAR",
    "RUSFAR1W",
    "RUSFAR2W",
    "RUSFAR1M",
    "RUSFAR3M",
    "RUSFARCNY",
    "RUSFARCN1W",
    "RUSFARRT",
    "RUSFAR1WRT",
    "RUSFAR2WRT",
    "RUSFAR1MRT",
    "RUSFAR3MRT",
    "RUSFARCNRT",
    "RUSFARC1WR",
]

metadata = MetaData()
indices_prices = Table("moex_iss_indices", metadata, autoload_with=connection())

if __name__ == "__main__":
    main()
