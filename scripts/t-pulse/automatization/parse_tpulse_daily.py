#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from datetime import datetime
import time
from sqlalchemy import text
from tpulse import TinkoffPulse


from utils.utils import connection


TABLE_NAME = "t_pulse_data"

# ----------------- Настройки парсинга -----------------
KEYS = ["id", "inserted", "commentsCount"]
pulse = TinkoffPulse()
MAX_RETRIES = 3
SLEEP_BETWEEN_PAGES = 0.5  # пауза между страницами
SLEEP_BETWEEN_TICKERS = 2  # пауза между тикерами
SLEEP_ON_ERROR = 5  # пауза перед повторной попыткой

# ----------------- Настройки логирования -----------------
LOG_FILE = "parse_tpulse.log"


def log(message):
    """Записываем сообщение с датой и временем в лог"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


# ----------------- Список тикеров -----------------

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
    "AKRN",
    "LSRG",
    "RTKMP",
    "SBERP",
    "SNGSP",
    "TATNP",
    "TTLK",
]


# ----------------- Функции -----------------


def parsing_tpulse_last_twentyeight_days(ticker, KEYS):
    """Парсим посты Т-пульса за последние 2 недели по тикеру с паузами и повторными попытками"""
    cursor = None
    raw_data = []
    twentyeight_days_ago = pd.Timestamp.now() - pd.Timedelta(days=28)

    while True:
        for attempt in range(MAX_RETRIES):
            try:
                response = pulse.get_posts_by_ticker(ticker, cursor)
                cursor = response.get("nextCursor")
                posts = response.get("items", [])

                for post in posts:
                    post_date = pd.to_datetime(post["inserted"]).tz_localize(None)
                    if post_date < twentyeight_days_ago:
                        return pd.DataFrame(raw_data)

                    data = {key: post[key] for key in KEYS}
                    data["ticker"] = ticker
                    data["text"] = post["content"]["text"]
                    data["reactioncount"] = post["reactions"]["totalCount"]
                    data["reactions_counters"] = post["reactions"]["counters"]
                    data["commentscount"] = data.pop(
                        "commentsCount"
                    )  # переименование для базы
                    raw_data.append(data)

                time.sleep(SLEEP_BETWEEN_PAGES)  # пауза между страницами
                break  # успешный запрос
            except Exception as e:
                log(
                    f"[WARN] Ошибка при парсинге {ticker}: {e}, попытка {attempt + 1}/{MAX_RETRIES}"
                )
                time.sleep(SLEEP_ON_ERROR)
        else:
            log(
                f"[ERROR] Не удалось получить данные по {ticker} после {MAX_RETRIES} попыток"
            )
            break

        if not cursor:
            break

    df = pd.DataFrame(raw_data)
    if not df.empty:
        df["inserted"] = pd.to_datetime(df["inserted"]).dt.date
    return df


def update_posts_table(df):
    """Обновляем таблицу в PostgreSQL"""
    if df.empty:
        log("[INFO] Нет новых данных для обновления.")
        return

    conn = connection()

    with conn.begin():  # автоматическое управление транзакциями
        for ticker in df["ticker"].unique():
            # Удаляем старые данные
            conn.execute(
                text(f"""
                    DELETE FROM {TABLE_NAME}
                    WHERE ticker = :ticker AND inserted >= current_date - interval '28 days'
                """),
                {"ticker": ticker},
            )

            # Вставляем новые данные
            ticker_data = df[df["ticker"] == ticker]
            count = 0

            for _, row in ticker_data.iterrows():
                conn.execute(
                    text(f"""
                        INSERT INTO {TABLE_NAME} (id, ticker, inserted, text, commentscount, reactioncount, reactions_counters)
                        VALUES (:id, :ticker, :inserted, :text, :commentscount, :reactioncount, :reactions_counters)
                        ON CONFLICT (id, ticker) DO UPDATE SET
                            text = EXCLUDED.text,
                            commentscount = EXCLUDED.commentscount,
                            reactioncount = EXCLUDED.reactioncount,
                            reactions_counters = EXCLUDED.reactions_counters
                    """),
                    {
                        "id": row.id,
                        "ticker": row.ticker,
                        "inserted": row.inserted,
                        "text": row.text,
                        "commentscount": row.commentscount,
                        "reactioncount": row.reactioncount,
                        "reactions_counters": str(row.reactions_counters),
                    },
                )
                count += 1

            log(f"[INFO] Данные по {ticker} обновлены: {count} постов")

    # Соединение автоматически закрывается при выходе из with блока


# ----------------- Основной цикл -----------------


def main():
    while True:
        log("[INFO] Запуск скрипта парсинга Т-пульса")
        all_data = pd.DataFrame()
        for ticker in tickers:
            log(f"[INFO] Парсинг тикера {ticker} ...")
            df = parsing_tpulse_last_twentyeight_days(ticker, KEYS)
            log(f"[INFO] Найдено {len(df)} постов за последние 2 недели для {ticker}")
            all_data = pd.concat([all_data, df], axis=0)
            time.sleep(SLEEP_BETWEEN_TICKERS)  # пауза между тикерами

        update_posts_table(all_data)
        log("[INFO] Скрипт завершил выполнение. Ждем 1 день до следующей итерации.")
        time.sleep(24 * 60 * 60)


if __name__ == "__main__":
    main()
