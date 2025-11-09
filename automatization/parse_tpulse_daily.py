#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from datetime import datetime, timedelta
import time
import psycopg2
from tpulse import TinkoffPulse

# ----------------- Настройки базы данных -----------------
DB_CONFIG = {
    "dbname": "russian-stocks-prediction-ml-dl",
    "user": "root",
    "password": "groot",
    "host": "185.70.105.233",
    "port": 5432,
}

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
with open("tickers.txt") as f:
    share_list = [line.strip() for line in f.readlines()]

# ----------------- Функции -----------------


def parsing_tpulse_last_2_weeks(ticker, KEYS):
    """Парсим посты Т-пульса за последние 2 недели по тикеру с паузами и повторными попытками"""
    cursor = None
    raw_data = []
    two_weeks_ago = pd.Timestamp.now() - pd.Timedelta(days=14)

    while True:
        for attempt in range(MAX_RETRIES):
            try:
                response = pulse.get_posts_by_ticker(ticker, cursor)
                cursor = response.get("nextCursor")
                posts = response.get("items", [])

                for post in posts:
                    post_date = pd.to_datetime(post["inserted"]).tz_localize(None)
                    if post_date < two_weeks_ago:
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
                    f"[WARN] Ошибка при парсинге {ticker}: {e}, попытка {attempt+1}/{MAX_RETRIES}"
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
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for ticker in df["ticker"].unique():
        cursor.execute(
            f"""
            DELETE FROM {TABLE_NAME}
            WHERE ticker = %s AND inserted >= current_date - interval '14 days'
        """,
            (ticker,),
        )
        conn.commit()

        count = 0
        for _, row in df[df["ticker"] == ticker].iterrows():
            cursor.execute(
                f"""
                INSERT INTO {TABLE_NAME} (id, ticker, inserted, text, commentscount, reactioncount, reactions_counters)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id, ticker) DO UPDATE SET
                    text = EXCLUDED.text,
                    commentscount = EXCLUDED.commentscount,
                    reactioncount = EXCLUDED.reactioncount,
                    reactions_counters = EXCLUDED.reactions_counters
            """,
                (
                    row.id,
                    row.ticker,
                    row.inserted,
                    row.text,
                    row.commentscount,
                    row.reactioncount,
                    str(row.reactions_counters),
                ),
            )
            count += 1
        conn.commit()
        log(f"[INFO] Данные по {ticker} обновлены: {count} постов")

    cursor.close()
    conn.close()


# ----------------- Основной цикл -----------------


def main():
    log("[INFO] Запуск скрипта парсинга Т-пульса")
    all_data = pd.DataFrame()
    for ticker in share_list:
        log(f"[INFO] Парсинг тикера {ticker} ...")
        df = parsing_tpulse_last_2_weeks(ticker, KEYS)
        log(f"[INFO] Найдено {len(df)} постов за последние 2 недели для {ticker}")
        all_data = pd.concat([all_data, df], axis=0)
        time.sleep(SLEEP_BETWEEN_TICKERS)  # пауза между тикерами

    update_posts_table(all_data)
    log("[INFO] Скрипт завершил выполнение.")


if __name__ == "__main__":
    main()


print("Python version:", sys.version)
print("pandas version:", pd.__version__)
print("psycopg2 version:", psycopg2.__version__)

# TinkoffPulse из tpulse не всегда имеет __version__
try:
    print("TinkoffPulse version:", TinkoffPulse.__version__)
except AttributeError:
    print("TinkoffPulse version: (не поддерживается)")