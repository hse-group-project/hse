import logging
import sys
import pandas as pd
from datetime import datetime, timedelta
import time
import os
import warnings
from tinkoff.invest import Client, CandleInterval
from utils.utils import connection

warnings.simplefilter(action="ignore", category=FutureWarning)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

TINKOFF_TOKEN = os.getenv("TINKOFF_TOKEN")


def normalize_datetime(dt):
    """Приводит datetime к наивному формату (без часового пояса)"""
    if hasattr(dt, "tzinfo") and dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt


def update_stock_data():
    """Упрощенная функция обновления данных"""
    engine = connection()

    # Получаем все тикеры и FIGI одним запросом
    with engine.connect() as conn:
        tickers_df = pd.read_sql(
            """
            SELECT c.ticker, c.figi, MAX(candles.datetime) as last_date 
            FROM companies c 
            LEFT JOIN candles ON c.ticker = candles.ticker 
            GROUP BY c.ticker, c.figi
        """,
            conn,
        )

    logging.info(f"🔄 Обработка {len(tickers_df)} тикеров...")

    total_added = 0

    for _, row in tickers_df.iterrows():
        ticker = row["ticker"]
        figi = row["figi"]
        last_date = row["last_date"]

        try:
            if pd.isna(last_date):
                logging.info(f"⏩ Пропускаем {ticker}: нет исторических данных")
                continue

            # Нормализуем дату (убираем часовой пояс если есть)
            last_date = normalize_datetime(last_date)

            # Получаем новые данные
            start_time = last_date + timedelta(days=1)
            end_time = datetime.utcnow()

            if start_time > end_time:
                continue

            with Client(TINKOFF_TOKEN) as client:
                candles = client.get_all_candles(
                    figi=figi,
                    from_=start_time,
                    to=end_time,
                    interval=CandleInterval.CANDLE_INTERVAL_DAY,
                )

                new_data = []
                for candle in candles:
                    candle_time = normalize_datetime(candle.time)

                    if candle_time > last_date:
                        new_data.append(
                            {
                                "ticker": ticker,
                                "datetime": candle_time,
                                "open": candle.open.units + candle.open.nano / 1e9,
                                "high": candle.high.units + candle.high.nano / 1e9,
                                "low": candle.low.units + candle.low.nano / 1e9,
                                "close": candle.close.units + candle.close.nano / 1e9,
                                "volume": candle.volume,
                                "is_complete": candle.is_complete,
                            }
                        )

                if new_data:
                    df = pd.DataFrame(new_data)
                    # Простое сохранение через pandas
                    df.to_sql("candles", engine, if_exists="append", index=False)
                    total_added += len(new_data)
                    logging.info(f"✅ {ticker}: +{len(new_data)} свечей")
                else:
                    logging.info(f"📭 {ticker}: нет новых данных")

            time.sleep(0.5)

        except Exception as e:
            logging.error(f"❌ {ticker}: {e}")

    logging.info(f"🎉 Добавлено {total_added} новых свечей")
    return total_added


def main():
    """Основной цикл"""
    logging.info("🚀 Сервис обновления данных запущен")

    while True:
        try:
            logging.info(f"\n=== {datetime.now().replace(microsecond=0)} ===")
            update_stock_data()
            logging.info("💤 Ожидание 24 часа...")
            time.sleep(24 * 3600)
        except KeyboardInterrupt:
            logging.error("\n🛑 Остановлено")
            break
        except Exception as e:
            logging.error(f"🔥 Ошибка: {e}")
            time.sleep(300)


if __name__ == "__main__":
    main()
