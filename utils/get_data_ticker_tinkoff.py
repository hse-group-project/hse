import pandas as pd
from stockstats import StockDataFrame


def data_from_ticker(
    ticker: str, left_date: str, right_date: str, conn
) -> pd.DataFrame:
    shares = pd.read_sql(
        f"""
        SELECT * FROM candles
        WHERE ticker = '{ticker}'
        AND datetime >= '{left_date}' AND datetime <= '{right_date}'
        ORDER BY datetime DESC
    """,
        conn,
    )

    shares = shares.drop_duplicates()

    shares["prev_close"] = shares["close"].shift(1)
    shares["open_to_prev_close"] = shares["open"] / shares["prev_close"] - 1
    shares["target"] = shares["close"]
    shares["delta_open_close"] = shares["open"] - shares["close"]
    shares["delta_open_close_pct"] = (shares["open"] - shares["close"]) / shares[
        "close"
    ]

    stock = StockDataFrame.retype(shares)

    shares["macd"] = stock["macd"]
    shares["rsi_14"] = stock["rsi_14"]
    shares["boll_ub"] = stock["boll_ub"]
    shares["boll_lb"] = stock["boll_lb"]

    shares = shares.drop(columns=["open", "prev_close", "close"])

    for col in shares.columns:
        shares[col] = shares[col].ffill()

    shares = shares.dropna().reset_index(drop=True)

    return shares
