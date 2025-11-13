import pandas as pd
import requests
from datetime import datetime
from sqlalchemy import create_engine, Table, MetaData, insert
import time
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)


def connection():
    db_params = {
        "dbname": "russian-stocks-prediction-ml-dl",
        "user": "root",
        "password": "groot",
        "host": "185.70.105.233",
        "port": "5432",
    }
    conn_str = f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    return create_engine(conn_str)


def get_data(url, name):
    response = requests.get(url)
    response.raise_for_status()

    data = response.json()
    raw_data = data.get("RawData", [])
    header_data = data.get("headerData", [])

    raw_data = pd.DataFrame(raw_data)
    header_data = pd.DataFrame(header_data)

    df_merged = header_data.merge(raw_data, left_on="id", right_on="colId", how="left")
    df_pivot = df_merged.pivot(index="dt", columns="elname", values="obs_val")
    df_pivot = df_pivot.reset_index().rename_axis(None, axis=1)
    month_dict = {
        "Январь": ["01-01", "01.01"],
        "Февраль": ["02-01", "02.01"],
        "Март": ["03-01", "03.01"],
        "Апрель": ["04-01", "04.01"],
        "Май": ["05-01", "05.01"],
        "Июнь": ["06-01", "06.01"],
        "Июль": ["07-01", "07.01"],
        "Август": ["08-01", "08.01"],
        "Сентябрь": ["09-01", "09.01"],
        "Октябрь": ["10-01", "10.01"],
        "Ноябрь": ["11-01", "11.01"],
        "Декабрь": ["12-01", "12.01"],
    }
    try:
        df_pivot["dt"] = df_pivot["dt"].apply(
            lambda x: f"{x.split()[-1]}-{month_dict[x.split()[0]][0]}"
        )
    except:  # noqa: E722
        df_pivot["dt"] = df_pivot["dt"].apply(
            lambda x: f"{x.split('.')[-1]}-{x.split('.')[1]}-{x.split('.')[0]}"
        )
    df_pivot["dt"] = pd.to_datetime(df_pivot["dt"])
    df_pivot.columns = ["dt"] + [
        f"""{name} {" ".join([word.strip("''").strip('""')[:3] for word in col.lower().split()])}"""
        for col in df_pivot.columns[1:]
    ]
    return df_pivot.sort_values(by="dt")


def fetch_last_cbrf_data(cur_year):
    total_df = pd.DataFrame()

    url_1 = f"https://cbr.ru/dataservice/data?y1={cur_year}&y2={cur_year}&publicationId=14&datasetId=27&measureId=2"
    url_2 = f"https://cbr.ru/dataservice/data?y1={cur_year}&y2={cur_year}&publicationId=18&datasetId=37&measureId=2"
    url_3 = f"https://cbr.ru/dataservice/data?y1={cur_year}&y2={cur_year}&publicationId=20&datasetId=41&measureId=22"
    url_4 = f"https://cbr.ru/dataservice/data?y1={cur_year}&y2={cur_year}&publicationId=5&datasetId=8&measureId="
    url_5 = f"https://cbr.ru/dataservice/data?y1={cur_year}&y2={cur_year}&publicationId=33&datasetId=127&measureId="

    url_list = [url_1, url_2, url_3, url_4, url_5]
    names_list = [
        "Ставки по кред",
        "Ставки по вклад",
        "Объем кредит",
        "Широкая д.м.",
        "Курс",
    ]

    for ind in range(len(names_list)):
        d = get_data(url_list[ind], names_list[ind])
        if ind == 0:
            total_df = d
        else:
            total_df = total_df.merge(d, on="dt", how="outer")
    total_df = total_df.rename(columns={"dt": "date"})
    return total_df


def update_db(df):
    orm_columns = cbrf_data.columns.keys()
    df.columns = orm_columns

    records = df.to_dict(orient="records")
    stmt = insert(cbrf_data).values(records)

    set_dict = {col: stmt.excluded[col] for col in df.columns if col != "date"}
    stmt = stmt.on_conflict_do_update(index_elements=["date"], set_=set_dict)

    with connection().begin() as conn:
        conn.execute(stmt)


def main():
    while True:
        cur_year = datetime.now().year
        full_df = fetch_last_cbrf_data(cur_year)
        if len(full_df) > 0:
            update_db(full_df)
            print("Данные собраны и обновлены в БД.")
        else:
            print("Данные за текущий год пока отсутствуют.")
        print("Следующая итерация будет запущена через 14 дней.")
        time.sleep(14 * 24 * 3600)


metadata = MetaData()
cbrf_data = Table("cbrf_data", metadata, autoload_with=connection())

if __name__ == "__main__":
    main()
