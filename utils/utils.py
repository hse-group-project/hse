from sqlalchemy import create_engine


def connection():
    db_params = {
        "dbname": "russian-stocks-prediction-ml-dl",
        "user": "root",
        "password": "groot",
        "host": "185.70.105.233",
        "port": "5432",
    }
    conn_str = f"postgresql+psycopg://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    return create_engine(conn_str)
