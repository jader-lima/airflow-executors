import yfinance
from airflow.decorators import dag, task
from airflow.macros import ds_add
from pathlib import Path
from time import sleep
import pendulum

TICKERS = [
    "BTC",
    "ETH",
    "DOGE",
    "AVAX"
]

@task()
def get_history(ticker, ds=None, ds_nodash=None):
    file_path = f"/opt/airflow/stocks/{ticker}/{ticker}_{ds_nodash}.csv"
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    yfinance.Ticker(ticker).history(
        period = "1d",
        interval = "1h",
        start = ds_add(ds, -1),
        end = "2022-01-10",
        prepost=True
    ).to_csv(file_path)
    sleep(10)
    
@dag(
    schedule_interval = "0 0 * * *",
    start_date = pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup = True
)
def get_crypto_dag():
    for ticker in TICKERS:
        get_history.override(task_id=ticker)(ticker)

dag = get_crypto_dag()
