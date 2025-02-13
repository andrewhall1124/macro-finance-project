from fredapi import Fred
import yfinance as yf
from dotenv import load_dotenv
import os
import polars as pl
from datetime import date

def get_two_year_data():
    load_dotenv()

    api_key = os.getenv('FRED_API_KEY')

    fred = Fred(api_key=api_key)
    data = fred.get_series('DGS2').reset_index()

    df = pl.from_pandas(data)
    df = df.rename({'index': 'date', '0': 'yield'})
    df = df.cast({'date': pl.Date})

    df = df.drop_nulls(subset='yield').sort('date')
    
    return df

def get_sector_etf_data():
    df = yf.download(
        tickers=['SPY'],
        start=date(1976, 6, 1),
        end=date.today()
    )

    df = df.stack(future_stack=True).reset_index()

    df = pl.from_pandas(df)
    df = df.rename({col: col.lower() for col in df.columns})
    df = df.cast({'date': pl.Date})

    df = df.select(['date', 'ticker', 'close', 'open'])

    return df

if __name__ == "__main__":
    two_year = get_two_year_data()
    equities = get_sector_etf_data()

    two_year.write_parquet('data/two_year.parquet')
    equities.write_parquet('data/equities.parquet')

    print(two_year)
    print(equities)