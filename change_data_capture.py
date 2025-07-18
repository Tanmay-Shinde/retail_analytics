import pandas as pd
import connection
from datetime import datetime


def change_dtl_data_capture(engine, file_path_dtl, max_db_date):
    daily_dtl_df = pd.read_csv(file_path_dtl)

    new_transactions = daily_dtl_df[daily_dtl_df['tran_dt'] > max_db_date]
    new_transactions.to_sql(name='tran_dtl', con=engine, if_exists='append', index=False)


def change_hdr_data_capture(engine, filepath_hdr, file_path_dtl):
    query = "SELECT MAX(tran_dt) FROM tran_hdr"
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)

    max_db_date = df.iloc[0]['MAX(tran_dt)']

    daily_df = pd.read_csv(filepath_hdr)
    max_daily_df_date = max(daily_df['tran_dt'])

    max_daily_df_date = max_daily_df_date.replace("-", "")

    max_daily_df_date = datetime.strptime(max_daily_df_date, '%Y%m%d')

    flag = False

    if max_daily_df_date > max_db_date:
        new_transactions = df[df['tran_dt'] > max_db_date]
        new_transactions.to_sql(name='tran_hdr', con=engine, if_exists='append', index=False)

        change_dtl_data_capture(engine, file_path_dtl, max_db_date)
        flag = True

    if flag:
        print("Updated database with new transactions")
    else:
        print("No new transactions")


def main():
    engine = connection.get_engine()

    filepath_hdr = "./data/daily_data/tran_hdr_daily.csv"
    filepath_dtl = "./data/daily_data/tran_dtl_daily.csv"

    change_hdr_data_capture(engine, filepath_hdr, filepath_dtl)
