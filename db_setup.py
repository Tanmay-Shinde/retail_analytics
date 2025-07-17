import connection
import pandas as pd
import numpy as np
from datetime import datetime


def setup_tran_dtl(mydb, engine):
    cursor = mydb.cursor()
    cursor.execute("CREATE TABLE tran_dtl (tran_id VARCHAR(255), product_id INT, qty INT, amt FLOAT(10, 2), "
                   "tran_dt DATE, FOREIGN KEY (tran_id) REFERENCES tran_hdr(tran_id), "
                   "FOREIGN KEY (product_id) REFERENCES product(product_id))")
    mydb.commit()
    cursor.close()

    return


def setup_tran_hdr(mydb, engine):
    cursor = mydb.cursor()
    cursor.execute("CREATE TABLE tran_hdr (tran_id VARCHAR(255) PRIMARY KEY, store_id INT, member_id INT, "
                   "tran_dt DATE, FOREIGN KEY (member_id) REFERENCES member(member_id))")
    mydb.commit()
    cursor.close()

    start_date = '2022-01-01'
    end_date = '2024-12-31'

    date_range = pd.date_range(start_date, end_date)

    hdr_dim = pd.DataFrame(date_range)
    hdr_dim.columns = ['date']

    hdr_dim['is_weekend'] = (hdr_dim['date'].dt.dayofweek >= 5).astype(int)

    hdr_dim['date'] = pd.to_datetime(hdr_dim['date'], unit='s')
    hdr_dim['date'] = hdr_dim['date'].dt.date

    hdr_dim['num_trans'] = np.nan

    hdr_dim.loc[hdr_dim['is_weekend'] == 0, 'num_trans'] = (
        np.random.randint(1, 11, size=len(hdr_dim[hdr_dim['is_weekend'] == 0]))
    )
    hdr_dim.loc[hdr_dim['is_weekend'] == 1, 'num_trans'] = (
        np.random.randint(15, 25, size=len(hdr_dim[hdr_dim['is_weekend'] == 1]))
    )

    hdr_dim['num_trans'] = hdr_dim['num_trans'].astype(int)

    tran_ids = []
    date_col = []

    for row in hdr_dim.iterrows():
        for i in range(0, row[1]['num_trans']):
            date = row[1]['date']
            # tran_id = str(row[1]['date']) + "T" + str(datetime.now().time()) + "_" + str(i)
            # tran_ids.append(tran_id)
            date_col.append(date)

    # sum(hdr_dim['num_trans']) -> 10423
    # len(tran_ids) -> 10423
    # len(date_col) -> 10423

    dates = pd.Series(date_col)
    tran_ids = pd.Series(tran_ids)

    tran_hdr_df = pd.DataFrame({"tran_id": tran_ids, "tran_dt": dates})

    tran_hdr_df['store_id'] = np.random.randint(1, 4, size=len(tran_hdr_df))

    members_df = pd.read_csv("./data/sample_data/member_sample.csv")
    mem_array = members_df['member_id'].values

    tran_hdr_df['member_id'] = np.random.choice(mem_array, size=len(tran_hdr_df))

    tran_hdr_df = tran_hdr_df[['tran_id', 'store_id', 'member_id', 'tran_dt']]

    tran_hdr_df.to_sql(name='tran_hdr', con=engine, if_exists='append', index=False)


def setup_product(mydb, engine):
    cursor = mydb.cursor()
    cursor.execute("CREATE TABLE product (product_id INT PRIMARY KEY, description VARCHAR(255), price FLOAT(10, 2), "
                   "category VARCHAR(255), max_qty INT)")
    mydb.commit()
    cursor.close()

    prod_df = pd.read_csv("./data/sample_data/product_sample.csv")
    prod_df.dropna(axis=0, inplace=True)
    prod_df.to_sql('product', con=engine, if_exists='append', index=False)


def setup_member(mydb, engine):
    cursor = mydb.cursor()
    cursor.execute("CREATE TABLE member (member_id INT PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255), "
                   "store_id INT, reg_date DATE)")
    mydb.commit()
    cursor.close()

    mem_df = pd.read_csv("./data/sample_data/member_sample.csv")
    mem_df['reg_date'] = pd.to_datetime(mem_df['reg_date'])
    mem_df['reg_date'] = mem_df['reg_date'].apply(lambda x: x.replace(year=x.year + 3))

    mem_df.to_sql('member', con=engine, if_exists='append', index=False)


def main():
    mydb = connection.get_db()
    engine = connection.get_engine()

    setup_member(mydb, engine)
    setup_product(mydb, engine)
    setup_tran_hdr(mydb, engine)
    setup_tran_dtl(mydb, engine)

    return


if __name__ == '__main__':
    main()
