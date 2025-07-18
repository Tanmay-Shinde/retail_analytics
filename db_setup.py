import connection
import pandas as pd
import numpy as np
from datetime import datetime


def setup_tran_dtl(mydb, engine, tran_ids):
    print("# tran_dtl: ", end="")
    cursor = mydb.cursor()
    cursor.execute("CREATE TABLE tran_dtl (tran_id VARCHAR(255), product_id INT, qty INT, amt FLOAT(10, 2), "
                   "tran_dt DATE, FOREIGN KEY (tran_id) REFERENCES tran_hdr(tran_id), "
                   "FOREIGN KEY (product_id) REFERENCES product(product_id))")
    mydb.commit()
    cursor.close()

    query = "SELECT tran_id, tran_dt FROM tran_hdr ORDER BY tran_dt"
    with engine.connect() as conn:
        dtl_dim = pd.read_sql(query, conn)

    dtl_dim['num_prods'] = np.random.randint(1, 11, size=len(dtl_dim))
    dtl_dim['num_prods'] = dtl_dim['num_prods'].astype(int)

    query = "SELECT product_id FROM product"
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    products = df['product_id'].values
    prod_ids = []
    tran_id_col = []
    date_col = []

    for row in dtl_dim.iterrows():
        for i in range(row[1]['num_prods']):
            # prod_ids.append(np.random.choice(products, size=1))
            # tran_id_col.append(row[1]['tran_id'])
            date_col.append(row[1]['tran_dt'])

    # sum(dtl_dim['num_prods']) -> 57642
    # len(tran_id_col) -> 57642
    # len(prod_ids) -> 57642
    # len(date_col) -> 57642

    tran_id_col = pd.Series(tran_id_col)
    prod_ids = pd.Series(prod_ids)
    date_col = pd.Series(date_col)

    tran_dtl_dim = pd.DataFrame({"tran_id": tran_id_col, "product_id": prod_ids, "tran_dt": date_col})
    tran_dtl_dim['product_id'] = tran_dtl_dim['product_id'].astype(int)

    query = "SELECT product_id, max_qty FROM product"
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    prod_qty = df.set_index('product_id')['max_qty'].to_dict()

    amt = []

    for row in tran_dtl_dim.iterrows():
        product_id = row[1]['product_id']
        max_qty = prod_qty[product_id]
        amt.append(np.random.randint(1, max_qty + 1, size=1))

    # len(amt) -> 57642

    tran_dtl_dim['qty'] = amt
    tran_dtl_dim['qty'] = tran_dtl_dim['qty'].astype(int)

    query = "SELECT product_id, price FROM product"
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    price_dict = df.set_index('product_id')['price'].to_dict()

    tran_dtl_dim['amt'] = tran_dtl_dim.apply(lambda row: price_dict.get(row['product_id']) * row['qty'], axis=1)

    tran_dtl_dim = tran_dtl_dim[['tran_id', 'product_id', 'qty', 'amt', 'tran_dt']]

    tran_dtl_dim.to_sql(name='tran_dtl', con=engine, if_exists='append', index=False)
    print("Setup Completed")


def setup_tran_hdr(mydb, engine):
    print("# tran_hdr: ", end="")
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
            tran_id = str(row[1]['date']) + "T" + str(datetime.now().time()) + "_" + str(i)
            tran_ids.append(tran_id)
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
    print("Setup Completed")

    setup_tran_dtl(mydb, engine, tran_ids)


def setup_product(mydb, engine):
    print("# product: ", end="")
    cursor = mydb.cursor()
    cursor.execute("CREATE TABLE product (product_id INT PRIMARY KEY, description VARCHAR(255), price FLOAT(10, 2), "
                   "category VARCHAR(255), max_qty INT)")
    mydb.commit()
    cursor.close()

    prod_df = pd.read_csv("./data/sample_data/product_sample.csv")
    prod_df.dropna(axis=0, inplace=True)
    prod_df.to_sql('product', con=engine, if_exists='append', index=False)
    print("Setup Completed")


def setup_member(mydb, engine):
    print("# member: ", end="")
    cursor = mydb.cursor()
    cursor.execute("CREATE TABLE member (member_id INT PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255), "
                   "store_id INT, reg_date DATE)")
    mydb.commit()
    cursor.close()

    mem_df = pd.read_csv("./data/sample_data/member_sample.csv")
    mem_df['reg_date'] = pd.to_datetime(mem_df['reg_date'])
    mem_df['reg_date'] = mem_df['reg_date'].apply(lambda x: x.replace(year=x.year + 3))

    mem_df.to_sql('member', con=engine, if_exists='append', index=False)
    print("Setup Completed")


def setup_store(mydb, engine):
    print("# store: ", end="")
    cursor = mydb.cursor()
    cursor.execute("CREATE TABLE store (store_id INT PRIMARY KEY, city VARCHAR(255), email VARCHAR(255))")
    mydb.commit()
    cursor.close()

    store_df = pd.DataFrame({'store_id': [1, 2, 3], 'city': ['Mumbai', 'Bangalore', 'Delhi'],
                             'email': ['store.mumbai@email.com', 'store.bangalore@email.com', 'store.delhi@email.com']})

    store_df.to_sql(name='store', con=engine, if_exists='append', index=False)


def main():
    mydb = connection.get_db()
    engine = connection.get_engine()

    print("Setting up the following tables: ")
    setup_member(mydb, engine)
    setup_product(mydb, engine)
    setup_tran_hdr(mydb, engine)
    print("Setup Complete")


if __name__ == '__main__':
    main()
