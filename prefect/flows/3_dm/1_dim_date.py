import pandas as pd
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector


@task(name="load_data", log_prints=True)
def load_data() -> pd.DataFrame:
    sql_query = f"""
    select  
	distinct(date),
	to_char(date, 'YYYYMMDD') as date_id,
	to_char(date, 'Month') as month,
	to_char(date, 'YYYY') as year
    from (
        select 
            distinct(b.shipping_limit_date::date) as date
        from cln.t_orders a
        join cln.t_order_items b
        on a.order_id = b.order_id
        union
        select 
            distinct(a.order_purchase_timestamp::date) as date
        from cln.t_orders a
        join cln.t_order_items b
        on a.order_id = b.order_id
    ) a
    order by date desc
    """
    database_block = SqlAlchemyConnector.load("perqara-database")
    with database_block.get_connection(begin=False) as engine:
        df = pd.read_sql(sql_query, con=engine)
        df.date = pd.to_datetime(df.date)
        df['date_id'] = df['date_id'].astype('int64')
        print(f"data :\n {df}")
        print(f"data :\n {df.dtypes}")

    return df


@task(name="insert dim", log_prints=True)
def insert_dim_table(table_target, df):
    database_block = SqlAlchemyConnector.load("perqara-database")
    with database_block.get_connection(begin=False) as engine:
        df.to_sql(
            table_target, schema="dm", con=engine, if_exists="replace", index=False
        )


@flow(name="insert_dim_date", log_prints=True)
def populate_dim_date():
    """1. fetching data"""
    df = load_data()

    """2. insert to dim_date"""

    table_target = "dim_date"
    insert_dim_table(table_target, df)


if __name__ == "__main__":
    populate_dim_date()
