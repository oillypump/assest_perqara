import pandas as pd
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector


@task(name="load_data", log_prints=True)
def load_data() -> pd.DataFrame:
    sql_query = f"""
    select
	row_number() over(order by order_status) as order_status_id,
	order_status
    from
        (
        select 
            distinct(order_status) 
        from cln.t_orders
        ) a
    """
    database_block = SqlAlchemyConnector.load("perqara-database")
    with database_block.get_connection(begin=False) as engine:
        df = pd.read_sql(sql_query, con=engine)

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


@flow(name="insert_dim_product_cat", log_prints=True)
def populate_dim_order_status():
    """1. fetching data"""
    df = load_data()
    """2. insert to dim_product_cat"""

    table_target = "dim_status_order"
    insert_dim_table(table_target, df)


if __name__ == "__main__":
    populate_dim_order_status()
