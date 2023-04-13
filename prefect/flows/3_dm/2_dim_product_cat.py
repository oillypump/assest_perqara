import pandas as pd
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector


@task(name="load_data", log_prints=True)
def load_data() -> pd.DataFrame:
    sql_query = f"""
    select 
	a.product_id,
	b.product_cat_name
        from cln.t_products a
        join cln.t_product_cat_name_translation b
        on a.product_category_name = b.product_category_name
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
def populate_dim_product_cat():
    """1. fetching data"""
    df = load_data()

    """2. insert to dim_product_cat"""

    table_target = "dim_product_cat"
    insert_dim_table(table_target, df)


if __name__ == "__main__":
    populate_dim_product_cat()
