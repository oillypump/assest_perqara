import pandas as pd
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector


@task(name="fetching stg_product_cat_name_translation", log_prints=True)
def fetching_data(table_source) -> pd.DataFrame:
    database_block = SqlAlchemyConnector.load("perqara-database")
    with database_block.get_connection(begin=False) as engine:
        df = pd.read_sql_table(table_source, schema="stg", con=engine)
        print(f"fetching data :\n {df}")
        print(f"fetching data :\n {df.dtypes}")
    return df


@task(name="transform data", log_prints=True)
def transform_stg_product_cat_name_translation(df):
    df["product_cat_name"] = (
        df["product_category_name_english"].str.replace("_", " ").str.title()
    )

    print(f"transformed data :\n {df}")
    print(f"transformed data :\n {df.dtypes}")

    return df


@task(name="insert to db cln", log_prints=True)
def insert_to_db_cln(df, table_target):
    database_block = SqlAlchemyConnector.load("perqara-database")
    with database_block.get_connection(begin=False) as engine:
        df.to_sql(
            table_target, schema="cln", con=engine, if_exists="replace", index=False
        )


@flow()
def cln_stg_product_cat_name_translation():
    """1. fetching from sql db stg"""
    table_source = "stg_product_cat_name_translation"
    df = fetching_data(table_source)

    """2. transform """
    df = transform_stg_product_cat_name_translation(df)
    """3. insert into db cln"""
    table_target = "t_product_cat_name_translation"
    insert_to_db_cln(df, table_target)


if __name__ == "__main__":
    cln_stg_product_cat_name_translation()
