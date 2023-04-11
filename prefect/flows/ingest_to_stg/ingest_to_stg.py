import pandas as pd
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector


@task(name="fetching data from dir", log_prints=True)
def fetching_data(dataset_filename: str) -> pd.DataFrame:
    # path = f"./data/{dataset_filename}.csv"
    path = f"/home/ubuntu/work/python3/perqara/data/{dataset_filename}.csv"
    print(f"your dataset to fetch =\n {dataset_filename}")
    print(f"your path to fetch =\n {path}")
    df = pd.read_csv(path)
    return df


@task(name="insert data to db stg", log_prints=True)
def insert_data(table_name, df):
    print(df)
    database_block = SqlAlchemyConnector.load("perqara-stg")
    with database_block.get_connection(begin=False) as engine:
        df.to_sql(table_name, con=engine, if_exists="replace", index=False)


@flow(name="Ingest CSV to STG", log_prints=True)
def ingest_csv_to_stg(dataset_filename: str, table_name: str):
    """1. fetching data"""

    # dataset_filename = "customers_dataset"
    df = fetching_data(dataset_filename)

    """2. ingest to db"""
    # table_name = "stg_customers"
    insert_data(table_name, df)


@flow()
def ingest_csv_parent_flow(dataset_filename: str, table_name: str):
    ingest_csv_to_stg(dataset_filename, table_name)


if __name__ == "__main__":
    dataset_filename = "customers_dataset"
    table_name = "stg_customers"
    ingest_csv_parent_flow(dataset_filename, table_name)
