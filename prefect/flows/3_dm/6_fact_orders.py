import pandas as pd
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector


@task(name="load_data", log_prints=True)
def load_data() -> pd.DataFrame:
    sql_query = f"""
    select
	a.order_id,
	count(a.order_id) as order_quantity,
	sum(b.price) as price,
	c.order_status_id,
	d.date_id as purchase_date,
	f.product_id,
	e.customer_id

    from cln.t_orders a
    join cln.t_order_items b
    on a.order_id = b.order_id
    join dm.dim_status_order c
    on a.order_status = c.order_status
    join dm.dim_date d
    on a.order_purchase_timestamp::date = d.date
    join dm.dim_product_cat f
    on b.product_id = f.product_id
    join dm.dim_customers e
    on a.customer_id = e.customer_id

    group by 
        a.order_id,
        c.order_status_id,
        d.date_id,
        f.product_id,
        e.customer_id

    order by d.date_id desc


    """
    database_block = SqlAlchemyConnector.load("perqara-database")
    with database_block.get_connection(begin=False) as engine:
        df = pd.read_sql(sql_query, con=engine)

        print(f"data :\n {df}")
        print(f"data :\n {df.dtypes}")

    return df


@task(name="insert fact", log_prints=True)
def insert_fact_table(table_target, df):
    database_block = SqlAlchemyConnector.load("perqara-database")
    with database_block.get_connection(begin=False) as engine:
        df.to_sql(
            table_target, schema="dm", con=engine, if_exists="replace", index=False
        )


@flow(name="insert_fact_orders", log_prints=True)
def populate_fact_orders():
    """1. fetching data"""
    df = load_data()
    """2. insert to fact_orders"""

    table_target = "fact_orders"
    insert_fact_table(table_target, df)


if __name__ == "__main__":
    populate_fact_orders()
