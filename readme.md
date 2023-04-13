## About

bla bla

## steps.

- create virt env
```
$ python -m venv venv
```

- activate virt env
```
$ source venv/bin/activate
```  

- install requirements
```
$ pip install -r requirements.txt
```

- running up postgresql and pgadmin [container](pg_db_container/docker-compose.yml)
```
$ mkdir data_pgadmin
$ mkdir data_postgresql

$ docker compose up -d

$ sudo chown 5050:5050 data_pgadmin
$ sudo chmod a+rwx data_postgresql
```
- create schemas
```
CREATE SCHEMA stg
    AUTHORIZATION root;

CREATE SCHEMA cln
    AUTHORIZATION root;

CREATE SCHEMA dm
    AUTHORIZATION root;
```

- run server prefect

```
$ prefect server start
```
- run prefect agent
```
$ prefect agent start -p 'default-agent-pool'
```


# create block connection to postgresql

- go to UI localhost:4200 > click + buttons
- add sqlalchemy connector
    * block name = perqara-database
    * driver = SyncDriver > postgresl+psycopg2
    * database = perqara_database
    * username = root
    * password = root
    * host = localhost
    * port = 5432
- save

- create schema stg from pg_admin UI
- create schema cln pg_admin UI
- create schema dm pg_admin UI

# ingest raw data to DB
- deploy ingestion [code](/prefect/flows/ingest_to_stg/ingest_to_stg.py) 
```
$ prefect deployment build ingest_to_stg.py:ingest_csv_parent_flow -n "Insert Staging with Params"
```
- apply ingestion .yaml file
```
$ prefect deployment apply ingest_csv_parent_flow-deployment.yaml
```
- go to UI localhost:4200 > Deployment > click three dots in right corner > Quick run   
- input parameters which suitable as your dataset_filename and table_name csv 
    ex : dataset_filename : geolocation_dataset, table_name = stg_geolocation
- Run
- Repeat on each dataset

**Note** : make sure path variable in this [code](/prefect/flows/ingest_to_stg/ingest_to_stg.py)  match in your dir.

# clean/transform data

# create data mart

## [requirements](requirements.txt)


select
	a.order_id,
	count(a.order_id) as order_quantity,
	sum(b.price) as price,
	c.order_status_id,
	d.date_id as purchase_date,
	e.date_id 
-- 	*	
from cln.t_orders a
join cln.t_order_items b
on a.order_id = b.order_id
join dm.dim_status_order c
on a.order_status = c.order_status
join dm.dim_date d
on a.order_purchase_timestamp::date = d.date
join dm.dim_date e
on a.order_approved_at::date = d.date

group by 
	a.order_id,
	c.order_status_id,
	d.date_id,
	e.date_id
order by a.order_id desc
