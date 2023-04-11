## to do

- [ ] create ingest python 1 by 1 with params
- [ ] cleaning data in each table do :
    * a. by unique id drop duplicate 
    * b. change type table text to varchar(utk yang cuma 2 huruf atau kurang dari 2 huruf)
    * c. 


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

- run server prefect

```
$ prefect server start
```
- run prefect agent
```
$ prefect agent start -p 'default-agent-pool'
```
- create db cln for cleaned and transformed data
- create db dm for data mart

- execute

# create block connection to postgresql

- go to UI localhost:4200 > click + buttons
- add sqlalchemy connector
    * block name = perqara-stg
    * driver = SyncDriver > postgresl+psycopg2
    * database = perqara_stg
    * username = root
    * password = root
    * host = localhost
    * port = 5432
- save

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


