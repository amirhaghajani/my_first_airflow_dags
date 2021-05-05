import datetime as dt
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import DAG
import psycopg2 as ps
import pandas as pd


dag_params = {
    'dag_id': 'my_first_postgresOperator_dag',
    'start_date': dt.datetime(2019, 10, 7),
    'schedule_interval': None,
}

source_query = '''
    SELECT  freehold_archive_id  id,
        area,
        basement1,
        cast(NULLIF(bedrooms,'') as DOUBLE PRECISION) bedrooms,
        cast(NULLIF(bedrooms_plus,'') as DOUBLE PRECISION) bedroomsplus,
        community,
        drive,
        exterior1,
        familyroom,
        fireplace_stove fireplacestove,
        frontingon frontingonnsew,
        cast(NULLIF(garagespaces,'') as DOUBLE PRECISION) garagespaces,
        garagetype,
        heatsource,
        heattype,
               case 
       when lotsizecode='Metres' THEN CAST(NULLIF(lotdepth,'') as DOUBLE PRECISION) * 3.28 ELSE cast(NULLIF(lotdepth,'') as DOUBLE PRECISION) END lotdepth,
        case 
       when lotsizecode='Metres' THEN CAST(NULLIF(lotfront,'') as DOUBLE PRECISION) * 3.28 ELSE CAST(NULLIF(lotfront,'') as DOUBLE PRECISION) END lotfront,

         CAst(NULLIF(parkingspaces,'') as DOUBLE PRECISION) parkingspaces,
        pool,
        sewers,
        EXTRACT(YEAR FROM contractdate)*100+ EXTRACT(MONTH FROM contractdate) date,

        style,
        type own1outtype,
        CAST(NULLIF(washrooms,'') as double precision) washrooms,
        sold_price,
        propertyfeatures1,
        propertyfeatures2,
        propertyfeatures3,
        propertyfeatures4,
        propertyfeatures5,
        propertyfeatures6

        from homeintel_dbo.vw_get_listing_residential_new_peel_halton_durham_toronto_york
        where trim(sale_lease)='Sale' and sold_price is not NULL and  contractdate>='2020-05-01'
        limit 100;
'''

def test(ds, **params):
    conn = PostgresHook(postgres_conn_id="my_conn_postgress_middle_db").get_conn()
    pd.read_sql("select id from test1", conn)

with DAG(**dag_params) as dag:

    # create_table = PostgresOperator(
    #     task_id='create_table',
    #     postgres_conn_id="my_conn_postgress_middle_db",
    #     sql='''CREATE TABLE new_table(
    #         custom_id integer NOT NULL, timestamp TIMESTAMP NOT NULL, user_id VARCHAR (50) NOT NULL
    #         );''',
    # )

    select_data = PythonOperator(
        task_id='select_data2',
        python_callable = test
    )