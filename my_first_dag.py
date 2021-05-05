import datetime as dt
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import DAG
import psycopg2 as ps
import pandas as pd
from airflow.hooks.base_hook import BaseHook

class Table_Field:
    def __init__(self,name,type):
        self.Name = name
        self.Type = type
       

class Table_Info:
    name = "test2"

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
        when lotsizecode='Metres' THEN CAST(NULLIF(lotdepth,'') as DOUBLE PRECISION) * 3.28 ELSE cast(NULLIF(lotdepth,'') as                DOUBLE PRECISION)           END lotdepth,
            case 
        when lotsizecode='Metres' THEN CAST(NULLIF(lotfront,'') as DOUBLE PRECISION) * 3.28 ELSE CAST(NULLIF(lotfront,'') as                DOUBLE PRECISION)           END lotfront,

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
            limit 10;
    '''
   


    fields = [Table_Field("id", "bigint"),
              Table_Field("area", "VARCHAR"),
              Table_Field("basement1", "VARCHAR"),
              Table_Field("bedrooms", "decimal"),
             
              Table_Field("bedroomsplus", "decimal"),
              Table_Field("community", "VARCHAR"),
              Table_Field("drive", "VARCHAR"),
              Table_Field("exterior1", "VARCHAR"),
             
              Table_Field("familyroom", "VARCHAR"),
              Table_Field("fireplacestove", "VARCHAR"),
              Table_Field("frontingonnsew", "VARCHAR"),
              Table_Field("garagespaces", "decimal"),

              Table_Field("garagetype", "VARCHAR"),
              Table_Field("heatsource", "VARCHAR"),
              Table_Field("heattype", "VARCHAR"),
              Table_Field("lotdepth", "decimal"),

              Table_Field("lotfront", "decimal"),
              Table_Field("parkingspaces", "decimal"),
              Table_Field("pool", "VARCHAR"),
              Table_Field("sewers", "VARCHAR"),

              Table_Field("date", "decimal"),
              Table_Field("style", "VARCHAR"),
              Table_Field("own1outtype", "VARCHAR"),
              Table_Field("washrooms", "decimal"),

              Table_Field("sold_price", "decimal"),
              Table_Field("propertyfeatures1", "VARCHAR"),
              Table_Field("propertyfeatures2", "VARCHAR"),
              Table_Field("propertyfeatures3", "VARCHAR"),

              Table_Field("propertyfeatures4", "VARCHAR"),
              Table_Field("propertyfeatures5", "VARCHAR"),
              Table_Field("propertyfeatures6", "VARCHAR"),
              
             ]
   
    def __init__(self):
        self.now = dt.datetime.now()
   
    def drop_table_str(self):
        q= f"DROP TABLE {self.get_table_name()}"
        return q
   
    def get_table_name(self):
        t = self.now
        return f"{self.name}_{t.year}_{t.month:02d}_{t.day:02d}_{t.hour:02d}_{t.minute:02d}_{t.second:02d}"
   
   
    def create_table_str(self, columnsOfTypeTable_Field, tableName):
        q = f"CREATE TABLE {tableName} ("
       
        isFirst=True
        for item in columnsOfTypeTable_Field:
            if not isFirst==True: q+=" , "
            q+=f'"{item.Name}" {item.Type} NULL '
            isFirst = False
        q+=");"
        return q

tblInfo = Table_Info()


def private_create_conn_for_postgers(connection_id):
    airConn = BaseHook.get_connection(connection_id)

    conn = ps.connect(host=airConn.host,user=airConn.login,password=airConn.password,port=airConn.port,database=airConn.schema )

    return conn

def execute_insert_many(connection_id, df, table):
    """
    Using cursor.executemany() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.itertuples(index=False)]
    # Comma-separated dataframe columns
    cols = '","'.join(list(df.columns))
    
    qq= ('%s,' * (len(df.columns)-1))+'%s'

    query  = ('INSERT INTO %s("%s") VALUES(%s)') % (table, cols,qq)
    
    conn = private_create_conn_for_postgers(connection_id)
    cursor = conn.cursor()
    cursor.executemany(query, tuples)
    conn.commit()
    cursor.close()


def private_read_data_from_source(connection_id, query):
    conn = PostgresHook(postgres_conn_id=connection_id).get_conn()
    data = pd.read_sql(query, conn)
    return data

def privat_create_table(connection_id, createQuery):
    conn = private_create_conn_for_postgers(connection_id)
    cursor = conn.cursor()
    cursor.execute(createQuery)
    conn.commit()
    cursor.close()


def dag_fn_read_data_from_source_and_insert_middle():
    data = private_read_data_from_source("my_conn_postgress_source_db", tblInfo.source_query)

    
    createTableStr = tblInfo.create_table_str(tblInfo.fields, tblInfo.get_table_name())

    privat_create_table('my_conn_postgress_middle_db', createTableStr)

    execute_insert_many('my_conn_postgress_middle_db',data,tblInfo.get_table_name())

dag_fn_read_data_from_source_and_insert_middle()


def test(ds, **params):
    conn = PostgresHook(postgres_conn_id="my_conn_postgress_middle_db").get_conn()
    pd.read_sql("select id from test1", conn)


dag_params = {
    'dag_id': 'my_first_postgresOperator_dag',
    'start_date': dt.datetime(2019, 10, 7),
    'schedule_interval': None,
    
}

with DAG(**dag_params) as dag:

    read_data = PythonOperator(
        task_id = 'read_data_from_aws_and_insert_middle_db',
        python_callable = dag_fn_read_data_from_source_and_insert_middle
    )

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


#-------------------------------------------------------------------------------

def add_features(df):
    featuresdf = df[
        ['id', 'propertyfeatures1', 'propertyfeatures2', 'propertyfeatures3', 'propertyfeatures4', 'propertyfeatures5',
         'propertyfeatures6']]
    cleaned = featuresdf.set_index('id').stack()
    final_feature = pd.get_dummies(cleaned, prefix='ftr').groupby(level=0).sum()
    if 'ftr_                  ' in final_feature:
        final_feature.drop(['ftr_                  '], axis=1, inplace=True)
    if 'ftr_' in final_feature:
        final_feature.drop(['ftr_'], axis=1, inplace=True)
    df.set_index('id', inplace=True)

    finalDf = df.merge(final_feature, left_index=True, right_index=True)
    finalDf.drop(
        ['propertyfeatures1', 'propertyfeatures2', 'propertyfeatures3', 'propertyfeatures4', 'propertyfeatures5',
         'propertyfeatures6'], axis=1, inplace=True)
    return finalDf


def clean_data(df):
    values = {'bedroomsplus': 0}
    maindf = df.fillna(value=values)
    maindf.dropna(inplace=True)
    return maindf

def create_query_for_read():
    
    q = "select "
    
    isFirst = True
    for item in tblInfo.fields:
        if not isFirst==True:
            q+=" , "
        q+=item.Name
        isFirst=False

    q+=f" from {tblInfo.get_table_name()}"
    return q

def my_clean_data():

    query = create_query_for_read()
    data = private_read_data_from_source("my_conn_postgress_middle_db",query)
    data2 = clean_data(data)
    

    data3 = add_features(data2)

    print(data3)
    return data3


answer = my_clean_data()


#---------------------------------------------------------

columns=[]
for item in tblInfo.fields:
    if(item.Name in answer.columns):
        columns.append(item)

newColumns = [col for col in answer.columns if col.startswith('ftr_')]

for item in newColumns:
    columns.append(Table_Field(item,'VARCHAR'))


createQuery = tblInfo.create_table_str(columns, 'DataWarehouse')
print(createQuery)

privat_create_table("my_conn_postgress_middle_db", createQuery)


#--------------------------------------------------------

execute_insert_many("my_conn_postgress_middle_db",answer,'DataWarehouse')