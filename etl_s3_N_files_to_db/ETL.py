import os
import boto3
import psycopg2
from io import BytesIO
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy

# Función para conectar a la base de datos
def acces_to_database(current_directory):

    current_directory = current_directory + '\config\db_config.txt'
    with open(current_directory, 'r') as archivo: 
        data_config = archivo.read().split('\n')   
 
    db_config = {
    "database": data_config[0],
    "user": data_config[1],
    "password": data_config[2],
    "host": data_config[3],
    "port": data_config[4]
    }

    return db_config

def create_table_sales(db_config,current_directory):

    conn = psycopg2.connect(**db_config) 
    cursor = conn.cursor()

    current_directory = current_directory + '\database\\ventas_ddl.txt'
 
    with open(current_directory, 'r') as archivo:
            ventas = archivo.read()
    cursor.execute(ventas)
    conn.commit()  
    conn.close() 
     

def extract(obj,s3,bucket_name):
    # Obtiene el nombre del objeto
    object_key = obj['Key']
    
    # Descarga el archivo desde S3
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    file_content = response['Body'].read() 
    
    # Lee el contenido del archivo en un DataFrame
    df = pd.read_csv(BytesIO(file_content))  # Puedes cambiar read_csv por otros métodos dependiendo del tipo de archivo

    return df

def transform(df):
    column_mapping = {
        'Order ID': 'order_id',
        'Product': 'product',
        'Quantity Ordered': 'quantity_ordered',
        'Price Each': 'price_each',
        'Order Date': 'order_date',
        'Purchase Address': 'purchase_address'
    }      

    df = df.rename(columns=column_mapping) 

    return df

def load(df,engine,obj):

    object_key = obj['Key']
    
    # Inserta el DataFrame en la base de datos
    df.to_sql('ventas', engine,if_exists='append', index=False)


    print(f"se carga el arhivo {object_key} a la tabla ventas")
 

def main(current_directory):
      
    db_config = acces_to_database(current_directory)

    create_table_sales(db_config,current_directory)

    conn = psycopg2.connect(**db_config) 

    engine = create_engine(f'postgresql+psycopg2://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}')
    cursor = conn.cursor()

    s3 = boto3.client('s3')

    # Nombre del bucket que deseas iterar
    bucket_name = 'ventas2019'

    # Lista todos los objetos en el bucket
    response = s3.list_objects_v2(Bucket=bucket_name)

    # Itera sobre los objetos en el bucket
    for obj in response.get('Contents', []):
        
        #EXTRACT
        df = extract(obj,s3,bucket_name)
        
        #TRANSFORM
        df = transform(df)

        #LOAD
        load(df,engine,obj)
 

if __name__ == '__main__':
    
    current_directory = os.path.dirname(__file__)

    main(current_directory)

 