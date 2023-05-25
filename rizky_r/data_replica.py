import psycopg2
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Koneksi ke PostgreSQL
pg_conn = psycopg2.connect(
    host=POSTGREE_HOST,
    port=POSTGREE_PORT,
    database=POSTGREE_DB,
    user=POSTGREE_USERNAME,
    password=POSTGREE_PASSWORD
)

# Koneksi ke Snowflake
user = SNOWFLAKE_USERNAME
password = SNOWFLAKE_PASSWORD
account = SNOWFLAKE_ACCOUNT
database = SNOWFLAKE_DATABASE
warehouse = SNOWFLAKE_WAREHOUSE
schema = SNOWFLAKE_SCHEMA
sf_table_categories = SNOWFLAKE_TABLE

connection_string = f'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}'

sf_engine = create_engine(connection_string)
# Session = sessionmaker(bind=engine)
# session = Session()


# Eksekusi query untuk mengekstrak data dari PostgreSQL Khusus table categories
pg_cursor = pg_conn.cursor()
pg_query = f'SELECT * FROM {sf_table_categories}'
pg_cursor.execute(pg_query)
rows_category = pg_cursor.fetchall()
pg_cursor.close()

df_category = pd.DataFrame(rows_category, columns=[desc[0] for desc in pg_cursor.description])
df_category['picture'] = df_category['picture'].astype(str)
print(df_category)

df_category.to_sql(sf_table_categories, con=sf_engine, if_exists='append', index=False)



# Dictionary mapping PostgreSQL tables to Snowflake tables
table_mapping = {
    'orders': 'orders',
    'customers': 'customers',
    'shippers' : 'shippers',
    'order_details': 'order_details',
    'products' : 'products',
    'suppliers' : 'suppliers',
    'customer_customer_demo' : 'customer_customer_demo',
    'customer_demographics' : 'customer_demographics',
}

# Batch size for insertion
batch_size = 1000

# Iterate over the table mapping and insert tables from PostgreSQL to Snowflake
for pg_table, sf_table in table_mapping.items():
    # Create SQL query to select all data from the PostgreSQL table
    sql_query = f"SELECT * FROM {pg_table}"

    # Read the table data from PostgreSQL into a DataFrame
    df = pd.read_sql(sql_query, pg_conn)

    # Convert memoryview objects to bytes in the DataFrame
    for column in df.columns:
        if df[column].dtype == 'object':
            df[column] = df[column].apply(lambda x: bytes(x) if isinstance(x, memoryview) else x)

    # # Split the DataFrame into batches
    num_batches = len(df) // batch_size + 1
    df_batches = np.array_split(df, num_batches)

    # # Insert the DataFrame batches into the Snowflake table
    for batch_df in df_batches:
        batch_df.to_sql(sf_table, sf_engine, if_exists='append', index=False)


# Tutup koneksi ke PostgreSQL
pg_conn.close()
sf_engine.dispose()



