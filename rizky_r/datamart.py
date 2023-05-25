import configparser
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Koneksi ke Snowflake
user = SNOWFLAKE_USERNAME
password = SNOWFLAKE_PASSWORD
account = SNOWFLAKE_ACCOUNT
database = SNOWFLAKE_DATABASE
warehouse = SNOWFLAKE_WAREHOUSE
schema = SNOWFLAKE_SCHEMA


connection_string = f'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}'

engine = create_engine(connection_string)
Session = sessionmaker(bind=engine)
session = Session()

credentials = configparser.ConfigParser()
credentials.read(session)

daily_gross_revenue = engine.execute(f'''
    CREATE OR REPLACE TABLE rizkyr_dm_daily_gross_revenue AS
    SELECT order_date, SUM(b.quantity * (b.unit_price - (b.unit_price * b.discount))) AS total
    FROM orders a
    LEFT JOIN order_details b ON a.order_id = b.order_id
    GROUP BY 1
    ORDER BY 1
    ''')

monthly_gross_revenue = engine.execute(f'''
    CREATE OR REPLACE TABLE rizkyr_dm_monthly_gross_revenue AS 
    SELECT order_date, product_name, sum(b.quantity*(b.unit_price-(b.unit_price*b.discount))) AS total 
    FROM orders a
    LEFT JOIN order_details b ON a.order_id=b.order_id
    LEFT JOIN products c ON b.product_id=c.product_id
    GROUP BY 1,2
    '''
)

monthly_total_purchase_per_product = engine.execute(f'''
    CREATE OR REPLACE TABLE rizkyr_dm_monthly_total_purchase_per_product AS 
    SELECT MONTH(order_date) as `month`, YEAR(order_date) AS `year`, product_name, sum(b.quantity) AS total FROM orders a
    LEFT JOIN order_details b ON a.order_id=b.order_id
    LEFT JOIN products c ON b.product_id=c.product_id
    GROUP BY 1,2,3
    '''
)


monthly_total_purchase_per_category = engine.execute(f'''
    CREATE OR REPLACE TABLE rizkyr_dm_monthly_total_purchase_per_category AS 
    SELECT month(order_date) AS `month`, year(order_date) AS `year`, d.category_name, sum(b.quantity) AS total FROM orders a
    LEFT JOIN order_details b ON a.order_id=b.order_id
    LEFT JOIN products c ON b.product_id=c.product_id
    LEFT JOIN categories d ON c.category_id=d.category_id
    GROUP BY 1,2,3
    '''
)

monthly_total_purchase_per_country = engine.execute(f'''
    CREATE OR REPLACE TABLE rizkyr_dm_monthly_total_purchase_per_country AS 
    SELECT month(order_date) AS `month`, year(order_date) AS `year`, ship_country AS country, count(b.quantity) AS total FROM orders a
    LEFT JOIN order_details b ON a.order_id=b.order_id
    GROUP BY 1,2,3
    '''
)