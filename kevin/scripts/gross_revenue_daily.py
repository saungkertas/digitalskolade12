import snowflake.connector
from datetime import date, timedelta

snowflake_conn = snowflake.connector.connect(
    user='kevinity310',
    password='@2Wsx1qaz',
    account='at53566.ap-southeast-1',
    warehouse="COMPUTE_WH",
    database='DIGITAL_SKOLA',
    schema='PROJECT_FINAL'
)

snowflake_cursor = snowflake_conn.cursor()
print("Successfully connected to Snowflake!")

current_date = date.today()
tomorrow = current_date + timedelta(days=1)

print("Data Running at :", tomorrow)

create_table_query = """
CREATE TABLE IF NOT EXISTS gross_revenue_daily (
    ORDER_DATE DATE,
    TOTAL_PRICE FLOAT
)
"""

snowflake_cursor.execute(create_table_query)
snowflake_conn.commit()

insert_data = f"""
INSERT INTO gross_revenue_daily (order_date, total_price)
SELECT daily_order_date, total_price
FROM (
    SELECT
        DATE_TRUNC('day', o.order_date) AS daily_order_date,
        SUM(od.unit_price * od.quantity * (1 - od.discount)) AS total_price
    FROM
        orders o
    INNER JOIN
        order_details od ON o.order_id = od.order_id
    WHERE
        DATE_TRUNC('day', o.order_date) = '{tomorrow}'
    GROUP BY
        DATE_TRUNC('day', o.order_date)
) sub
WHERE NOT EXISTS (
    SELECT 1 FROM gross_revenue_daily
    WHERE order_date = sub.daily_order_date
)
"""
print("Prosess inserting data ...")
snowflake_cursor.execute(insert_data)

snowflake_conn.commit()
print("✅ Success to Insert Data")

create_view = """
    CREATE OR REPLACE VIEW view_gross_revenue_daily AS 
        SELECT * FROM gross_revenue_daily
    """

snowflake_cursor.execute(create_view)
snowflake_conn.commit()
print("✅ Success to Update View")

# Close the connection
snowflake_cursor.close()
