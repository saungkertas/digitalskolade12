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
# Print a success message if the connection is established
print("Successfully connected to Snowflake!")

create_table = """
CREATE OR REPLACE TABLE total_order_per_country_per_month AS
SELECT DATE_TRUNC('month', o.order_date) AS ORDER_DATE,
o.ship_country, SUM((od.unit_price * od.quantity * (1 - od.discount))) as TOTAL_PRICE
FROM orders o
INNER JOIN order_details od ON o.order_id = od.order_id
GROUP BY DATE_TRUNC('month', o.order_date), o.ship_country
ORDER BY ORDER_DATE ASC;
"""

snowflake_cursor.execute(create_table)
snowflake_conn.commit()
print("✅ Success to Update Table total_order_per_country_per_month")

create_view = """
    CREATE OR REPLACE VIEW view_total_order_per_country_per_month AS 
        SELECT * FROM total_order_per_country_per_month
    """

snowflake_cursor.execute(create_view)
snowflake_conn.commit()
print("✅ Success to Update View")

# Close the connection
snowflake_cursor.close()


# Close the connection
snowflake_cursor.close()
