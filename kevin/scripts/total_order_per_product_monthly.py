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
CREATE OR REPLACE TABLE total_order_per_product_monthly AS
SELECT DATE_TRUNC('month', o.order_date) AS ORDER_DATE,
product_name, SUM(od.quantity) AS TOTAL_ORDER
FROM orders o
INNER JOIN order_details od ON o.order_id = od.order_id
LEFT JOIN products p ON p.product_id = od.product_id
GROUP BY DATE_TRUNC('month', o.order_date), product_name
ORDER BY ORDER_DATE, TOTAL_ORDER DESC;
"""

snowflake_cursor.execute(create_table)
snowflake_conn.commit()
print("✅ Success to Update Table total_order_per_product_monthly")

create_view = """
    CREATE OR REPLACE VIEW VIEW_TOTAL_ORDER_PER_PRODUCT_MONTHLY AS 
        SELECT * FROM total_order_per_product_monthly
    """

snowflake_cursor.execute(create_view)
snowflake_conn.commit()
print("✅ Success to Update View")

# Close the connection
snowflake_cursor.close()