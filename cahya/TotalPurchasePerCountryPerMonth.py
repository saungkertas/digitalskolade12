import snowflake.connector

# Koneksi ke Snowflake
snowflake_conn = snowflake.connector.connect(
    user='achmaddwicahya',
    password='************',
    account='pp45544.ap-southeast-1',
    warehouse="COMPUTE_WH",
    database='FINAL_PROJECT',
    schema='NORTHWIND'
)

# Membuat cursor untuk menjalankan perintah SQL
snowflake_cursor = snowflake_conn.cursor()

# Membuat tabel "cahya_Total_Purchase_Per_Country_Per_Month" jika belum ada
create_new_table = """
CREATE TABLE IF NOT EXISTS cahya_Total_Purchase_Per_Country_Per_Month (
    Country VARCHAR,
    Order_Month DATE,
    Total_Purchase NUMBER
)
"""
snowflake_cursor.execute(create_new_table)
snowflake_conn.commit()

# Memasukkan data ke dalam tabel "cahya_Total_Purchase_Per_Country_Per_Month"
insert_data = f"""
INSERT INTO cahya_Total_Purchase_Per_Country_Per_Month (Country, Order_Month, Total_Purchase)
SELECT
    cahya_orders.SHIP_COUNTRY AS Country,
    DATE_TRUNC('MONTH', ORDER_DATE) AS Order_Month,
    SUM(cahya_order_details.QUANTITY) AS Total_Purchase
FROM
    cahya_orders
    INNER JOIN cahya_order_details ON cahya_orders.ORDER_ID = cahya_order_details.ORDER_ID
WHERE
    YEAR(cahya_orders.ORDER_DATE) = 2023
GROUP BY
    cahya_orders.SHIP_COUNTRY,
    DATE_TRUNC('MONTH', ORDER_DATE)
ORDER BY
    3 DESC;
"""

snowflake_cursor.execute(insert_data)
snowflake_conn.commit()

