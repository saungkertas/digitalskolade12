import snowflake.connector

# Koneksi ke Snowflake
snowflake_conn = snowflake.connector.connect(
    user='achmaddwicahya',
    password='***************',
    account='pp45544.ap-southeast-1',
    warehouse="COMPUTE_WH",
    database='FINAL_PROJECT',
    schema='NORTHWIND'
)

# Membuat cursor untuk menjalankan perintah SQL
snowflake_cursor = snowflake_conn.cursor()

# Membuat tabel "cahya_Daily_Gross_Revenue" jika belum ada
create_new_table = """
CREATE TABLE IF NOT EXISTS y (
    ORDER_DATE DATE,
    GROSS_REVENUE FLOAT
)
"""
snowflake_cursor.execute(create_new_table)
snowflake_conn.commit()

# Memasukkan data ke dalam tabel "cahya_Daily_Gross_Revenue"
insert_data = f"""
INSERT INTO cahya_Daily_Gross_Revenue (ORDER_DATE, GROSS_REVENUE)
SELECT
  o.ORDER_DATE AS ORDER_DATE,
  SUM(od.QUANTITY * p.UNIT_PRICE) AS GROSS_REVENUE
FROM
  cahya_orders o
INNER JOIN cahya_order_details od ON o.ORDER_ID = od.ORDER_ID
INNER JOIN cahya_products p ON od.PRODUCT_ID = p.PRODUCT_ID
WHERE
  YEAR(o.ORDER_DATE) = 2023
GROUP BY
  o.ORDER_DATE
ORDER BY
  o.ORDER_DATE;
"""
snowflake_cursor.execute(insert_data)
snowflake_conn.commit()
