import snowflake.connector

# Koneksi ke Snowflake
snowflake_conn = snowflake.connector.connect(
    user='achmaddwicahya',
    password='********',
    account='pp45544.ap-southeast-1',
    warehouse="COMPUTE_WH",
    database='FINAL_PROJECT',
    schema='NORTHWIND'
)

# Membuat cursor untuk menjalankan perintah SQL
snowflake_cursor = snowflake_conn.cursor()

# Membuat tabel "cahya_Gross_Revenue_Per_Product_Per_Month" jika belum ada
create_new_table = """
CREATE TABLE IF NOT EXISTS cahya_Gross_Revenue_Per_Product_Per_Month (
    Product_Name VARCHAR,
    Order_Month DATE,
    Gross_Revenue FLOAT
)
"""
snowflake_cursor.execute(create_new_table)
snowflake_conn.commit()

# Memasukkan data ke dalam tabel "cahya_Gross_Revenue_Per_Product_Per_Month"
insert_data = f"""
INSERT INTO cahya_Gross_Revenue_Per_Product_Per_Month (Product_Name, Order_Month, Gross_Revenue)
SELECT
  p.PRODUCT_NAME AS Product_Name,
  DATE_TRUNC('MONTH', ORDER_DATE) AS Order_Month,
  SUM(od.QUANTITY * p.UNIT_PRICE) AS Gross_Revenue
FROM
  cahya_orders o
  INNER JOIN cahya_order_details od ON o.ORDER_ID = od.ORDER_ID
  INNER JOIN cahya_products p ON od.PRODUCT_ID = p.PRODUCT_ID
WHERE
  YEAR(o.ORDER_DATE) = 2023
GROUP BY
  p.PRODUCT_NAME,
  DATE_TRUNC('MONTH', ORDER_DATE)  
ORDER BY
    Gross_Revenue DESC;
"""

snowflake_cursor.execute(insert_data)
snowflake_conn.commit()

