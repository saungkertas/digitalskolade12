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

# Membuat tabel "cahya_Total_Purchase_Per_Product_Per_Month" jika belum ada
create_new_table = """
CREATE TABLE IF NOT EXISTS cahya_Total_Purchase_Per_Product_Per_Month (
    Product_Name VARCHAR,
    Order_Month DATE,
    Total_Purchase NUMBER
)
"""
snowflake_cursor.execute(create_new_table)
snowflake_conn.commit()

# Memasukkan data ke dalam tabel "cahya_Total_Purchase_Per_Product_Per_Month"
insert_data = f"""
INSERT INTO cahya_Total_Purchase_Per_Product_Per_Month (Product_Name, Order_Month, Total_Purchase)
SELECT
  p.PRODUCT_NAME AS Product_Name,
  DATE_TRUNC('MONTH', ORDER_DATE) AS Order_Month,
  SUM(od.QUANTITY) AS Total_Purchase
FROM
  cahya_orders o
  INNER JOIN cahya_order_details od ON o.ORDER_ID = od.ORDER_ID
  INNER JOIN cahya_products p ON od.PRODUCT_ID = p.PRODUCT_ID
WHERE
  YEAR(o.ORDER_DATE) = 2023
GROUP BY
  DATE_TRUNC('MONTH', ORDER_DATE),
  p.PRODUCT_NAME
ORDER BY 
  3 DESC;
"""

snowflake_cursor.execute(insert_data)
snowflake_conn.commit()

