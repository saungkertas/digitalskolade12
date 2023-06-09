CREATE OR REPLACE TABLE DATAMART.MONTHLY_ORDERS_COUNTRY AS(
    WITH T1 as (
        SELECT DATE_ORDER.ORDER_DATE, DATE_ORDER.CUSTOMER_ID, DETAIL.QUANTITY
        FROM PUBLIC.ORDER_DETAILS AS DETAIL
        LEFT JOIN PUBLIC.ORDERS AS DATE_ORDER
        ON DETAIL.ORDER_ID = DATE_ORDER.ORDER_ID
        WHERE DATE_ORDER.ORDER_DATE >='2023-01-01'
    ),
    T2 AS (
        SELECT T1.ORDER_DATE, CUST.COUNTRY, T1.QUANTITY
        FROM T1
        LEFT JOIN PUBLIC.CUSTOMERS AS CUST
        ON CUST.CUSTOMER_ID = T1.CUSTOMER_ID
    )
    SELECT T2.ORDER_DATE, 
        T2.COUNTRY, 
        SUM(T2.QUANTITY) AS QUANTITY
    FROM T2
    GROUP BY 1,2
);