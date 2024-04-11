WITH latest_order_status AS (
  SELECT
    USER_ID,
    STATUS,
    ROW_NUMBER() OVER(PARTITION BY USER_ID ORDER BY ORDER_DATE DESC) AS rn
  FROM {{ source('jaffle_shop', 'Orders') }}
)

SELECT 
  c.ID AS CUSTOMER_ID,
  c.FIRST_NAME,
  c.LAST_NAME,
  COUNT(o.ID) AS TOTAL_ORDERS,
  MIN(o.ORDER_DATE) AS FIRST_ORDER_DATE,
  MAX(o.ORDER_DATE) AS LAST_ORDER_DATE,
  MAX(o._ETL_LOADED_AT) AS LAST_ORDER_ETL_LOADED_AT,
  los.STATUS AS LATEST_ORDER_STATUS
FROM {{ source('jaffle_shop', 'Customers') }} c
JOIN {{ source('jaffle_shop', 'Orders') }} o ON c.ID = o.USER_ID
LEFT JOIN latest_order_status los ON c.ID = los.USER_ID AND los.rn = 1
GROUP BY c.ID, c.FIRST_NAME, c.LAST_NAME, los.STATUS
