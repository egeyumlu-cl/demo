SELECT
    DATE_TRUNC('month', MIN(o.ORDER_DATE)) AS acquisition_month,
    COUNT(DISTINCT c.ID) AS new_customers
  FROM {{ source('jaffle_shop', 'Customers') }} c
  JOIN {{ source('jaffle_shop', 'Orders') }} o ON c.ID = o.USER_ID
  GROUP BY DATE_TRUNC('month', MIN(o.ORDER_DATE))