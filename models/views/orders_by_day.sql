SELECT
    DATE_TRUNC('day', o.ORDER_DATE) AS order_date,
    COUNT(o.ID) AS total_orders,
    SUM(p.AMOUNT) AS total_revenue,
    AVG(p.AMOUNT) AS average_order_value
  FROM {{ source('jaffle_shop', 'Orders') }} o
  LEFT JOIN {{ source('stripe', 'Payment') }} p ON o.ID = p.ORDERID
  GROUP BY DATE_TRUNC('day', o.ORDER_DATE)