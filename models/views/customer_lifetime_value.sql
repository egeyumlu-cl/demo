SELECT 
    c.ID AS CUSTOMER_ID,
    c.FIRST_NAME,
    c.LAST_NAME,
    SUM(p.AMOUNT) AS TOTAL_SPENT,
    COUNT(DISTINCT o.ID) AS TOTAL_ORDERS,
    AVG(p.AMOUNT) AS AVG_ORDER_VALUE
FROM {{source('jaffle_shop', 'Customers')}} c
JOIN {{source('jaffle_shop', 'Orders')}} o ON c.ID = o.USER_ID
JOIN {{source('stripe', 'Payment')}} p ON o.ID = p.ORDERID
GROUP BY c.ID, c.FIRST_NAME, c.LAST_NAME