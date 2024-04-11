SELECT
    DATE_TRUNC('month', first_order_date) AS acquisition_month,
    COUNT(DISTINCT c.ID) AS new_customers
FROM
     {{ source('jaffle_shop', 'Customers') }} c
JOIN
    (SELECT
        USER_ID,
        MIN(ORDER_DATE) AS first_order_date
     FROM  {{ source('jaffle_shop', 'Orders') }} o
     GROUP BY USER_ID) o ON c.ID = o.USER_ID
GROUP BY
    DATE_TRUNC('month', first_order_date)
