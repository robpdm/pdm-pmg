/*
Q1: How many customers have we aquired through the specified SKU categories?
Q2: Of these customers, how many come back for a second purchase from the specified SKU categories?
Q3: What's the average days between purchases:
Q4: Of the customers coming back for a second purchase from the specified SKU categories, what SKUs are they buying the most?
*/

-- CTE to join customer-level data with purchase-level data.
WITH customer_orders AS(
    SELECT
    a.customer_id,
    a.trxn_date order_date,
    a.order_id,
    a.order_name,
    b.product_title,
    b.product_id,
    b.sku product_sku,
    a.order_increment,
    -- Group SKUs into categoroes provided by Miranda.
    CASE
        WHEN b.sku IN('19-001-487',	'39-001-1598',	'39-001-1599',	'39-001-1600',	'32-017-055',	'20-020-482',	'20-020-486') THEN 'Lunar New Year'
        WHEN b.sku IN('19-001-496',	'19-001-496',	'19-001-497',	'10-081-077',	'39-001-1616',	'19-001-1604',	'19-001-498',	'19-001-1606',	'19-001-1605',	'19-001-1603',	'10-081-074',	'10-081-076',	'19-001-1619',	'39-001-1613',	'10-081-080',	'10-081-079',	'39-001-1615',	'33-067-337',	'19-001-1617',	'39-001-1614',	'32-017-056',	'19-001-1618',	'33-067-336',	'29-001-1608',	'39-001-1609',	'23-034-245',	'10-081-078',	'10-081-075',	'23-034-244') THEN 'Love Collection'
        WHEN b.sku IN('20-075-649',	'20-075-649',	'20-075-652',	'20-075-654',	'20-075-650',	'20-075-657',	'20-075-648',	'20-075-655',	'20-075-651',	'20-075-653',	'20-075-654',	'20-075-656') THEN 'Satin Allure - New Shades'
        WHEN b.sku IN('19-001-489',	'33-068-316',	'33-067-338',	'39-001-1637',	'39-001-1639',	'39-001-1640',	'33-068-317',	'33-068-321',	'33-068-318',	'29-001-1638',	'39-001-1642',	'39-001-1645',	'33-068-323',	'33-068-320',	'33-068-324',	'39-001-1641',	'39-001-1643',	'22-038-632',	'39-001-1644',	'39-001-1635',	'39-001-1636',	'20-020-192',	'24-006-329',	'33-068-325') THEN 'Bronze Collection'
        ELSE 'Other'
    END product_sku_category
    FROM `pmg-service-project-945896cb.dbt_prod.agg_customer` a
    INNER JOIN `pmg-service-project-945896cb.dbt_prod.agg_customer_merchandise` b
    ON a.order_id = b.trxn_id
    WHERE a.trxn_type = 'order'
),

-- Isolate new customers that are buying from the specified SKUs.
customer_first_order AS(
    SELECT
    customer_id,
    order_date,
    order_id,
    product_sku,
    product_sku_category
    FROM customer_orders
    WHERE order_increment = 1
    AND product_sku_category != 'Other'
),

-- Isolate returning customers that are buying from the specified SKUs.
customer_second_order AS(
    SELECT
    customer_id,
    order_date,
    order_id,
    product_sku,
    product_sku_category
    FROM customer_orders
    WHERE order_increment = 2
    AND product_sku_category != 'Other'
),

-- Join new and returning customer data for singular view.
customer_final AS(
    SELECT
    a.customer_id customer_id_first_order,
    a.order_date first_order_date,
    a.order_id first_order_id,
    a.product_sku first_product_sku,
    a.product_sku_category first_product_sku_category,
    b.customer_id customer_id_second_order,
    b.order_date second_order_date,
    b.order_id second_order_id,
    b.product_sku second_product_sku,
    b.product_sku_category second_product_sku_category,
    DATE_DIFF(b.order_date,a.order_date,DAY) days_diff_orders
    FROM customer_first_order a
    LEFT JOIN customer_second_order b
    ON a.customer_id = b.customer_id
)

/*
-- Query to answer questions 1-3.
SELECT
first_product_sku_category acqusition_product_sku_category,
COUNT(DISTINCT customer_id_first_order) new_customers,
COUNT(DISTINCT customer_id_second_order) returning_customers,
ROUND(AVG(days_diff_orders),1) avg_days_diff_orders
FROM customer_final
WHERE first_order_date BETWEEN '2023-01-01' AND '2023-04-08'
GROUP BY 1
ORDER BY 3 DESC
*/



-- Query to answer question 4.
SELECT
second_product_sku returning_product_sku,
second_product_sku_category returning_product_sku_cateogry,
COUNT(DISTINCT customer_id_second_order) returning_customers
FROM customer_final
WHERE first_order_date BETWEEN '2023-01-01' AND '2023-04-08'
GROUP BY 1,2
ORDER BY 3 DESC
