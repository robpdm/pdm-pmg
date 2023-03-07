/*
1. How many first-time customers were there from Oct 2022 - present?
2. Of these customers, how many came back for a 2nd purchase?
3. Of these customers, what did they buy?
*/

-- Isolate customer information based on 1st and 2nd order.
WITH orders AS(
    SELECT
    ARRAY_AGG(ac ORDER BY order_running_number ASC)[SAFE_OFFSET(0)] fo_customers,
    ARRAY_AGG(ac ORDER BY order_running_number ASC)[SAFE_OFFSET(1)] so_customers
    FROM `pmg-service-project-945896cb.dbt_prod.agg_customer` ac
    WHERE first_order_date BETWEEN '2022-10-01' AND '2023-02-08'
    AND trxn_type = 'order'
    GROUP BY customer_id
),

-- Create CTE that does not contain product fields to suppress duplicate records for Order ID and related, as this was throwing off avg. days between orders.
order_no_prod AS(
    SELECT DISTINCT
    fo_customers.customer_id first_order_customer_id,
    fo_customers.trxn_date first_order_date,
    fo_customers.order_id first_order_id,
    so_customers.customer_id second_order_customer_id,
    so_customers.trxn_date second_order_date,
    so_customers.order_id second_order_id,
    DATE_DIFF(a.so_customers.trxn_date,a.fo_customers.trxn_date,DAY) order_days_diff,
    FROM orders a
    LEFT JOIN `pmg-service-project-945896cb.dbt_prod.agg_customer_merchandise` b
    ON so_customers.order_id = b.trxn_id AND so_customers.trxn_ts = b.trxn_ts
    -- WHERE so_customers.customer_id IN ('1002250305605') -- Drop customer IDs here for QA.
),

-- Create CTE with product fields.
order_with_prod AS(
    SELECT DISTINCT
    fo_customers.customer_id first_order_customer_id,
    fo_customers.trxn_date first_order_date,
    fo_customers.order_id first_order_id,
    so_customers.customer_id second_order_customer_id,
    so_customers.trxn_date second_order_date,
    so_customers.order_id second_order_id,
    DATE_DIFF(a.so_customers.trxn_date,a.fo_customers.trxn_date,DAY) order_days_diff,
    b.product_id,
    b.product_title
    FROM orders a
    LEFT JOIN `pmg-service-project-945896cb.dbt_prod.agg_customer_merchandise` b
    ON so_customers.order_id = b.trxn_id AND so_customers.trxn_ts = b.trxn_ts
    -- WHERE so_customers.customer_id IN ('1002250305605') -- Drop customer IDs here for QA.  
)


-- Use to answer questions 1 + 2
SELECT
COUNT(first_order_customer_id) new_customers,
COUNT(second_order_customer_id) returning_customers,
COUNT(second_order_customer_id) / COUNT(first_order_customer_id) pct_returning_customers,
ROUND(AVG(order_days_diff),2) avg_order_days_diff
FROM order_no_prod

-- Use to answer question 3
SELECT
product_id,
product_title,
COUNT(*) orders,
COUNT(*) / SUM(COUNT(*)) OVER() pct_total_orders
FROM order_with_prod
WHERE second_order_id IS NOT NULL
GROUP BY 1,2
ORDER BY pct_total_orders DESC
