-- Isolate customer information based on 1st and 2nd order. 
WITH orders AS(
    SELECT
    ARRAY_AGG(ac ORDER BY order_running_number ASC)[SAFE_OFFSET(0)] fo_customers,
    ARRAY_AGG(ac ORDER BY order_running_number ASC)[SAFE_OFFSET(1)] so_customers
    FROM `pmg-service-project-945896cb.dbt_prod.agg_customer` ac
    WHERE first_order_date BETWEEN '2022-10-01' AND '2023-01-31'
    AND trxn_type = 'order'
    GROUP BY customer_id
),

-- Create CTE that does not contain product fields to suppress duplicate records for Order ID and related, as this was throwing off avg. days between orders.
order_no_prod_proc AS(
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

-- Create aggregated CTE of customer info.
order_no_prod_final AS(
    SELECT
    DATE_TRUNC(a.first_order_date,MONTH) first_order_month,
    COUNT(DISTINCT a.first_order_customer_id) new_customers,
    COUNT(DISTINCT a.second_order_customer_id) / COUNT(DISTINCT a.first_order_customer_id) pct_returning_customers,
    ROUND(AVG(a.order_days_diff),2) avg_order_days_diff,
    FROM order_no_prod_proc a
    GROUP BY 1
),

-- Create CTE with product fields.
order_with_prod AS(
    SELECT DISTINCT
    DATE_TRUNC(a.fo_customers.trxn_date,MONTH) first_order_month,
    b.product_id,
    b.product_title,
    SUM(b.order_lines_quantity) quantity
    FROM orders a
    LEFT JOIN `pmg-service-project-945896cb.dbt_prod.agg_customer_merchandise` b
    ON so_customers.order_id = b.trxn_id AND so_customers.trxn_ts = b.trxn_ts -- Joining merch table on customer's second order ID because we want to return the NEXT purchased products.
    -- WHERE so_customers.customer_id IN ('1002250305605') -- Drop customer IDs here for QA.  
    GROUP BY 1,2,3
),

-- Create an array of the top three NEXT products purchased to be joined downstream.
fo_prod_array AS(
    SELECT
    first_order_month,
    ARRAY_AGG(CONCAT(product_id,' | ',REPLACE(product_title,'™','')) ORDER BY quantity DESC LIMIT 3) top_products, top_products,
    FROM order_with_prod
    GROUP BY 1
)

-- Final Query
SELECT
a.first_order_month,
a.new_customers,
a.pct_returning_customers,
a.avg_order_days_diff,
b.top_products top_products_second_purchase
FROM order_no_prod_final a
LEFT JOIN fo_prod_array b
ON a.first_order_month = b.first_order_month
ORDER BY 1 ASC
