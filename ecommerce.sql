/* finding the different status of the active orders*/
--Different order status
SELECT
order_status,
COUNT(*) AS orders,
ROUND(COUNT(*) / SUM(COUNT(*)) OVER () * 100, 3) || '%' AS pct_of_orders
FROM
`jrjames83-1171.sampledata.orders`
GROUP BY
1
ORDER BY
2 DESC


/* How many orders made per each year? */
-- Orders by year
SELECT
EXTRACT(YEAR
FROM
order_purchase_timestamp) AS year,
COUNT(*) AS orders
FROM
`jrjames83-1171.sampledata.orders`
GROUP BY
year
ORDER BY
year DESC


/* Orders Monthly Performance*/
-- Orders Monthly Performance
SELECT
EXTRACT(YEAR FROM order_purchase_timestamp) AS Year,
EXTRACT(MONTH FROM order_purchase_timestamp) AS Month,
COUNT(*) AS orders
FROM
`jrjames83-1171.sampledata.orders`
GROUP BY
Year,Month
ORDER BY
Year DESC, Month ASC

-- Orders Monthly Performance with month name
SELECT
EXTRACT(YEAR FROM order_purchase_timestamp) AS Year,
FORMAT_DATE('%b',order_purchase_timestamp) AS Month,
COUNT(*) AS orders
FROM
`jrjames83-1171.sampledata.orders`
GROUP BY
Year,Month
ORDER BY
Year, Month

/* How many are returning customers?*/
SELECT
COUNT(c.customer_unique_id) - COUNT(DISTINCT c.customer_unique_id) AS
repeating_purchase_customers
FROM
`jrjames83-1171.sampledata.orders` AS o
JOIN
jrjames83-1171.sampledata.customers AS c
ON
c.customer_id = o.customer_id;

/* Getting A list of returning custmers*/
-- Getting the list of repeating-orders customers
WITH
t AS (
SELECT
c.customer_unique_id,
ROW_NUMBER() OVER(PARTITION BY c.customer_unique_id ORDER BY order_purchase_timestamp)
AS order_number
FROM
`jrjames83-1171.sampledata.orders` AS o
JOIN
`jrjames83-1171.sampledata.customers` AS c
ON
c.customer_id = o.customer_id
ORDER BY
1,
2 )
SELECT
t.customer_unique_id
FROM
t
WHERE
t.order_number > 1

/* Top 5 cities in terms of number of orders made?*/
SELECT
c.customer_city AS city,
COUNT(o.order_id) AS n_orders
FROM
`jrjames83-1171.sampledata.orders` AS o
JOIN
`jrjames83-1171.sampledata.customers` AS c
ON
c.customer_id = o.customer_id
GROUP BY
city
ORDER BY
n_orders DESC
LIMIT
5

/* How many new cusotmers where acquired each by year and month?*/
WITH
t AS (
SELECT
c.customer_unique_id AS customer,
o.order_purchase_timestamp AS purchase_date,
ROW_NUMBER() OVER(PARTITION BY c.customer_unique_id ORDER BY order_purchase_timestamp)
AS order_number
FROM
`jrjames83-1171.sampledata.orders` AS o
JOIN
`jrjames83-1171.sampledata.customers` AS c
ON
c.customer_id = o.customer_id
ORDER BY
1,
2,
3)
SELECT
EXTRACT( YEAR
FROM
t.purchase_date) AS year,
EXTRACT( MONTH
FROM
t.purchase_date) AS month,
COUNT(*) AS acquired_customers
FROM
t
WHERE
order_number > 1
GROUP BY
year,
month
ORDER BY
year DESC,
month DESC

/* How can we filter the table for the customer who made only one order?*/
-- Filtering out non repeating_order customers
WITH
base_table AS(
SELECT
c.customer_unique_id AS customer,
o.order_purchase_timestamp AS purchase_date,
ROW_NUMBER() OVER(PARTITION BY c.customer_unique_id ORDER BY o.order_purchase_timestamp)
AS order_number
FROM
`jrjames83-1171.sampledata.orders` AS o
JOIN
`jrjames83-1171.sampledata.customers` AS c
ON
c.customer_id = o.customer_id
ORDER BY
1,
2 ),
exclude_these AS(
  SELECT
customer,
MAX(order_number)
FROM
base_table
GROUP BY
1
HAVING
MAX(order_number) = 1 )
SELECT
*
FROM
base_table
WHERE
base_table.customer NOT IN (
SELECT
customer
FROM
exclude_these)
ORDER BY
1,
3

/* When was the first time a product got ordered multiple times?*/
-- product nth occurence
SELECT
oi.product_id AS product,
o.order_purchase_timestamp AS order_date,
ROW_NUMBER() OVER (PARTITION BY oi.product_id ORDER BY o.order_purchase_timestamp) AS
product_nth_occurence
FROM
`jrjames83-1171.sampledata.orders` AS o
JOIN
`jrjames83-1171.sampledata.order_items` AS oi
ON
o.order_id = oi.order_id
ORDER BY
1,
3

/* Days between orders of a specific product*/
WITH
base_table AS(
SELECT
oi.product_id AS product,
o.order_purchase_timestamp AS order_date,
ROW_NUMBER()
OVER (PARTITION BY oi.product_id ORDER BY o.order_purchase_timestamp)
AS product_nth_occurence,
LAG(o.order_purchase_timestamp)
OVER (PARTITION BY oi.product_id ORDER BY o.order_purchase_timestamp)
AS prev_order_date
FROM
`jrjames83-1171.sampledata.orders` AS o
JOIN
`jrjames83-1171.sampledata.order_items` AS oi
ON
o.order_id = oi.order_id
ORDER BY
1,
3)
SELECT
bt.*,
DATE_DIFF(DATE(bt.order_date),
DATE(bt.prev_order_date),
DAY) AS days_between_orders_for_product
FROM
base_table AS bt

/*Average days between orders of a specific product*/
WITH
base_table AS(
SELECT
oi.product_id AS product,
o.order_purchase_timestamp AS order_date,
ROW_NUMBER()
OVER (PARTITION BY oi.product_id ORDER BY o.order_purchase_timestamp)
AS product_nth_occurence,
LAG(o.order_purchase_timestamp)
OVER (PARTITION BY oi.product_id ORDER BY o.order_purchase_timestamp)
AS prev_order_date
FROM
`jrjames83-1171.sampledata.orders` AS o
JOIN
`jrjames83-1171.sampledata.order_items` AS oi
ON
o.order_id = oi.order_id
ORDER BY
1,
3), t AS(
SELECT
bt.*,
DATE_DIFF(DATE(bt.order_date),
DATE(bt.prev_order_date),
DAY) AS days_between_orders_for_product
FROM
base_table AS bt
)
SELECT
t.product,
ROUND(AVG(t.days_between_orders_for_product), 1)
AS avg_days_between_orders,
COUNT(*) AS times_ordered
FROM t
WHERE t.days_between_orders_for_product IS NOT NULL
GROUP BY 1
ORDER BY 3 DESC

/*Average Days Between Orders for Returning Customers*/
WITH
base_table AS
(
SELECT
c.customer_unique_id AS customer,
o.order_purchase_timestamp AS purchase_date,
ROW_NUMBER() OVER(PARTITION BY c.customer_unique_id ORDER BY o.order_purchase_timestamp)
AS order_number,
LAG(o.order_purchase_timestamp) OVER (PARTITION BY c.customer_unique_id
ORDER BY o.order_purchase_timestamp) AS prev_customer_order_date
FROM
`jrjames83-1171.sampledata.orders` AS o
JOIN
`jrjames83-1171.sampledata.customers` AS c
ON
c.customer_id = o.customer_id
ORDER BY
1,
2 )
, exclude_these AS(
SELECT
customer,
MAX(order_number)
FROM
base_table
GROUP BY
1
HAVING
MAX(order_number) = 1
)
SELECT
bt.order_number,
ROUND(AVG(DATE_DIFF(bt.purchase_date, prev_customer_order_date, DAY)), 1) AS
avg_days_between_orders_returning_customers,
COUNT(DISTINCT bt.customer) AS count_unique_customers
FROM
base_table AS bt
WHERE
bt.customer NOT IN (
SELECT
customer
FROM
exclude_these)
GROUP BY 1
ORDER BY 1

/* Hourly Revenue Profile - Aggregated Values*/
SELECT
EXTRACT( HOUR
FROM
o.order_purchase_timestamp) AS hour,
ROUND(SUM(op.payment_value), 2) AS sales,
FROM
`jrjames83-1171.sampledata.orders` AS o
JOIN
`jrjames83-1171.sampledata.order_payments` AS op
ON
op.order_id = o.order_id
GROUP BY
1
ORDER BY
1

/* Hourly Revenue Profile - Non-Aggregated Values*/
SELECT
DATE_TRUNC(o.order_purchase_timestamp, HOUR) AS hour,
ROUND(SUM(op.payment_value), 2) AS sales,
FROM
`jrjames83-1171.sampledata.orders` AS o
JOIN
`jrjames83-1171.sampledata.order_payments` AS op
ON
op.order_id = o.order_id
GROUP BY
1
ORDER BY
1

/* Sales By Daytime*/
-- Sales by day times
WITH
hourly_sales AS (
SELECT
EXTRACT(HOUR
FROM
o.order_purchase_timestamp) AS hour,
ROUND(SUM(op.payment_value), 2) AS sales,
FROM
`jrjames83-1171.sampledata.orders` AS o
JOIN
`jrjames83-1171.sampledata.order_payments` AS op
ON
op.order_id = o.order_id
GROUP BY
1
ORDER BY
1)
SELECT
CASE
WHEN hour BETWEEN 6 AND 11 THEN 'Morning'
WHEN hour BETWEEN 12
AND 16 THEN 'Afternoon'
WHEN hour BETWEEN 17 AND 22 THEN 'Evening'
WHEN hour BETWEEN 0
AND 5
OR hour = 23 THEN 'Overnight'
ELSE
'Check_the_logic'
END
AS day_time,
SUM(sales) AS total_sales
FROM
hourly_sales
GROUP BY
1

/* Another way of using CASE to simulate SUMIF() function in Excel*/
-- Alternative CASE statement for simulating SUMIF()
WITH
hourly_sales AS (
SELECT
EXTRACT(HOUR
FROM
o.order_purchase_timestamp) AS hour,
ROUND(SUM(op.payment_value), 2) AS sales,
FROM
`jrjames83-1171.sampledata.orders` AS o
JOIN
`jrjames83-1171.sampledata.order_payments` AS op
ON
op.order_id = o.order_id
GROUP BY
1
ORDER BY
1)
SELECT
SUM(CASE WHEN hour BETWEEN 6 AND 11 THEN sales ELSE 0 END) AS morning_sales,
SUM(CASE WHEN hour BETWEEN 12 AND 16 THEN sales ELSE 0 END) AS afternoon_sales,
SUM(CASE WHEN hour BETWEEN 17 AND 22 THEN sales ELSE 0 END) AS evening_sales,
SUM(CASE WHEN hour BETWEEN 0 AND 5 OR hour = 23 THEN sales ELSE 0 END) AS overnight_sales
FROM
hourly_sales
