# How many unique nodes are there on the Data Bank system?
SELECT COUNT(DISTINCT node_id) AS unique_nodes
FROM skillful-eon-393010.data_bank.customer_node

# What is the number of nodes per region?
SELECT 
  regions.region_name,
  COUNT(DISTINCT customers.node_id) AS node_count
FROM skillful-eon-393010.data_bank.region AS regions
JOIN skillful-eon-393010.data_bank.customer_node AS customers
  ON regions.region_id = customers.region_id
GROUP BY regions.region_name;

 # How many customers are allocated to each region?
SELECT
  region_id,
  COUNT(customer_id) AS customer_count
FROM skillful-eon-393010.data_bank.customer_node
GROUP BY region_id
ORDER BY region_id;

# How many days on average are customers reallocated to a different node 
WITH node_days AS (
  SELECT 
    customer_id,
    node_id,
    DATE_DIFF (end_date, start_date, DAY) AS days_in_node
  FROM skillful-eon-393010.data_bank.customer_node
  GROUP BY customer_id, node_id, start_date, end_date
),
total_node_days AS (
  SELECT
    customer_id,
    node_id,
    SUM(days_in_node) AS total_days_in_node
  FROM node_days
  GROUP BY customer_id, node_id
)
SELECT ROUND(AVG(total_days_in_node)) AS avg_node_reallocation_days
FROM total_node_days;

# What is the unique count and total amount for each transaction type?
SELECT 
  txn_type,
  COUNT(customer_id) AS transaction_count,
  SUM(txn_amount) AS total_amount
FROM skillful-eon-393010.data_bank.customer_transactions
GROUP BY txn_type

# What is the average total historical deposit counts and amounts for all customers? (gtw kenapa error)
WITH deposits AS (
  SELECT 
    customer_id, 
    COUNT(customer_id) AS txn_count, 
    AVG(txn_amount) AS avg_amount
  FROM skillful-eon-393010.data_bank.customer_transactions
  WHERE txn_type = ' deposit'
  GROUP BY customer_id
)
SELECT 
  ROUND(AVG(txn_count)) AS avg_deposit_count, 
  ROUND(AVG(avg_amount)) AS avg_deposit_amt
FROM deposits;

# For each month - how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month?
WITH monthly_transactions AS(
  SELECT
    customer_id,
    DATE_TRUNC(txn_date, MONTH) AS month,
    SUM(CASE WHEN txn_type = ' deposit' THEN 0 ELSE 1 END) AS deposit_count,
    SUM(CASE WHEN txn_type = ' purchase' THEN 0 ELSE 1 END) AS purchase_count,
    SUM(CASE WHEN txn_type = ' withdrawal' THEN 0 ELSE 1 END) AS withdrawal_count
  FROM skillful-eon-393010.data_bank.customer_transactions
  GROUP BY customer_id, month
SELECT
  month,
  COUNT(DISTINCT customer_id) AS customer_count
FROM monthly_transactions
WHERE deposit_count>1
  AND (purchase_count >=1 OR withdrawal_count >=1)
GROUP BY month
ORDER BY month;

# What is the closing balance for each customer at the end of the month? Also show the change in balance each month in the same table output.(kok hasilnya minus semua - case closed (ternyata ada spasinya))
WITH customer_funds AS(
  SELECT
    customer_id,
    EXTRACT(MONTH FROM txn_date) AS month_number,
    SUM(CASE WHEN txn_type = ' deposit' THEN txn_amount ELSE -1 * txn_amount END) AS total_amount
  FROM skillful-eon-393010.data_bank.customer_transactions
  GROUP BY customer_id,month_number
  ORDER BY customer_id
)
SELECT
  customer_id,
  month_number,
  SUM(total_amount) OVER(PARTITION BY customer_id ORDER BY month_number) AS closing_balance
FROM customer_funds;

# The Data Bank team wanted to run an experiment where different groups of customers would be allocated data using 3 different options

--  running customer balance column that includes the impact each transaction
WITH amounts AS(
  SELECT
    customer_id,
    EXTRACT(MONTH FROM txn_date) as month_num,
    txn_type,
    txn_amount,
    CASE WHEN txn_type = ' deposit' THEN txn_amount ELSE -1*txn_amount END AS total_amount
  FROM skillful-eon-393010.data_bank.customer_transactions
  ORDER BY customer_id
)
SELECT
  customer_id,
  month_num,
  total_amount,
  SUM(total_amount) OVER (PARTITION BY customer_id, month_num ORDER BY month_num ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_balance
FROM amounts;

-- customer balance at the end of each month
SELECT
  customer_id,
  EXTRACT(MONTH FROM txn_date) as month_num,
  SUM (CASE WHEN txn_type = ' deposit' THEN txn_amount ELSE -1 * txn_amount END) AS monthly_balance
FROM skillful-eon-393010.data_bank.customer_transactions
GROUP BY 1,2
ORDER BY 1

-- minimum, average and maximum values of the running balance for each customer
WITH amounts AS(
  SELECT 
    customer_id, 
    EXTRACT(MONTH FROM txn_date) as month_num,
    txn_type, 
    txn_amount, 
    CASE WHEN txn_type = ' deposit' THEN txn_amount ELSE -1 * txn_amount END AS total_amount
  FROM skillful-eon-393010.data_bank.customer_transactions
  ORDER BY customer_id),

balances AS (
  SELECT 
    customer_id, 
    month_num, 
    total_amount, 
    SUM(total_amount) OVER (PARTITION BY customer_id, month_num ORDER BY month_num ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
  FROM amounts)

SELECT 
  customer_id, 
  MIN(running_balance) as min_bal, 
  ROUND(AVG(running_balance),2) AS avg_bal, 
  MAX(running_balance) AS max_bal
  FROM balances
  GROUP BY 1
  ORDER BY 1

-- OPTION 1 monthly allocation by amount (total allocation nya 0 knp ya soalnya total amount nya minus bro, kemungkinan ga bisa baca string txn type nya)
WITH amounts AS(
  SELECT 
    customer_id, 
    EXTRACT(MONTH FROM txn_date) as month_num,
    FORMAT_DATETIME("%B", DATETIME(txn_date)) AS month_name,
    txn_type, 
    txn_amount, 
    CASE WHEN txn_type = ' deposit' THEN txn_amount ELSE -1 * txn_amount END AS total_amount
  FROM skillful-eon-393010.data_bank.customer_transactions
  ORDER BY customer_id),

balances AS (
  SELECT 
    customer_id, 
    month_num,
    month_name, 
    total_amount, 
    SUM(total_amount) OVER (PARTITION BY customer_id, month_num ORDER BY month_num ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
  FROM amounts),

allocations AS(
  SELECT *,
    LAG(running_balance,1) OVER(PARTITION BY customer_id ORDER BY customer_id, month_num) AS monthly_allocation
  FROM balances
)

SELECT 
  month_num,
  month_name,
  SUM(CASE WHEN monthly_allocation <0 THEN 0 ELSE monthly_allocation END) AS total_allocation
FROM allocations
GROUP BY 1,2
ORDER BY 1,2;

-- OPTION 2 allocation by average 
WITH amounts AS(
  SELECT 
    customer_id, 
    EXTRACT(MONTH FROM txn_date) as month_num,
    FORMAT_DATETIME("%B", DATETIME(txn_date)) AS month_name,
    txn_type, 
    txn_amount, 
    CASE WHEN txn_type = ' deposit' THEN txn_amount ELSE -1 * txn_amount END AS total_amount
  FROM skillful-eon-393010.data_bank.customer_transactions
  ORDER BY customer_id),

balances AS (
  SELECT 
    customer_id, 
    month_num,
    month_name, 
    total_amount, 
    SUM(total_amount) OVER (PARTITION BY customer_id, month_num ORDER BY month_num ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
  FROM amounts),

avg_running AS(
  SELECT
    customer_id,
    month_num,
    month_name,
    ROUND(AVG(running_balance),2) AS avg_bal
  FROM balances
  GROUP BY 1,2,3
  ORDER BY 1
)

SELECT
  month_num,
  month_name,
  SUM(CASE WHEN avg_bal < 0 THEN 0 ELSE avg_bal END) AS total_allocation
FROM avg_running
GROUP BY 1,2
ORDER BY 1,2

-- OPTION 3 data updated real time
WITH amounts AS(
  SELECT 
    customer_id, 
    EXTRACT(MONTH FROM txn_date) as month_num,
    FORMAT_DATETIME("%B", DATETIME(txn_date)) AS month_name,
    txn_type, 
    txn_amount, 
    CASE WHEN txn_type = ' deposit' THEN txn_amount ELSE -1 * txn_amount END AS total_amount
  FROM skillful-eon-393010.data_bank.customer_transactions
  ORDER BY customer_id),

balances AS (
  SELECT 
    customer_id, 
    month_num,
    month_name, 
    total_amount, 
    SUM(total_amount) OVER (PARTITION BY customer_id, month_num ORDER BY month_num ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
  FROM amounts)

SELECT
  month_num,
  month_name,
  SUM(CASE WHEN running_balance < 0 THEN 0 ELSE running_balance END) AS total_allocation
FROM balances
GROUP BY 1,2
ORDER BY 1,2

# allocation using interest rate of 6% pa (sama hasilnya 0)
WITH monthly_balances AS(
  SELECT 
    customer_id, 
    EXTRACT(MONTH FROM txn_date) as month_num,
    FORMAT_DATETIME("%B", DATETIME(txn_date)) AS month_name,
    txn_type, 
    txn_amount, 
    CASE txn_type
      WHEN ' deposit' THEN txn_amount 
      WHEN ' purchase' THEN -1*txn_amount
      WHEN ' withdrawal' THEN -1*txn_amount
      ELSE 0
      END AS monthly_balance
  FROM skillful-eon-393010.data_bank.customer_transactions
  ORDER BY customer_id),

interest_earned AS(
  SELECT *,
  ROUND (((monthly_balance *6*1)/(100.0*12)),2) AS interest
  FROM monthly_balances
  -- GROUP BY customer_id, month_num, month_name, monthly_balances
  ORDER BY customer_id, month_num, month_name
  ),

total_earnings AS(
  SELECT
    customer_id,
    month_num,
    month_name,
    (monthly_balance + interest) AS earnings
  FROM interest_earned
  GROUP BY 1,2,3,4
  ORDER BY 1,2,3
)

SELECT
  month_num,
  month_name,
  SUM(CASE WHEN earnings < 0 THEN 0 ELSE earnings END) as allocation
  FROM total_earnings
  GROUP BY 1,2
  ORDER BY 1,2;

