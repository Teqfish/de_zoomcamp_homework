Question 1:
It only builds the specified model.

Question 2:
It will fail an accepted values test.

Question 3:
~~~sql
SELECT COUNT(*) AS nb_records
FROM prod.fct_monthly_zone_revenue
~~~
Answer: 12,184

Question 4:
~~~sql
SELECT
  pickup_zone,
  SUM(revenue_monthly_total_amount) AS total_revenue
FROM prod.fct_monthly_zone_revenue
WHERE revenue_month >= DATE '2020-01-01'
  AND revenue_month <  DATE '2021-01-01'
  AND service_type = 'Green'
GROUP BY pickup_zone
ORDER BY total_revenue DESC
~~~
Answer: East Harlem North

Question 5:
~~~sql
SELECT
	SUM(total_monthly_trips) as total_trips
FROM prod.fct_monthly_zone_revenue
WHERE revenue_month >= DATE '2019-10-01'
  AND revenue_month <  DATE '2019-11-01'
  AND service_type = 'Green'
~~~
Answer: 384624
