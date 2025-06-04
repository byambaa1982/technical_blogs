# technical_blogs
I am a Databricks Data Engineer Professional. Here, I will share my expertise, insights, and experiences to help others learn and grow in the field of data engineering.

- [Read about SCD Type 2 Tables](https://github.com/byambaa1982/technical_blogs/tree/main/scd_type_2_table)

```python

df.show()

+----------+-------+------------+
| sale_date|product|sales_amount|
+----------+-------+------------+
|2025-06-01|      A|         100|
|2025-06-01|      B|         150|
|2025-06-02|      A|         200|
|2025-06-02|      B|         250|
|2025-06-03|      A|         300|
+----------+-------+------------+

windowSpec = Window.partitionBy("product").orderBy("sale_date")
df_with_running_total = df.withColumn("running_total", sum("sales_amount").over(windowSpec))
df_with_running_total.show()


+----------+-------+------------+-------------+
| sale_date|product|sales_amount|running_total|
+----------+-------+------------+-------------+
|2025-06-01|      A|         100|          100|
|2025-06-02|      A|         200|          300|
|2025-06-03|      A|         300|          600|
|2025-06-01|      B|         150|          150|
|2025-06-02|      B|         250|          400|
+----------+-------+------------+-------------+


```
```sql


SELECT
    sale_date,
    product,
    sales_amount,
    SUM(sales_amount) OVER (PARTITION BY product ORDER BY sale_date) AS running_total
FROM sales
ORDER BY product, sale_date;


```