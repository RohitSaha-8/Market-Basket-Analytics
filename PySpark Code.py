• Explore the orders CSV file and create a DataFrame
• Read the orders data as a DataFrame in PySpark
• Note: The column “days_since_prior_order” may contain NULL values
• Display the data up to 10 rows
• Replace all null values with a dummy “999“ value in the DataFrame that was created in task 1
• Examine the orders CSV file and find the busiest day of the week by reading the data as a PySpark DataFrame.
• Display the result that contains the total orders placed on each day of the week (Monday to Sunday)

df_order = spark.read.format("csv").option("delimiter", ",").option("header", "true").option("inferSchema", "true").load("Market_Analysis/insta-cart/orders.csv")
from pyspark.sql.functions import col, when, expr, count
orderNew_df = df_order.withColumn("days_since_prior_order", when(col("days_since_prior_order").isNull(),"999").otherwise(col("days_since_prior_order")))
day_df = df_order.groupBy(col("order_dow").alias("Day_of_the_Week")).count().orderBy("order_dow").select(col("Day_of_the_Week"),col("count").alias("total_orders"))
DayofWeek_df = day_df.withColumn("Day of the Week", expr("CASE WHEN Day_of_the_Week = 0 THEN 'Sunday' " + "WHEN Day_of_the_Week = 1 THEN 'Monday' " + "WHEN Day_of_the_Week = 2 THEN 'Tuesday' " + "WHEN Day_of_the_Week = 3 THEN 'Wednesday' " + "WHEN Day_of_the_Week = 4 THEN 'Thursday' " + "WHEN Day_of_the_Week = 5 THEN 'Friday' " + "WHEN Day_of_the_Week = 6 THEN 'Saturday' END")).select(col("Day of the Week"),col("total_orders"))

• Give a breakdown of orders by the hour and identify the busiest hour:
• Select the number of order IDs as “Total_Orders” and the hour at which the order was placed.
• Display the result that contains total orders and the hour.

Hour_df = df_order.groupBy(col("order_hour_of_day").alias("Hour")).count().orderBy(col("Hour")).select(col("Hour"),col("count").alias("Total_Orders"))
BusiestHour_df = Hour_df.orderBy(col("Total_Orders").desc()).limit(1)

• Identify the most popular item based on the order count by exploring order_products__prior and products datasets.
• Calculate the top 10 popular items based on the count of orders.
• Display the result that contains the product name as “Popular_product_name” and the count of order id as “Order_Count”.

products_prior_df = spark.read.format("csv").option("delimiter", ",").option("header", "true").option("inferSchema", "true").load("Market_Analysis/insta-cart/order_products__prior.csv")
products_df = spark.read.format("csv").option("delimiter", ",").option("header", "true").option("inferSchema", "true").load("Market_Analysis/insta-cart/products.csv")
popular_df = products_prior_df.join(products_df, "product_id").groupBy(col("product_name").alias("popular_product_name")).agg(count(col("order_id")).alias("order_count")).orderBy(col("order_count").desc()).limit(10)

• Explore the department dataset and create a DataFrame.
• Recognize the department which has published the maximum products.
• Display the department ID that has published the maximum products.

department_df = spark.read.format("csv").option("delimiter", ",").option("header", "true").option("inferSchema", "true").load("Market_Analysis/insta-cart/departments.csv")
max_prod_dept_df = products_df.join(department_df, "department_id").groupBy(col("department_id"), col("department")).agg(count(col("product_name")).alias("max_products")).orderBy(col("max_products").desc())