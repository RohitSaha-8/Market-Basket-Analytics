# Market Basket Analysis Data using PySpark

## Introduction:
This project involves analyzing Instacart's grocery order data to assist businesses in making strategic decisions. The dataset, consisting of millions of rows from orders, products, aisles, and departments, offers insights into customer behavior and product popularity.

## Project Steps:

### Step 1: Data Upload to HDFS via FTP
1.1 Download the relevant "insta-cart" dataset.
1.2 Upload the dataset to the FTP lab from the local system.
1.3 Use the HDFS Web Console and the "put" command to transfer the dataset to HDFS.

### Step 2: PySpark Analysis Tasks
Task 2.1: Logging into PySpark Shell
Login to the PySpark shell for further analysis.

### Task 2.2: Exploring Orders CSV
2.2.1 Read orders data as a DataFrame in PySpark, considering possible NULL values in "days_since_prior_order."
2.2.2 Display the first 10 rows of the DataFrame.

### Task 2.3: Handling NULL Values
Replace all NULL values in the DataFrame with a dummy value "999"

### Task 2.4: Busiest Day of the Week
Identify the busiest day of the week based on the "order_dow" column.
Display the total orders placed on each day of the week (Sunday to Saturday).

### Task 2.5: Busiest Hour of the Day
Select the number of order IDs as "Total_Orders" and the hour at which the order was placed.
Display the result containing the total orders and the corresponding hour.

### Task 2.6: Most Popular Item Analysis
Identify the most popular item based on the order count from order_products__prior and products datasets.
Calculate the top 10 popular items based on the count of orders.
Display the result with "Popular_Product_Name" and "Order_Count"

### Task 2.7: Department Product Analysis
Explore the department dataset, recognizing the department that published the maximum products.
Display the department ID that has the maximum product count.

## Conclusion:
By executing these tasks using PySpark, this project aims to provide valuable insights into the busiest days, hours, popular items, and prolific departments for Instacart. The outcomes will help businesses tailor their strategies to meet customer demands effectively and enhance overall service efficiency.
