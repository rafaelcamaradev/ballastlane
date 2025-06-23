# Data Pipeline Analysis

This README explains the steps involved in the data pipeline for gathering and analyzing customer and offer data, as implemented in `pipeline.ipynb`.

## 1. Data Ingestion

The first step involves ingesting raw data from CSV files into Spark DataFrames. The following files are read:

*   `customers.csv`: Contains customer demographic information.
*   `offers.csv`: Contains details about various offers.
*   `events.csv`: Contains event logs related to customer interactions with offers.

```python
customers_df = spark.read.csv(customers_file, header=True, inferSchema=True)
offers_df = spark.read.csv(offers_file, header=True, inferSchema=True)
raw_events_df = spark.read.csv(events_file, header=True, inferSchema=True)
```

## 2. Data Preprocessing

After ingestion, the raw data undergoes several preprocessing steps to prepare it for analysis:

### 2.1. Identify Completed Offers

A new column `offer_completed` is added to the `events_df` to indicate whether an offer was completed. This is determined by checking if the `event` column has the value "offer completed".

```python
events_df = raw_events_df.withColumn(
    "offer_completed",
    when(trim(col("event")) == "offer completed", lit(1)).otherwise(lit(0))
)
```

### 2.2. Extract Offer ID from JSON

The `value` column in `events_df` contains JSON strings. The `offer_id` is extracted from these JSON strings. This involves cleaning the JSON by replacing single quotes with double quotes and then parsing the JSON to get the `offer id`.

```python
events_df = events_df.withColumn("json_details", regexp_replace("value", "'", '"'))
schema = StructType().add("offer id", StringType())
events_df = events_df.withColumn("json_parsed", from_json("json_clean", schema))
events_df = events_df.withColumn("offer_id", col("json_parsed.`offer id`"))
```

### 2.3. Convert Event Timestamps

The `time` column (Unix timestamp) in `events_df` is converted into a human-readable timestamp and datetime format for easier analysis.

```python
events_df = events_df.withColumn("event_timestamp_readable", from_unixtime(col("time")))
events_df = events_df.withColumn("event_datetime", to_timestamp(col("event_timestamp_readable")))
```

### 2.4. Join DataFrames

The preprocessed `events_df` is joined with `offers_df` (on `offer_id`) to enrich event data with offer details. Subsequently, this combined DataFrame is joined with `customers_df` (on `customer_id`) to include customer demographics, resulting in a comprehensive `full_df`.

```python
events_with_offers = events_df.join(offers_df, "offer_id", "left")
full_df = events_with_offers.join(customers_df, "customer_id", "left")
```

## 3. Analytical Questions and Value Gathering

The pipeline then proceeds to answer several analytical questions by gathering specific values from the processed DataFrames:

### 3.1. Total Number of Customers

Counts the total number of unique customers from the `customers_df`.

```python
total_customers = customers_df.count()
```

### 3.2. Total Number of Offers

Counts the total number of unique offers from the `offers_df`.

```python
total_offers = offers_df.count()
```

### 3.3. Total Number of Events

Counts the total number of events recorded in the `events_df`.

```python
total_events = events_df.count()
```

### 3.4. Number of Unique Customers Who Made a Purchase

Filters the `full_df` for 'transaction' events and counts the distinct `customer_id` to find unique purchasing customers.

```python
unique_purchasing_customers = full_df.filter(col("event") == "transaction") \
                                     .select("customer_id").distinct().count()
```

### 3.5. Top 5 Most Popular Offers

Identifies the top 5 offers based on the number of completed offers. This is done by filtering for completed offers, grouping by `offer_id` and `offer_type`, and then counting and ordering by `purchase_count`.

```python
top_5_offers = full_df.filter(col("offer_completed") == 1) \
                      .groupBy("offer_id", "offer_type") \
                      .agg(count("offer_id").alias("purchase_count")) \
                      .orderBy(col("purchase_count").desc()) \
                      .limit(5)
```

### 3.6. Average Transaction Amount

Calculates the average amount of all 'transaction' events by extracting the `amount` from the JSON in the `value` column and averaging it.

```python
average_transaction_amount = events_df.filter(col("event") == "transaction") \
    .agg(avg(get_json_object(col("value"), "$.amount").cast("double"))).collect()[0][0]
```

### 3.7. Number of Offers Completed vs. Not Completed

Groups the `events_df` by the `offer_completed` status and counts the occurrences of each status.

```python
offers_completion_status = events_df.groupBy("offer_completed") \
                                   .agg(count("offer_id").alias("count"))
```

### 3.8. Distribution of Customers by Age Group

Adds an `age_group` column to `customers_df` based on predefined age ranges and then counts the number of customers in each group.

```python
customers_df = customers_df.withColumn("age_group", \
                                     when(col("age") < 25, "<25") \
                                     .when((col("age") >= 25) & (col("age") < 35), "25-34") \
                                     .when((col("age") >= 35) & (col("age") < 45), "35-44") \
                                     .when((col("age") >= 45) & (col("age") < 55), "45-54") \
                                     .when((col("age") >= 55) & (col("age") < 65), "55-64") \
                                     .otherwise("65+"))
age_distribution = customers_df.groupBy("age_group").agg(count("customer_id").alias("customer_count")) \
                               .orderBy("age_group")
```

### 3.9. Offers with Highest Completion Rate

Calculates the completion rate for each offer by dividing the number of completed offers by the total number of events related to that offer, and then orders them by completion rate.

```python
offer_summary = full_df.groupBy("offer_id", "offer_type") \
                         .agg(count("offer_id").alias("total_events"), \
                              sum(col("offer_completed")).alias("completed_offers"))

offer_completion_rate = offer_summary.withColumn(
    "completion_rate",
    when(col("total_events") != 0, (col("completed_offers") / col("total_events")) * 100).otherwise(None)
)
highest_completion_rate_offers = offer_completion_rate.orderBy(col("completion_rate").desc()) \
                                                       .limit(5)
```

### 3.10. Customer Lifetime Value (CLV)

Calculates a simplified Customer Lifetime Value (CLV) by summing the total transaction amount for each customer.

```python
customer_clv = full_df.filter(col("event") == "transaction") \
                       .groupBy("customer_id") \
                       .agg(sum(get_json_object(col("value"), "$.amount")).alias("total_spent")) \
                       .orderBy(col("total_spent").desc())
```


