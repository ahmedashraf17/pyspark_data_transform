from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, concat, lit

# Create a SparkSession
spark = SparkSession.builder.appName("TransformationExample").getOrCreate()

# Load the data
df = spark.read.csv("path/to/your/data.csv", header=True, inferSchema=True)

# Inspect the data
print("Schema:")
df.printSchema()
print("Sample Data:")
df.show(5)

# Perform data transformations

# 1. Filter the data based on a condition
filtered_df = df.where(col("age") > 30)

# 2. Group the data and calculate the average of a column
avg_salary = df.groupBy("department").agg(avg("salary").alias("avg_salary"))

# 3. Add a new column with a custom expression
transformed_df = df.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))

# 4. Handle missing values
filled_df = df.fill({"salary": 0})

# 5. Apply a conditional transformation
classified_df = df.withColumn("classification",
                             when(col("salary") < 50000, "low")
                             .when(col("salary") >= 50000, "medium")
                             .when(col("salary") >= 100000, "high")
                             .otherwise("unknown"))

# 6. Aggregate data and calculate metrics
summary_df = df.groupBy("department").agg(
    count("*").alias("total_employees"),
    avg("salary").alias("avg_salary"),
    max("salary").alias("max_salary"),
    min("salary").alias("min_salary")
)

# Save the transformed data
filtered_df.write.csv("path/to/filtered_data.csv")
avg_salary.write.csv("path/to/avg_salary.csv")
transformed_df.write.csv("path/to/transformed_data.csv")
filled_df.write.csv("path/to/filled_data.csv")
classified_df.write.csv("path/to/classified_data.csv")
summary_df.write.csv("path/to/summary_data.csv")

# Stop the SparkSession
spark.stop()
