import os
import sys

# Remove old standalone Spark configuration
os.environ.pop("SPARK_HOME", None)

# Configure Python
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Configure Java
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-23"

os.environ["PATH"] = (
    os.path.join(os.environ["JAVA_HOME"], "bin")
    + os.pathsep
    + os.environ["PATH"]
)

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("BikeSharingAnalysis")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

spark = (
    SparkSession.builder
    .appName("BikeSharingAnalysis")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("BikeSharingAnalysis") \
    .master("local[*]") \
    .getOrCreate()

# Reduce Spark's noisy logs (keeps your console clean)
spark.sparkContext.setLogLevel("ERROR")

print("--- 1. DATA LOADING ---")
# Load the CSV (Ensure bike_sharing.csv is in the same folder as this script!)
df = spark.read.csv("bike_sharing.csv", header=True, inferSchema=True)

print("\n[Schema]")
df.printSchema()

print("\n[First 5 rows]")
df.show(5, truncate=False)

print("\n[Total rentals]")
print(f"Total rentals: {df.count()}")

# 2. Create Temporary View
df.createOrReplaceTempView("bike_rentals_view")
print("\nTemporary view 'bike_rentals_view' created.")

# 3. Basic SQL Queries
print("\n--- 3. BASIC SQL QUERIES ---")

print("\n[Q3.1 Rentals > 30 minutes]")
spark.sql("SELECT * FROM bike_rentals_view WHERE duration_minutes > 30").show()

print("\n[Q3.2 Rentals starting at Station A]")
spark.sql("SELECT * FROM bike_rentals_view WHERE start_station = 'Station A'").show()

print("\n[Q3.3 Total Revenue]")
spark.sql("SELECT SUM(price) AS total_revenue FROM bike_rentals_view").show()

# 4. Aggregation Queries
print("\n--- 4. AGGREGATION QUERIES ---")

print("\n[Q4.1 Rentals per start station]")
spark.sql("""
    SELECT start_station, COUNT(*) AS total_rentals 
    FROM bike_rentals_view 
    GROUP BY start_station 
    ORDER BY total_rentals DESC
""").show()

print("\n[Q4.2 Average duration per start station]")
spark.sql("""
    SELECT start_station, ROUND(AVG(duration_minutes), 2) AS avg_duration 
    FROM bike_rentals_view 
    GROUP BY start_station
""").show()

print("\n[Q4.3 Station with highest number of rentals]")
spark.sql("""
    SELECT start_station, COUNT(*) AS total_rentals 
    FROM bike_rentals_view 
    GROUP BY start_station 
    ORDER BY total_rentals DESC 
    LIMIT 1
""").show()

# 5. Time-Based Analysis
print("\n--- 5. TIME-BASED ANALYSIS ---")

print("\n[Q5.1 & 5.2 Rentals per hour (Peak hours)]")
spark.sql("""
    SELECT HOUR(start_time) AS rental_hour, COUNT(*) AS total_rentals 
    FROM bike_rentals_view 
    GROUP BY HOUR(start_time) 
    ORDER BY total_rentals DESC
""").show()

print("\n[Q5.3 Most popular morning station (7am - 12pm)]")
spark.sql("""
    SELECT start_station, COUNT(*) AS morning_rentals 
    FROM bike_rentals_view 
    WHERE HOUR(start_time) BETWEEN 7 AND 12 
    GROUP BY start_station 
    ORDER BY morning_rentals DESC 
    LIMIT 1
""").show()

# 6. User Behavior Analysis
print("\n--- 6. USER BEHAVIOR ANALYSIS ---")

print("\n[Q6.1 Average age of users]")
spark.sql("SELECT ROUND(AVG(age), 1) AS average_age FROM bike_rentals_view").show()

print("\n[Q6.2 Count users by gender]")
spark.sql("""
    SELECT gender, COUNT(DISTINCT user_id) AS unique_users 
    FROM bike_rentals_view 
    GROUP BY gender
""").show()

print("\n[Q6.3 Rentals by age group]")
spark.sql("""
    SELECT 
        CASE 
            WHEN age BETWEEN 18 AND 30 THEN '18-30'
            WHEN age BETWEEN 31 AND 40 THEN '31-40'
            WHEN age BETWEEN 41 AND 50 THEN '41-50'
            WHEN age >= 51 THEN '51+'
            ELSE 'Under 18'
        END AS age_group,
        COUNT(*) AS total_rentals
    FROM bike_rentals_view 
    GROUP BY age_group
    ORDER BY total_rentals DESC
""").show()

# Stop Spark gracefully
spark.stop()