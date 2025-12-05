# Databricks notebook source
df_silver=spark.read.format("csv")\
            .option("header","true")\
            .option("inferSchema","true")\
            .load('abfss://bronze@dqcheckstheertha.dfs.core.windows.net/part-00000-ccfc6224-03f4-4835-a75c-d76a0ed69f9a-c000.csv')

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

# ==========================================================
#        DATA QUALITY FRAMEWORK FOR df_silver
# ==========================================================

from pyspark.sql.functions import col, isnan, to_date, countDistinct
from pyspark.sql.types import IntegerType, DoubleType, FloatType, DateType, LongType, DecimalType

# Ensure df_silver exists
try:
    df = df_silver
except NameError:
    raise Exception(" df_silver not found. Ensure pipeline loads df_silver before this notebook.")

results = {}
failures = []


# ==========================================================
# 1️⃣ BASIC CHECKS
# ==========================================================

row_count = df.count()
results["row_count"] = row_count

if row_count == 0:
    failures.append(" df_silver is empty.")


# ==========================================================
# 2️⃣ NULL CHECKS by column type
# ==========================================================

required_cols = [
    "OrderID",
    "CustomerKey",
    "OrderDate",
    "ProductCode",
    "Quantity",
    "UnitPrice"
]

df_dtypes = dict(df.dtypes)

for c in required_cols:

    dtype = df_dtypes[c].lower()

    # Numeric columns → allow isnan()
    if dtype in ["int", "integer", "bigint", "double", "float", "long", "decimal"]:
        null_count = df.filter(col(c).isNull() | isnan(c)).count()

    # Non-numeric → string/date/boolean
    else:
        null_count = df.filter(col(c).isNull() | (col(c) == "")).count()

    results[f"{c}_nulls"] = null_count

    if null_count > 0:
        failures.append(f" {c} contains {null_count} null/blank values.")



# ==========================================================
# 3️⃣ DATA TYPE / VALUE VALIDATION
# ==========================================================

# --- Numeric must be > 0 ---
numeric_positive_cols = ["Quantity", "UnitPrice"]

for c in numeric_positive_cols:
    invalid = df.filter(
        (col(c).cast("double").isNull()) | 
        (col(c).cast("double") <= 0)
    ).count()
    
    results[f"{c}_invalid_values"] = invalid

    if invalid > 0:
        failures.append(f" {c} has {invalid} invalid or non-positive values.")


# --- Date validation ---
if "OrderDate" in df.columns:
    bad_dates = df.filter(to_date(col("OrderDate")).isNull()).count()

    results["OrderDate_bad"] = bad_dates

    if bad_dates > 0:
        failures.append(f" OrderDate contains {bad_dates} invalid date formats.")



# ==========================================================
# 4️⃣ DUPLICATE CHECKS
# ==========================================================

# Choose which columns define uniqueness
unique_key = ["OrderID"]

dup_count = (
    df.groupBy(unique_key)
      .count()
      .filter(col("count") > 1)
      .count()
)

results["duplicate_OrderID"] = dup_count

if dup_count > 0:
    failures.append(f" Found {dup_count} duplicate OrderID records.")



# ==========================================================
# 5️⃣ BUSINESS RULE CHECKS
# ==========================================================

# Rule: If IsErrorRow = true, row should not be processed
if "IsErrorRow" in df.columns:
    error_rows = df.filter(col("IsErrorRow") == True).count()
    results["IsErrorRow_true"] = error_rows
    if error_rows > 0:
        failures.append(f" {error_rows} rows flagged as IsErrorRow=True.")

# Rule: CustomerKey should not be invalid
if "IsInvalidCustomer" in df.columns:
    invalid_cust = df.filter(col("IsInvalidCustomer") == True).count()
    results["IsInvalidCustomer_true"] = invalid_cust
    if invalid_cust > 0:
        failures.append(f"{invalid_cust} rows flagged as invalid customers.")



# ==========================================================
# 6️⃣ FINAL OUTPUT
# ==========================================================

import json

print("\n📊 *** DATA QUALITY RESULTS ***")
print(json.dumps(results, indent=2))

if failures:
    print("\n *** DATA QUALITY FAILURES ***")
    for f in failures:
        print("-", f)
    raise Exception(" Data Quality Checks Failed.")
else:
    print("\n✔ All Data Quality Checks Passed Successfully!")


# COMMAND ----------

from pyspark.sql import Row
from datetime import datetime

dq_output = []

for k, v in results.items():
    dq_output.append(Row(
        check_name=k,
        check_value=v,
        pipeline_run_time=datetime.now().isoformat()
    ))

dq_df = spark.createDataFrame(dq_output)

dq_df.write.format("delta").mode("append").saveAsTable("dq_results")

# COMMAND ----------

dq_df.write.format("delta") \
      .mode("append") \
      .save("abfss://silver@dqcheckstheertha.dfs.core.windows.net/dq_results")