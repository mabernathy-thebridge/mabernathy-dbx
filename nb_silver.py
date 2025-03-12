# Databricks notebook source
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# COMMAND ----------

# DBTITLE 1, Notebook Parameters
dbutils.widgets.dropdown("should_test", "false", ["true", "false"])

# COMMAND ----------

should_test = dbutils.widgets.get("should_test").lower() == "true"

# COMMAND ----------

# Function to read SQL file content
def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

# COMMAND ----------

# Get list of SQL files in silver config folder
silver_path = "./configs/silver"
sql_files = [f for f in os.listdir(silver_path) if f.endswith('.sql')]

# COMMAND ----------

# Function to process a single SQL file and create Delta table
def process_sql_file(sql_file):
    try:
        # Get the table name from file name (remove .sql extension)
        table_name = sql_file.replace('.sql', '')
        
        # Read SQL content
        sql_content = read_sql_file(os.path.join(silver_path, sql_file))
        
        # Create temporary view with unique name to avoid conflicts
        temp_view_name = f"temp_view_{table_name}"
        df_sql = spark.sql(sql_content)
        df_sql.createOrReplaceTempView(temp_view_name)
        
        # Create Delta table
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS practice_sandbox.ma_sandbox.silver_{table_name}
        USING DELTA
        AS
        SELECT * FROM {temp_view_name}
        """)
        df_count = df_sql.count()
        # Clean up temporary view
        spark.sql(f"DROP VIEW IF EXISTS {temp_view_name}")
        
        return f"Successfully created Delta table: practice_sandbox.ma_sandbox.silver_{table_name} with {df_count} rows"
    
    except Exception as e:
        return f"Error processing {sql_file}: {str(e)}"

# COMMAND ----------

# Process SQL files in parallel using ThreadPoolExecutor
max_workers = min(len(sql_files), 4)  # Limit max workers to avoid overwhelming the system
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    future_to_sql = {executor.submit(process_sql_file, sql_file): sql_file for sql_file in sql_files}
    
    for future in as_completed(future_to_sql):
        sql_file = future_to_sql[future]
        try:
            result = future.result()
            print(result)
        except Exception as e:
            print(f"Error processing {sql_file}: {str(e)}")

# COMMAND ----------

# List all created tables
tables = spark.sql("SHOW TABLES IN practice_sandbox.ma_sandbox").filter("tableName LIKE 'silver_%'")
display(tables)

# COMMAND ----------

def test_not_null_ids(table_name):
    """Test that ID columns don't contain nulls"""
    # Get all ID columns (assuming they end with '_id' or are named 'id')
    columns_df = spark.sql(f"DESCRIBE {table_name}")
    id_columns = [row.col_name for row in columns_df.collect() 
                 if row.col_name.lower().endswith('key') 
                 or row.col_name.lower() == 'key']
    
    results = []
    for col in id_columns:
        null_count = spark.sql(f"SELECT COUNT(*) as null_count FROM {table_name} WHERE {col} IS NULL").collect()[0].null_count
        results.append({
            'table': table_name,
            'column': col,
            'test': 'not_null',
            'passed': null_count == 0,
            'failed_records': null_count
        })
    return results

def test_unique_ids(table_name):
    """Test that ID columns contain unique values"""
    columns_df = spark.sql(f"DESCRIBE {table_name}")
    id_columns = [row.col_name for row in columns_df.collect() 
                 if row.col_name.lower().endswith('key') 
                 or row.col_name.lower() == 'key']
    
    results = []
    for col in id_columns:
        duplicate_count = spark.sql(f"""
            SELECT COUNT(*) as dup_count 
            FROM (
                SELECT {col}
                FROM {table_name}
                WHERE {col} IS NOT NULL
                GROUP BY {col}
                HAVING COUNT(*) > 1
            )
        """).collect()[0].dup_count
        
        results.append({
            'table': table_name,
            'column': col,
            'test': 'unique',
            'passed': duplicate_count == 0,
            'failed_records': duplicate_count
        })
    return results

# COMMAND ----------

# Run tests conditionally based on parameter
if should_test:
    print("Running data quality tests...")
    all_test_results = []

    tables_list = spark.sql("SHOW TABLES IN practice_sandbox.ma_sandbox").filter("tableName LIKE 'silver_%'").collect()
    for table in tables_list:
        table_name = f"practice_sandbox.ma_sandbox.{table.tableName}"
        
        # Run not null tests
        null_results = test_not_null_ids(table_name)
        all_test_results.extend(null_results)
        
        # Run unique tests
        unique_results = test_unique_ids(table_name)
        all_test_results.extend(unique_results)

    # Create DataFrame with test results
    test_results_df = spark.createDataFrame(all_test_results)
    display(test_results_df)

    # Check if any tests failed
    failed_tests = test_results_df.filter("passed = false").count()
    if failed_tests > 0:
        print(f"\n⚠️ {failed_tests} tests failed! Check the results above for details.")
    else:
        print("\n✅ All tests passed!")
else:
    print("Skipping tests as should_test is set to false")
