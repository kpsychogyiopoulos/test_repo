# ENERGY-MARKET
  
This project implements an automated pipeline for the ingestion, transformation, and persistence of energy market datasets. The system programmatically retrieves source Excel files, performs multi-file consolidation, and executes a deduplication logic to ensure high data integrity before synchronizing the cleaned records with the ```energy_market``` table in Datahub.

It is maintained as a project in IBM Cloud Pak for Data under the name: ```dev-datahub-energy-market ```.


## Table of Contents  
- [ProjectStructure](#project-structure)
- [Installation & Library Imports](#installation--library-imports)
- [Spark-Session Orchestration](#spark-session-orchestration)
- [Code execution](#kedro-module-execution)  
- [Source Code Modifications](#source-code-modifications)

---

## Project Structure

Use the following assets containing the latest version of the source code (as of __27/04/2026__) to apply further changes.

_project_data_assets/data_asset_:
- __energy-market-master.zip__ : This project asset is deployed within the CP4D persistent volume. It contains the refactored code base.
- __requirements.txt__: The file containing all python dependencies needed to be installed prior to code execution.

_project_git_repo/dev-datahub-jira/assets/jupyterlab_:
- __energy-market-master/__ : Hosts the source code and modular Python scripts required for system operation. Performs the ETL pipeline on the data retrieved from ```enexgroup``` loading them in Datahub silver, schema ```nrgmarket``` and table ```energy_market```.
- __run_energy_market.ipynb__ : Orchestrates the execution of the Python modules contained within the __energy-market-master__ directory to run the end-to-end pipeline.

---

## Installation & Library Imports
  
To run this project, you need to use runtime environment with Python 3.11 / Spark 3.5 and the following resources:
    
    4 Executors: 4 vCPU and 8 GB RAM, Driver: 2 vCPU and 16 GB RAM. 
    
The libraries are installed using the requirements.txt file.


You can install the required libraries running the initial notebook shell that includes the command:  


```bash  
pip install --no-cache-dir --user -r /project_data/data_asset/requirements.txt
``` 

In the following shells we import:

- Libraries required for standard operation.
- Libraries required to set up a Spark Session designed for CP4D and watsonx.data

---

## Spark-Session Orchestration

We run the functions that create a SparkSession factory designed for Cloud Pak for Data and watsonx.data, which automates the creation and management of Iceberg sessions integrated with ADLS and MinIO storage. Finally a spark session is configured and set ready for use.

---

## Code execution

After initializing the global Spark session, the instance is injected into the main() function within main.py. This ensures that the main execution entry point has access to a managed Spark context for distributed data operations.
To initiate the pipeline within the JupyterLab environment, we use the %run magic command to execute the primary entry point:

```bash 
  os.chdir('/home/spark/project/assets/jupyterlab/energy-market-master')
  
  # Now run your script
  %run main.py
```

---

## Source Code Modifications

To facilitate the "lift and shift" migration and ensure successful operation within the CP4D runtime, the following architectural adjustments and environment-specific configurations were implemented:

- Edited file main.py to use the existing spark session and pass it as a parameter to the functions ```delete_existing_data()``` and ```append_data_to_spark()```:
  🛠energy-market-master/main.py (lines 34, 72 and 84): 
              
              # NEW ✅
              spark = SparkSession.builder.getOrCreate()
              ...
              del_confirm = delete_existing_data(spark, config.get_raw_table_name(), min_date, max_date)
              ...
              insert_count = append_data_to_spark(spark, combined_data, config.get_raw_table_name(), min_date, max_date)
                  

- Defined the name of the Datahub table to which we will append ```enexgroup``` data:
  🛠️energy-market-master/config.ini:
              
              # OLD ❌
              download_folder = /app/data/energy-market/files
              ...
              raw_table_name = nrgmarket_src.energy_market

              # NEW ✅
              download_folder = download_folder
              ...
              raw_table_name = dev_datahub_silver.nrgmarket.energy_market

  - Renamed file ```sql_operations.py``` to ```spark_operations.py``` and performed the following code changes within this file:
    🛠️energy-market-master/utils/spark_operations.py:

             # NEW (lines 40-46) ✅
             delete_query = f"DELETE FROM {table_name} WHERE DDAY BETWEEN {min_date} AND {max_date}"

             count_query = f"SELECT COUNT(*) FROM {table_name}"
             count_todelete_query = f"SELECT COUNT(*) FROM {table_name} WHERE DDAY BETWEEN {min_date} AND {max_date}"
          
             count_before = spark.sql(count_query).collect()[0][0]
             count_todelete = spark.sql(count_todelete_query).collect()[0][0]
              
             # NEW (lines 56, 57) ✅
             spark.sql(delete_query)
             count_after = spark.sql(count_query).collect()[0][0]
  
             # NEW (lines 106, 122) ✅
             count_query = f"SELECT COUNT(*) FROM {table_name}"

            count_existed_query = f"SELECT COUNT(*) FROM {table_name} WHERE DDAY BETWEEN {min_date} AND {max_date}"
        
            count_before = spark.sql(count_query).collect()[0][0]
            count_existed = spark.sql(count_existed_query).collect()[0][0]
        
            if count_existed > 0:
                print(f"Insertion aborted. Data for dates {min_date_int} to {max_date_int} was not properly deleted.")
                return -1  # Abort process
        
            print(f"Inserting new data into '{table_name}'...")
        
            spark_df = spark.createDataFrame(combined_data)
            spark_df.write.mode("append").saveAsTable(table_name)
        
            count_after = spark.sql(count_query).collect()[0][0]
