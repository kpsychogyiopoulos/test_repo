# JIRA
  
This project implements an ETL pipeline to migrate project management metadata from Jira Atlassian into Datahub table ```jira_tickets```.

It is maintained as a project in IBM Cloud Pak for Data under the name: ```dev-datahub-jira ```.


## Table of Contents  
- [ProjectStructure](#project-structure)
- [Installation & Library Imports](#installation--library-imports)
- [Spark-Session Orchestration](#spark-session-orchestration)
- [Code execution](#kedro-module-execution)  
- [Source Code Modifications](#source-code-modifications)

---

## Project Structure

_project_data_assets/data_asset_:
- __jira-master.zip__ : This project asset is deployed within the CP4D persistent volume. It contains the refactored code base.
- __requirements.txt__: The file containing all python dependencies needed to be installed prior to code execution.

_project_git_repo/dev-datahub-jira/assets/jupyterlab_:
- __jira-master/__ : Hosts the source code and modular Python scripts required for system operation. Performs the ETL pipeline on Jira.
- __run_hedno_losses.ipynb__ : Orchestrates the execution of the Python modules contained within the __jira-master__ directory to run the end-to-end pipeline.

---

## Installation & Library Imports
  
To run this project, you need to use runtime environment with Python 3.11 and Spark 3.5. The libraries are installed using the requirements.txt file.


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

After initializing the global Spark session, the instance is injected into the import_db function within cli.py. This ensures that the main execution entry point in __main__.py has access to a managed Spark context for distributed data operations.
To initiate the pipeline within the JupyterLab environment, we use the %run magic command to execute the primary entry point. Replace <command> with the specific functional stage required:

```bash 
%run /home/spark/project/assets/jupyterlab/jira-master/src/jira/__main__.py <command> 

get_data: Triggers the Jira API extraction and local staging.

import_db: Initiates the Spark-based ingestion into the Lakehouse.
```

---

## Source Code Modifications

To facilitate the "lift and shift" migration and ensure successful operation within the CP4D runtime, the following architectural adjustments and environment-specific configurations were implemented:

- Edited file cli.py to use the existing spark session, create a temporary view and use spark sql commands to replace or append to the target table ```jira_tickets``` in schema ```dabaml``` in Datahub's silver catalog:
  🛠️jira/clients/cli.py: 

              # OLD ❌
              engine = get_sqlserver_connection()
              ...
              df.to_sql(
                  f"{SETTINGS.default_args.table_name}",
                  engine,
                  schema=SETTINGS.sqlserver_import_schema,
                  if_exists=if_exists,
                  index=False,
              )
              
              # NEW ✅
              spark = SparkSession.builder.getOrCreate()
              ...
              target_table = f"{SETTINGS.datahub_catalog}.{SETTINGS.sqlserver_import_schema}.{SETTINGS.default_args.table_name}"
              ...
              if if_exists == "replace":
                  # Το 'replace' στο Spark SQL μεταφράζεται σε INSERT OVERWRITE
                  spark.sql(f"INSERT OVERWRITE TABLE {target_table} SELECT * FROM temp_table_output")
              else:
                  # Το 'append' μεταφράζεται σε INSERT INTO
                  spark.sql(f"INSERT INTO {target_table} SELECT * FROM temp_table_output")

- Added parameter values to the parameters listed below:
  🛠️jira/config.py:
              
              # NEW ✅
              table_name: str = "jira_tickets"
              ...
              sqlserver_import_schema: str = "dabaml"
              datahub_catalog: str = "dev_datahub_silver"
              ... 
              api_user: str = <api_user>
              api_token: str = <api_token>

