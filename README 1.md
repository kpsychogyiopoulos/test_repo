# HEDNO-LOSSES
  
This project has been developed with Kedro. It implements preprocessing, training, inference and clustering phases to detect power thefts for HEDNO. It processes data from a range of tables stored in Datahub.

The project is located in IBM Cloud Pack for Data, Projects and is named as ```dev-datahub-hedno-losses ```.


## Table of Contents  
- [ProjectStructure](...)
- [Installation & Library Imports](#installation)   
- [Environment Variables Initialisation](#data)
- [Spark-Session Orchestration](#usage)
- [Kedro module Execution](#results)  
- [Source Code Modifications](...)
  
## Project Structure

project_data_assets/data_asset:
- __PT_20260301_v2.zip__ : The folder was unzipped in the filesystem of the CP4D project to be accessed by the code. Includes initial data files (csv files) that were used by the project as input as well as pkl files that include trained parameters and model embeddings that are used by the project. There are also parquet files that are stored in intermediate folders created and used by the code. Lastly there are pdf files that include plots and reports for the results.
- __inference_pkl.zip__: pkl files that were unzipped inside PT_20260301_v2/ necessary for the inference part during the code execution
- data_transf.zip
- __requirements.txt__: The file containing all python dependencies needing to be installed before the code execution.

project_git_repo/datahub-hedno-losses/assets/jupyterlab:
- __hedno-losses-master/__ : Includes the python modules that have been developed and are used to run the code.
- __run_hedno_losses.ipynb__ : Executes the python modules in "hedno-losses-master" folder

## Installation & Library Imports
  
To run this project, you need to have Python and the following libraries installed:  
- ruamel.yaml
- tokenizers==0.19.1
- transformers==4.44.2
- torch==2.4.1
- scikit-plot==0.3.7
- yellowbrick==1.5
- optuna==3.2.0
- umap-learn==0.5.6
- kedro==0.18.14
- kedro-datasets==1.6.0
- Boruta==0.3
- mlxtend==0.23.1
- spacy==3.7.5
- typing_extensions==4.12.2
- shap==0.44.1
- hdfs==2.7.0
- s3fs==2023.12.2
- scikit-learn==1.4.2 

The following libraries were installed using different versions than the original ones:
- scikit-learn==1.2.2 (old) -> scikit-learn==1.4.2 (new)
- shap==0.41.0 (old) -> shap==0.44.1 (new)


You can install the required libraries running the initial notebook shell that includes the command:  


```bash  
pip install --no-cache-dir --user -r /project_data/data_asset/requirements.txt
``` 

In the following shells we import:

- the libraries used by the code to ensure everything works as supposed to.
- the libraries required to setup a Spark Session designed for CP4D and watsonx.data


## Environment Variables Initialisation

Environment variables that will be passed as parameters to the python module. We declare them by running the following command:



```
os.environ["date"] = "PT_20200115"
os.environ["phase"] = "inference"
os.environ["current_date"] = "2020-01-16"
```

In the shell above we pass the respective dates. Phase can be --inference or --train.


## Spark-Session Orchestration

Having imported the environment variables, next we run the functions that create a SparkSession factory designed for Cloud Pak for Data and watsonx.data, which automates the creation and management of Iceberg sessions integrated with ADLS and MinIO storage. Finally a spark session is configured and set ready for use.


## Kedro module execution

Having setup and initiated the spark session that we will be using, we inject the spark session to our Kedro pipeline.

This shell is designed to execute the Kedro Pipeline. First it performs a number of steps to prepare for the execution of the Kedro pipeline.Specifically:

- __SHAP & Transformers__: It patches safe_isinstance to prevent SHAP from searching for transformers modules.
- __Tensorflow Blocking__: Blocking of TensorFlow to avoid circular imports in the CP4D environment.
- __LightGBM Patch__: This code applies a "monkey patch" to LightGBM to prevent a crash caused by the _n_classes attribute returning None instead of an integer. It replaces the broken attribute with a custom property that safely defaults to a valid value (1 for regressors, 2 for classifiers), ensuring the model can perform comparisons and predictions without triggering a TypeError.
- __Sklearn OneHotEncoder Patch__: A monkey patch for the scikit-learn library that adds a missing property to the OneHotEncoder class to ensure compatibility with newer coding standards or specific pipeline requirements.
- __Log Suppression__: A logging "silencer" designed to clean up the console output by suppressing verbose and repetitive messages from the project's main libraries.

Finally:

- __Kedro Execution__: The orchestration engine that initializes the Kedro framework and tunes the Spark environment for distributed execution. It creates a KedroSession using the specific hedno configuration environment and triggers the actual execution of the pipeline.


## Source Code Modifications

In order to complete the lift and shift procedure and have the code running successfully in CP4D runtime environment we added the following changes:

- Edited the following yml files to include the correct paths to the new data sources:
  🛠️conf/hedno/globals: 
    - path change from ```bash /mnt/misetl/powerthefts/prl/``` to  ```bash /home/spark/project/assets/data_asset/PT_20260301_v2/``` which is the path in which all the project related data files exist.
  
  🛠️conf/hedno/catalog/raw/catalog.yml:
    - We changed the data sources to point to datahub tables in watsonx.data instead of csv files.
      Eg.:
          ```
          old ❌ 
                _spark_input_dataset: &spark_input_dataset
                  type: spark.SparkDataSet
                  file_format: csv
                  load_args:
                    header: True
                    sep: "|"
                    encoding: utf-8

          new ✅   
                _spark_table_dataset: &spark_table_dataset
                  type: spark.SparkHiveDataSet
                  database: dev_datahub_silver
  
          old ❌
                powerthefts:
                  <<: *spark_input_dataset
                  filepath: ${hedno_raw}/${date}/PowerTheft.csv
          
          new ✅
                powerthefts:
                  <<: *spark_table_dataset
                  table: hlosses_test.ml_powerthefts
          ```

