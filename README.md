---
page_type: sample
languages:
- pyspark
products:
- Databricks
description: "Data quality checking frame work for Pyspark in Databricks based on Great Expectations framework"
---

# Data quality checking frame work for Pyspark in Databricks based on Great Expectations framework

<!-- 
Guidelines on README format: https://review.docs.microsoft.com/help/onboard/admin/samples/concepts/readme-template?branch=master
-->

## Create a databricks instance

Create a data bricks workspace. If you already have one then skip this step.

## Clone the repo

Create a user that has enough rights to execute all the needed statements used to deploy the database.  For example

## Execute the Create_required_tables notebook 

To create the config delta table with some dummy data for the demo. The location of the delta table and the data being inserted can be changed as per need.

## Execute the Data_validation_with_Config notebook

Input parameters : 
interface_id -- Interface id to identity the interface being tested.
output_json_filename -- The JSON file containing the test suite to be used

First creates a JSON file containing the test suite based on what was inserted into the config delta table earlier.

Then creates a dummy spark dataframe and conducts the tests on it. 

TESTING LINE
