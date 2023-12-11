# Databricks notebook source
!pip install great_expectations
!pip  install simplejson

# COMMAND ----------

import json

class JSONFileReader:
    def __init__(self, filename):
        self.filename = filename

    def read(self):
        with open(self.filename) as f:
            return json.load(f)

# COMMAND ----------

from abc import ABC, abstractmethod

class Expectation(ABC):
    def __init__(self, column, dimension, add_info = {}):
        self.column = column
        self.dimension = dimension
        self.add_info = add_info

    @abstractmethod
    def test(self, ge_df):
        pass

# COMMAND ----------

class NotNullExpectation(Expectation):
    def __init__(self, column, dimension, add_info = {}):
        super().__init__(column, dimension, add_info)

    def test(self, ge_df):
        ge_df.expect_column_values_to_not_be_null(column=self.column, mostly=self.add_info["mostly"], meta = {"dimension": self.dimension})

class UniqueExpectation(Expectation):
    def __init__(self, column, dimension, add_info = {}):
        super().__init__(column, dimension, add_info)

    def test(self, ge_df):
        ge_df.expect_column_values_to_be_unique(column=self.column, mostly=self.add_info["mostly"], meta = {"dimension": self.dimension})

class ValuesInListExpectation(Expectation):
    def __init__(self, column, dimension, add_info = {}):
        super().__init__(column, dimension, add_info)

    def test(self, ge_df):
        ge_df.expect_column_values_to_be_in_set(column=self.column, value_set=self.add_info["value_set"], mostly=self.add_info["mostly"], meta = {"dimension": self.dimension})

class ValuesInBetweenExpectation(Expectation): # WORK IN PROGRESS
    def __init__(self, column, dimension, add_info = {}):
        super().__init__(column, dimension, add_info)

    def test(self, ge_df):
        ge_df.expect_column_values_to_be_between(column=self.column, min_value=self.add_info["min_value"],max_value=self.add_info["max_value"], mostly=self.add_info["mostly"], meta = {"dimension": self.dimension})

class ValuesToMatchRegex(Expectation): # WORK IN PROGRESS
    def __init__(self, column, dimension, add_info = {}):
        super().__init__(column, dimension, add_info)

    def test(self, ge_df):
        ge_df.expect_column_values_to_match_regex(column=self.column, regex=self.add_info["regex"], mostly=self.add_info["mostly"], meta = {"dimension": self.dimension})

class ValuesInTypeList(Expectation): # WORK IN PROGRESS
    def __init__(self, column, dimension, add_info = {}):
        super().__init__(column, dimension, add_info)

    def test(self, ge_df):
        ge_df.expect_column_values_to_be_in_type_list(column=self.column, type_list=self.add_info["type_list"], mostly=self.add_info["mostly"], meta = {"dimension": self.dimension})


# COMMAND ----------

from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

class DataQuality:

    def __init__(self, pyspark_df, config_path):
        self.pyspark_df = pyspark_df
        self.config_path = config_path

    def rule_mapping(self, dq_rule):
        return{"check_if_not_null" : "NotNullExpectation", "check_if_unique" : "UniqueExpectation", "check_if_values_in_list" : "ValuesInListExpectation","check_if_values_inbetween":"ValuesInBetweenExpectation", "check_if_values_match_regex":"ValuesToMatchRegex","check_if_values_in_typelist":"ValuesInTypeList"}[dq_rule]

    def _get_expectation(self):
        class_obj = globals()[self.rule_mapping()]
        return class_obj(self.extractor_args)

    def convert_to_ge_df(self):
        return SparkDFDataset(self.pyspark_df)

    def read_config(self):
        json_reader = JSONFileReader(self.config_path)
        return json_reader.read()

    def run_test(self):
        ge_df = self.convert_to_ge_df()
        config = self.read_config()
        #print(config)
        for column in config["columns"]:
            if column["dq_rule(s)"] is None:
                continue
            for dq_rule in column["dq_rule(s)"]:
                expectation_obj = globals()[self.rule_mapping(dq_rule["rule_name"])]
                expectation_instance = expectation_obj(column["column_name"], dq_rule["rule_dimension"], dq_rule["add_info"])
                expectation_instance.test(ge_df)

        dq_results = ge_df.validate()
        return dq_results


# COMMAND ----------

def create_df_from_dq_results(spark, dq_results):
    dq_data = []
    for result in dq_results["results"]:
        if result["success"] == True:
            status = 'PASSED'
        else:
            status = 'FAILED'
        dq_data.append((
        result["expectation_config"]["kwargs"]["column"],
        result["expectation_config"]["meta"]["dimension"],
        status,
        result["expectation_config"]["expectation_type"],
        result["result"]["unexpected_count"],
        result["result"]["element_count"],
        result["result"]["unexpected_percent"],
        float(100-result["result"]["unexpected_percent"]),
        result["expectation_config"]["kwargs"]["mostly"]))
    dq_columns = ["column", "dimension", "status", "expectation_type", "unexpected_count", "element_count", "unexpected_percent", "expected_percent","mostly"]
    dq_df = spark.createDataFrame(data=dq_data,schema=dq_columns)
    return dq_df,dq_results["success"]

# COMMAND ----------

from timeit import default_timer as timer

import simplejson as json
from pyspark.sql.functions import col

def format_json_file(interface_id,outfile):
  t0 = timer()
  config_mstr_df = spark.sql(f"SELECT * FROM GE_CONFIG_TABLE WHERE INTERFACE_ID = \"{interface_id}\"")
  #Distinct
  spark.conf.set("spark.sql.execution.arrow.enabled", "true")
  config_mstr_cols_df = config_mstr_df.dropDuplicates(["INTERFACE_ID","COLUMN_NAME"]).select(col("INTERFACE_ID"),col("COLUMN_NAME"))
    
  config_json_org = {}
  config_json_org["data_product_name"] = interface_id
  columns = []
  
  for row in config_mstr_cols_df.rdd.collect():
    interface_id = row['INTERFACE_ID'] 
    colm_name = row['COLUMN_NAME']
    #print(interface_id, colm_name)
    config1 = {}
    config1["column_name"] = colm_name
    rules = []
    col_name = "'" + colm_name + "'"
    config_mstr_df_i = config_mstr_df.filter((config_mstr_df.column_name == colm_name) & (config_mstr_df.interface_id == interface_id))
    for row2 in config_mstr_df_i.rdd.collect():
      config2 = {}
      config2["rule_name"] = row2["rule_name"]
      config2["rule_dimension"] = row2["rule_dimension"]
      add_info = {}
      if row2["mostly"] is None:
        add_info["mostly"] = 1.0
      else:
        add_info["mostly"] = row2["mostly"]
 
      if row2["value_set"] != "":
        value_set = row2["value_set"].split('|')
        if config2["rule_name"] == "check_if_values_inbetween":
          add_info["min_value"] = int(value_set[0])
          add_info["max_value"] = int(value_set[1])
        else:
          if config2["rule_name"] == "check_if_values_in_list":
            add_info["value_set"] = value_set
      config2["add_info"] = add_info
      rules.append(config2)
    config1["dq_rule(s)"] = rules
    columns.append(config1)
  config_json_org["columns"] = columns
  #json_object = json.dumps(config_json_org, indent = 4)
  with open(outfile, "w") as f:
    json.dump(config_json_org, f)
