# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS GE_CONFIG_TABLE;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE GE_CONFIG_TABLE (interface_id VARCHAR(20), column_name VARCHAR(50) , rule_name VARCHAR(50), rule_dimension VARCHAR(50), value_set VARCHAR(50), mostly DECIMAL(2,1));
# MAGIC
# MAGIC  INSERT INTO GE_CONFIG_TABLE VALUES
# MAGIC     ('ORG_001', 'Roll_no', 'check_if_not_null', 'Completeness','', 1.0),
# MAGIC     ('ORG_001', 'Roll_no', 'check_if_unique', 'Uniqueness','', 1.0),
# MAGIC     ('ORG_001', 'Name', 'check_if_not_null', 'Completeness','', 1.0 ),
# MAGIC     ('ORG_001', 'Subject', 'check_if_values_in_list', 'Validity','Maths|Science|English|Hindi|Sanskrit',0.7 ),
# MAGIC     ('ORG_001', 'Marks', 'check_if_values_inbetween', 'InBetween','10|100',1.0 );
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM GE_CONFIG_TABLE;
