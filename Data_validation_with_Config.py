# Databricks notebook source
# MAGIC %run ./Common_funcs

# COMMAND ----------

dbutils.widgets.text("interface_id","ORG_001")
dbutils.widgets.text("output_json_filename","GE_json.json")
INTERFACE_ID = dbutils.widgets.get("interface_id")
outfile = dbutils.widgets.get("output_json_filename")
interface_id = "'" + INTERFACE_ID + "'"

# COMMAND ----------

# CREATE A FORMATTED JSON FILE CONTAINING THE TEST SUITE TO BE USED AS INPUT TO THE GREAT EXPECTATION TESTS
format_json_file(INTERFACE_ID,outfile)

# COMMAND ----------

# MAGIC %md # Create Sample spark dataframe and execute validation tests

# COMMAND ----------


student_df = spark.createDataFrame([
    (1,"Ram","Maths",50),
    (2,"Shyam","History",45),
    (3,"Mohan", "Science",29),
    (4,"Sohan", "Maths",60),
    (5,"Rohini", "Science",32),
    (6,"Raj", "Maths",10),
    (7,"Meena", "Hindi",5),
    (8,"Rani", "Sanskrit",20)], ["Roll_no", "Name", "Subject","Marks"])

dq = DataQuality(student_df, outfile)
dq_results = dq.run_test()

dq_df,test_outcome = create_df_from_dq_results(spark, dq_results)
print(f"Overall Test Outcome pass?: {test_outcome}")
print("Summary of results")
dq_df.show()


# COMMAND ----------

# MAGIC %md # SHOW COMPLETE RESULTS

# COMMAND ----------

print(dq_results)

# COMMAND ----------

if test_outcome == False: 
  raise Exception("Data quality checks failed")
