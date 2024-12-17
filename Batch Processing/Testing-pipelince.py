# Databricks notebook source
# MAGIC %run ./ELT-pipeline-wc

# COMMAND ----------

class batchWCTest:
    def __init__(self):
        self.base_data_dir = "/FileStore/tables/school_enrollments"

    def cleanTests(self):
        print(f"start cleaning ..")
        spark.sql("drop table if exists word_count_table")
        dbutils.fs.rm("/user/hive/warehouse/word_count_table", True)
        dbutils.fs.rm(f"{self.base_data_dir}/checkpoint",True)
        dbutils.fs.rm(f"{self.base_data_dir}/data/csv",True)

        dbutils.fs.mkdirs(f"{self.base_data_dir}/data/csv")
        print("Done")

    def loadData(self,itr):
        print(f"start loading data ..")
        dbutils.fs.cp(f"{self.base_data_dir}/datasets/csv/school_enrollments_{itr}.csv", f"{self.base_data_dir}/data/csv/", True)
        print("Done")

    def assertResult(self,expected_count):
        actual_count = spark.sql("select count(*) from word_count_table where substr(words,1,1) = 's'").collect()[0][0]
        assert expected_count == actual_count, f"Expected {expected_count} but got {actual_count}"
    
    def runTests(self):
        self.cleanTests()
        wc = batchWC()

        print("testing the first iteration")
        self.loadData(1)
        wc.wordCount()
        self.assertResult(3)
        print("first interation completed")

        print("testing the second iteration")
        self.loadData(2)
        wc.wordCount()
        self.assertResult(6)
        print("second interation completed")
 



 

# COMMAND ----------

if __name__ == "__main__":
    test = batchWCTest()
    test.runTests()

# COMMAND ----------


