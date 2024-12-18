# Databricks notebook source
# MAGIC %run ./elt-pipeline-wc

# COMMAND ----------

class streamWCTest:
    def __init__(self):
        self.base_data_dir = "/FileStore/tables/school_enrollments"

    def cleanTests(self):
        print(f"start cleaning ..")
        spark.sql("drop table if exists word_count_table")
        dbutils.fs.rm("/user/hive/warehouse/word_count_table", True)
        dbutils.fs.rm(f"{self.base_data_dir}/checkpoints",True)
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
        import time
        sleepTime = 30
        expected_count = 3

        self.cleanTests()
        wc = streamWC()
        sQuery = wc.wordCount()

        print("testing the first iteration...")
        self.loadData(1)
        print(f"waiting for {sleepTime} seconds...")
        time.sleep(sleepTime)
        self.assertResult(expected_count)
        print("first interation completed")

        print("testing the second iteration...")
        self.loadData(2)
        print(f"waiting for {sleepTime} seconds...")
        time.sleep(sleepTime)
        self.assertResult(expected_count)
        print("second interation completed")

        sQuery.stop()

 

# COMMAND ----------

if __name__ == "__main__":
    sWCTests = streamWCTest()
    sWCTests.runTests()

# COMMAND ----------


