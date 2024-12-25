# Databricks notebook source
# MAGIC %run ./elt-pipeline-wc
# MAGIC %run ./invoice-stream
# MAGIC

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



class invoiceStreamTestSuite():
    def __init__(self):
        self.base_data_dir = "/FileStore/data_spark_streaming_scholarnest"

    def cleanTests(self):
        print(f"Starting Cleanup...", end='')
        spark.sql("drop table if exists invoice_line_items")
        dbutils.fs.rm("/user/hive/warehouse/invoice_line_items", True)

        dbutils.fs.rm(f"{self.base_data_dir}/chekpoint/invoices", True)
        dbutils.fs.rm(f"{self.base_data_dir}/data/invoices", True)

        dbutils.fs.mkdirs(f"{self.base_data_dir}/data/invoices")
        print("Done")

    def ingestData(self, itr):
        print(f"\tStarting Ingestion...", end='')
        dbutils.fs.cp(f"{self.base_data_dir}/datasets/invoices/invoices_{itr}.json", f"{self.base_data_dir}/data/invoices/")
        print("Done")

    def assertResult(self, expected_count):
        print(f"\tStarting validation...", end='')
        actual_count = spark.sql("select count(*) from invoice_line_items").collect()[0][0]
        assert expected_count == actual_count, f"Test failed! actual count is {actual_count}"
        print("Done")

    def waitForMicroBatch(self, sleep=30):
        import time
        print(f"\tWaiting for {sleep} seconds...", end='')
        time.sleep(sleep)
        print("Done.")

    def runTests(self):
        self.cleanTests()
        iStream = invoiceStream()
        streamQuery = iStream.process()

        print("Testing first iteration of invoice stream...") 
        self.ingestData(1)
        self.waitForMicroBatch()        
        self.assertResult(1249)
        print("Validation passed.\n")

        print("Testing second iteration of invoice stream...") 
        self.ingestData(2)
        self.waitForMicroBatch()
        self.assertResult(2506)
        print("Validation passed.\n") 

        print("Testing third iteration of invoice stream...") 
        self.ingestData(3)
        self.waitForMicroBatch()
        self.assertResult(3990)
        print("Validation passed.\n")

        streamQuery.stop()





# COMMAND ----------

if __name__ == "__main__":
    sWCTests = streamWCTest()
    sWCTests.runTests()
    isTS = invoiceStreamTestSuite()
    isTS.runTests()	

# COMMAND ----------


