# Databricks notebook source
class batchWC:

    def __init__(self):
        self.base_dir = "/FileStore/tables/school_enrollments"

    def getRawData(self):
        from pyspark.sql.functions import explode, split

        df  = (spark.read
          .format('csv')
          .option('header', 'true')
          .load(f"{self.base_dir}/data/csv")
            )
        return df.select(explode(split(df.sch_mgmt_name," ")).alias("words"))
    
    def getQualityData(self,rawDF):
        from pyspark.sql.functions import trim, lower

        return (rawDF.select( lower(trim(rawDF.words)).alias("words"))
                        .where(rawDF.words.isNotNull())
                        .where("words rlike '[a-z]'")
                )
    
    def getWordCount(self,qualityDF):
        return qualityDF.groupBy("words").count()
    
    def overwriteCountData(self,wordCountDF):
        (
        wordCountDF.write.format("delta")
        .mode("overwrite")
        .saveAsTable("word_count_table")
        )

    def wordCount(self):
        print("executing word count")
        rawDF = self.getRawData()
        qualityDF = self.getQualityData(rawDF)
        resultDF = self.getWordCount(qualityDF)
        self.overwriteCountData(resultDF)
        print("Done")
        


# COMMAND ----------

display(spark.sql(f"select count(*) from word_count_table where substr(words,1,1)='s'"))

# COMMAND ----------


