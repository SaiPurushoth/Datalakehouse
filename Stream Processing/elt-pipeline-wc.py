# Databricks notebook source
class streamWC:

    def __init__(self):
        from pyspark.sql.types import StructType, StructField, StringType
        self.base_dir = "/FileStore/tables/school_enrollments"
        self.schema = StructType([
        StructField("ac_year", StringType(), True),
        StructField("st_code", StringType(), True),
        StructField("state_name", StringType(), True),
        StructField("dt_code", StringType(), True),
        StructField("district_name", StringType(), True),
        StructField("block_cd", StringType(), True),
        StructField("udise_block_name", StringType(), True),
        StructField("loc_name", StringType(), True),
        StructField("sch_category_id", StringType(), True),
        StructField("tr_cat_name", StringType(), True),
        StructField("school_category", StringType(), True),
        StructField("sch_mgmt_id", StringType(), True),
        StructField("sch_mgmt_name", StringType(), True),
        StructField("caste_id", StringType(), True),
        StructField("caste_name", StringType(), True),
        StructField("pre_primary_boy", StringType(), True),
        StructField("pre_primary_girl", StringType(), True),
        StructField("class1_boy", StringType(), True),
        StructField("class2_boy", StringType(), True),
        StructField("class3_boy", StringType(), True),
        StructField("class4_boy", StringType(), True),
        StructField("class5_boy", StringType(), True),
        StructField("class6_boy", StringType(), True),
        StructField("class7_boy", StringType(), True),
        StructField("class8_boy", StringType(), True),
        StructField("class9_boy", StringType(), True),
        StructField("class10_boy", StringType(), True),
        StructField("class11_boy", StringType(), True),
        StructField("class12_boy", StringType(), True),
        StructField("class1_girl", StringType(), True),
        StructField("class2_girl", StringType(), True),
        StructField("class3_girl", StringType(), True),
        StructField("class4_girl", StringType(), True),
        StructField("class5_girl", StringType(), True),
        StructField("class6_girl", StringType(), True),
        StructField("class7_girl", StringType(), True),
        StructField("class8_girl", StringType(), True),
        StructField("class9_girl", StringType(), True),
        StructField("class10_girl", StringType(), True),
        StructField("class11_girl", StringType(), True),
        StructField("class12_girl", StringType(), True)
    ])

    def getRawData(self):
        from pyspark.sql.functions import explode, split

        df  = (spark.readStream
          .format('csv')
          .option('header', 'true')
          .schema(self.schema)
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
        return (
        wordCountDF.writeStream
        .format("delta")
        .option("checkpointLocation", f"{self.base_dir}/checkpoints/word_count")
        .outputMode("complete")
        .toTable("word_count_table")
        )

    def wordCount(self):
        print("strating word count stream")
        rawDF = self.getRawData()
        qualityDF = self.getQualityData(rawDF)
        resultDF = self.getWordCount(qualityDF)
        sQuery=self.overwriteCountData(resultDF)
        print("Done")
        return sQuery
 

# COMMAND ----------


