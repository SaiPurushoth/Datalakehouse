# Databricks notebook source
class SilverLayerData:
    def __init__(self):
        self.checkpoint_location = '/FileStore/checkpoint/silver_layer'

    def cleanup_checkpoint(self):
        dbutils.fs.rm(self.checkpoint_location, True)

    def get_data(self,table_name):
        return (
            spark
            .readStream
            .table(f'{table_name}')
        )

    def camel_case(self,df,column_name):
        from pyspark.sql.functions import lower,col,initcap
        return (df.withColumn(column_name, lower(col(column_name)))
                    .withColumn(column_name, initcap(col(column_name))) 
                )
    def drop_column(self,df,column_name):
        return (
            df.drop(column_name)
        )
    def write_data_to_silver(self,df,table_name,partition_column_name = 'district_name'):
        return (df.writeStream
                .option("checkpointLocation", f'{self.checkpoint_location}/{table_name}')
                .outputMode("append")
                .trigger(once=True)
                .partitionBy(partition_column_name)
                .table(f'{table_name}') 
                )
        
    def lower_column_names(self,df):
        for column_name in df.columns:
            df = df.withColumnRenamed(column_name,column_name.lower())
        
        return df

    def clean_school_enrollment(self):
        df = self.get_data('school_enrollments')
        df = self.camel_case(df,'district_name')
        df = self.camel_case(df,'udise_block_name')
        df = self.drop_column(df,'ingest_date')
        df = self.write_data_to_silver(df,'school_enrollments_silver')

    
    def clean_school_infra(self):
        df = self.get_data('school_infra')
        df = self.lower_column_names(df)
        df = self.camel_case(df,'district_name')
        df = self.camel_case(df,'udise_block_name')
        df = self.drop_column(df,'ingest_date')
        df = self.write_data_to_silver(df,'school_infra_silver')
        
    def clean_teacher_details(self):
        df = self.get_data('teacher_details')
        df = self.lower_column_names(df)
        df = self.camel_case(df,'district_name')
        df = self.camel_case(df,'block_name')
        df = self.drop_column(df,'ingest_date')
        df = self.write_data_to_silver(df,'teacher_details_silver')

        

# COMMAND ----------

if __name__ == "__main__":
    SilverLayerData().cleanup_checkpoint()
    SilverLayerData().clean_school_enrollment()
    SilverLayerData().clean_school_infra()
    SilverLayerData().clean_teacher_details()

# COMMAND ----------


