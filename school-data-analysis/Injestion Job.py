# Databricks notebook source

class InjestData:
    def __init__(self):
        self.base_path = '/FileStore/tables/'
        self.checkpoint_location = '/FileStore/checkpoint/'

    def cleanup_checkpoint(self):
        dbutils.fs.rm(self.checkpoint_location, True)

    def get_school_enrollments_schema(self):
        from pyspark.sql.types import StructField, StructType, StringType, IntegerType
        schema = StructType(
        [
            StructField('ac_year', StringType(), True),
            StructField('st_code', StringType(), True),
            StructField('state_name', StringType(), True),
            StructField('dt_code', IntegerType(), True),
            StructField('district_name', StringType(), True),
            StructField('block_cd', IntegerType(), True),
            StructField('udise_block_name', StringType(), True),
            StructField('loc_name', StringType(), True),
            StructField('ch_category_id', IntegerType(), True),
            StructField('tr_cat_name', StringType(), True),
            StructField('school_category', StringType(), True),
            StructField('sch_mgmt_id', IntegerType(), True),
            StructField('sch_mgmt_name', StringType(), True),
            StructField('caste_id', IntegerType(), True),
            StructField('caste_name', StringType(), True),
            StructField('pre_primary_boy', IntegerType(), True),
            StructField('pre_primary_girl', IntegerType(), True),
            StructField('class1_boy', IntegerType(), True),
            StructField('class2_boy', IntegerType(), True),
            StructField('class3_boy', IntegerType(), True),
            StructField('class4_boy', IntegerType(), True),
            StructField('class5_boy', IntegerType(), True),
            StructField('class6_boy', IntegerType(), True),
            StructField('class7_boy', IntegerType(), True),
            StructField('class8_boy', IntegerType(), True),
            StructField('class9_boy', IntegerType(), True),
            StructField('class10_boy', IntegerType(), True),
            StructField('class11_boy', IntegerType(), True),
            StructField('class12_boy', IntegerType(), True),
            StructField('class1_girl', IntegerType(), True),
            StructField('class2_girl', IntegerType(), True),
            StructField('class3_girl', IntegerType(), True),
            StructField('class4_girl', IntegerType(), True),
            StructField('class5_girl', IntegerType(), True),
            StructField('class6_girl', IntegerType(), True),
            StructField('class7_girl', IntegerType(), True),
            StructField('class8_girl', IntegerType(), True),
            StructField('class9_girl', IntegerType(), True),
            StructField('class10_girl', IntegerType(), True),
            StructField('class11_girl', IntegerType(), True),
            StructField('class12_girl', IntegerType(), True),
        ]
        )
        return schema
    
    def get_school_infra_schema(self):
        from pyspark.sql.types import StructField, StructType, StringType, IntegerType
        schema = StructType(
            [
                StructField('Academic_Year', StringType(), True),
                StructField('State_Code', IntegerType(), True),
                StructField('State_Name', StringType(), True),
                StructField('District_Code', IntegerType(), True),
                StructField('District_Name', StringType(), True),
                StructField('Block_Code', IntegerType(), True),
                StructField('Udise_Block_Name', StringType(), True),
                StructField('Location', StringType(), True),
                StructField('School_Category_Id', IntegerType(), True),
                StructField('School_Category_Name', StringType(), True),
                StructField('School_Management_Id', IntegerType(), True),
                StructField('School_Management_Name', StringType(), True),
                StructField('Total_Number_of_Schools', IntegerType(), True),
                StructField('Building', IntegerType(), True),
                StructField('Boundary_Wall', IntegerType(), True),
                StructField('Single_Class_Room', IntegerType(), True),
                StructField('Separate_Room_for_Headmaster', IntegerType(), True),
                StructField('Land_Available', IntegerType(), True),
                StructField('Electricity', IntegerType(), True),
                StructField('Functional_Electricity', IntegerType(), True),
                StructField('Solar_Panel', IntegerType(), True),
                StructField('Furniture', IntegerType(), True),
                StructField('Playground', IntegerType(), True),
                StructField('Library_or_Reading_Corner_or_Book_Bank', IntegerType(), True),
                StructField('Librarian', IntegerType(), True),
                StructField('Newspaper', IntegerType(), True),
                StructField('Kitchen_Garden', IntegerType(), True),
                StructField('Boy_Toilet', IntegerType(), True),
                StructField('Functional_Boy_Toilet', IntegerType(), True),
                StructField('Girl_Toilet', IntegerType(), True),
                StructField('Functional_Girl_Toilet', IntegerType(), True),
                StructField('Toilet_Facility', IntegerType(), True),
                StructField('Functional_Toilet_Facility', IntegerType(), True),
                StructField('Functional_Urinal_Boy', IntegerType(), True),
                StructField('Functional_Urinal_Girl', IntegerType(), True),
                StructField('Functional_Urinal', IntegerType(), True),
                StructField('Functional_Toilet_and_Urinal', IntegerType(), True),
                StructField('Drinking_Water', IntegerType(), True),
                StructField('Functional_Drinking_Water', IntegerType(), True),
                StructField('Water_Purifier', IntegerType(), True),
                StructField('Rain_Water_Harvesting', IntegerType(), True),
                StructField('Water_Tested', IntegerType(), True),
                StructField('Handwash', IntegerType(), True),
                StructField('Incinerator', IntegerType(), True),
                StructField('WASH_Facility_Drinking_Water_Toilet_and_Handwash', IntegerType(), True),
                StructField('Ramps', IntegerType(), True),
                StructField('Medical_Checkup', IntegerType(), True),
                StructField('Complete_Medical_Checkup', IntegerType(), True),
                StructField('Internet', IntegerType(), True),
                StructField('Computer_Available', IntegerType(), True)
            ]
        )
        return schema
    
    def get_teacher_details_schema(self):
        from pyspark.sql.types import StructField, StructType, StringType, IntegerType
        schema = StructType(
            [
                StructField('Academic_Year', StringType(), True),
                StructField('State_Code', IntegerType(), True),
                StructField('State_Name', StringType(), True),
                StructField('District_Code', IntegerType(), True),
                StructField('District_Name', StringType(), True),
                StructField('Block_Code', IntegerType(), True),
                StructField('Block_Name', StringType(), True),
                StructField('School_Management_Id', IntegerType(), True),
                StructField('School_Management_Name', StringType(), True),
                StructField('School_Category_Id', IntegerType(), True),
                StructField('School_Category_Name', StringType(), True),
                StructField('Academic_Qualification_Id', IntegerType(), True),
                StructField('Academic_Qualification_Name', StringType(), True),
                StructField('Professional_Qualification_Id', IntegerType(), True),
                StructField('Professional_Qualification_Name', StringType(), True),
                StructField('Only_Pre_Primary_Male', IntegerType(), True),
                StructField('Only_Pre_Primary_Female', IntegerType(), True),
                StructField('Pre_Primary_and_Primary_Male', IntegerType(), True),
                StructField('Pre_Primary_and_Primary_Female', IntegerType(), True),
                StructField('Only_Primary_Male', IntegerType(), True),
                StructField('Only_Primary_Female', IntegerType(), True),
                StructField('Primary_and_Upperprimary_Male', IntegerType(), True),
                StructField('Primary_and_Upperprimary_Female', IntegerType(), True),
                StructField('Only_Upperprimary_Male', IntegerType(), True),
                StructField('Only_Upperprimary_Female', IntegerType(), True),
                StructField('Upperprimary_and_Secondary_Male', IntegerType(), True),
                StructField('Upperprimary_and_Secondary_Female', IntegerType(), True),
                StructField('Only_Secondary_Male', IntegerType(), True),
                StructField('Only_Secondary_Female', IntegerType(), True),
                StructField('Secondary_and_Highersecondary_Male', IntegerType(), True),
                StructField('Secondary_and_Highersecondary_Female', IntegerType(), True),
                StructField('Only_Highersecondary_Male', IntegerType(), True),
                StructField('Only_Highersecondary_Female', IntegerType(), True),
                StructField('Total_Teacher', IntegerType(), True),
                StructField('Total_Male', IntegerType(), True),
                StructField('Total_Female', IntegerType(), True)

            ]
        )
        return schema

    def get_stream_from_file(self,schema,folder_name):
        return (
            spark.readStream
            .format('cloudFiles')
            .option('cloudFiles.format', 'csv')
            .schema(schema)
            .option("header", True)
            .load(f'{self.base_path}/{folder_name}')
            )
        
    def write_stream_to_delta(self, df,folder_name):
        return ( 
                df.writeStream
                .format('delta')
                .option('checkpointLocation', f'{self.checkpoint_location}/{folder_name}')
                .outputMode('append')
                .table(folder_name)
            )



    def ingest_school_enrollment(self):
        folder_name = 'school_enrollments'
        schema = self.get_school_enrollments_schema()
        read_df = self.get_stream_from_file(schema,folder_name)
        self.write_stream_to_delta(read_df,folder_name)

    def ingest_school_infra(self):
        folder_name = 'school_infra'
        schema = self.get_school_infra_schema()
        read_df = self.get_stream_from_file(schema,folder_name)
        self.write_stream_to_delta(read_df,folder_name)

    def ingest_school_teacher(self):
        folder_name = 'teacher_details'
        schema = self.get_teacher_details_schema()
        read_df = self.get_stream_from_file(schema,folder_name)
        self.write_stream_to_delta(read_df,folder_name)




# COMMAND ----------

if __name__ == '__main__':
    InjestData().ingest_school_enrollment()
    InjestData().ingest_school_infra()
    InjestData().ingest_school_teacher()

# COMMAND ----------


