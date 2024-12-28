# Databricks notebook source
# MAGIC %md
# MAGIC # **Load The Files To DBFS Storage**

# COMMAND ----------

from enum import Enum
class School(Enum):
    FACILITY_LOCATION = 1
    TEACHER_DETAILS = 2
    SCHOOL_ENROLLMENTS = 3
    SCHOOL_INFRA = 4


class LoadData:
    def __init__(self):
        self.source_base_dir = 'file:/Workspace/Repos/saipurushothg@presidio.com/Datalakehouse/school-data-analysis/source'
        self.target_base_dir = '/FileStore/tables'
        self.folder_dict = {'facility_location':'json','teacher_details':'csv','school_enrollments':'csv','school_infra':'csv'}

    def cleanup_all(self):
        print("Cleaning up the target directory")
        dbutils.fs.rm(self.target_base_dir, recurse=True)
        print("Target directory cleaned up")

    def cleanup_folder(self,folder_name):
        print("Cleaning up the target directory")
        dbutils.fs.rm(f"{self.target_base_dir}/{folder_name}", recurse=True)
        print("Target directory cleaned up")

    def get_file_count(self,folder_name):
        print(f"Total number of files in {folder_name}")
        return len(dbutils.fs.ls(f"{self.source_base_dir}/{folder_name}/"))

    def copy_data(self,file_path):
        print(f"Copying {file_path} to {self.target_base_dir}/{file_path}")
        dbutils.fs.cp(f"{self.source_base_dir}/{file_path}", f"{self.target_base_dir}/{file_path}")
        print(f"Copying {file_path} to {self.target_base_dir}/{file_path} succeeded")

    def load_files(self,no_of_files,folder_name,types):
        print(f"Loading {no_of_files} files from {folder_name}")
        initial_count=1 
        max_count = self.get_file_count(folder_name)
        print(f'max files in {folder_name} is {max_count}')
        print(f'files to be moved is {no_of_files}')
        self.cleanup_folder(folder_name)
        if no_of_files > max_count:
            raise Exception("No of files to be loaded is greater than the number of files available")
        while initial_count<= no_of_files: 
            file_path = f"{folder_name}/{folder_name}_{initial_count}.{types}"
            self.copy_data(file_path)
            initial_count+=1
        print("Loading {no_of_files} files from {folder_name} succeeded")

    def copy_all_data(self):
        print("Loading all files from all folders")
        self.cleanup_all()
        for folder_name in self.folder_dict:
            no_of_files = self.get_file_count(folder_name)
            self.load_files(no_of_files,folder_name,self.folder_dict[folder_name])
        print("Loading all files from all folders succeeded")

    def copy_data_to_dbfs(self,no_of_files = -1,option = -1):
        print("Copying data to dbfs")
        if option == School.FACILITY_LOCATION:
            folder_name = 'facility_location'
            if no_of_files == -1:
                no_of_files = self.get_file_count(folder_name)
            self.load_files(no_of_files,folder_name,self.folder_dict[folder_name])
        elif option == School.TEACHER_DETAILS:
            folder_name =  'teacher_details'
            if no_of_files == -1:
                no_of_files = self.get_file_count(folder_name)
                self.load_files(no_of_files,folder_name,self.folder_dict[folder_name])
        elif option == School.SCHOOL_ENROLLMENTS:
            folder_name = 'school_enrollments'
            if no_of_files == -1:
                no_of_files = self.get_file_count(folder_name)
                self.load_files(no_of_files,folder_name,self.folder_dict[folder_name])
        elif option == School.SCHOOL_INFRA:
            folder_name = 'school_infra'
            if no_of_files == -1:
                no_of_files = self.get_file_count(folder_name)
                self.load_files(no_of_files,folder_name,self.folder_dict[folder_name])
        else:
            self.copy_all_data()
        print("Copying data to dbfs succeeded")


# COMMAND ----------

if __name__ == "__main__":
    loadObj = LoadData()
    loadObj.copy_data_to_dbfs()

# COMMAND ----------


