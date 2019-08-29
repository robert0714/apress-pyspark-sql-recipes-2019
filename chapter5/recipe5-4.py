# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 5 
# Recipe 5-4. Perform Joining Operations on Two DataFrames
# Run following PySpark code lines, line by line in PySpark shell
# -*- coding: UTF-8 -*-
from pyspark.sql.dataframe import DataFrame 
from refactor01.spark_config import PySpakCassandraConfig
from refactor01.spark_config import PySpakMSSQLConfig
import logging

class Recipe5_4(object):

    def load_and_get_table_df(self, keys_space_name: str, table_name: str) -> DataFrame: 
        sqlContext = PySpakCassandraConfig().returnSqlContext()
        table_df : DataFrame = sqlContext\
                   .read.format("org.apache.spark.sql.cassandra")\
                   .options(table=table_name, keyspace=keys_space_name).load() 
        return table_df
    
    def load_and_get_table_df_by_mssql(self , table_name: str) -> DataFrame:  
        table_df : DataFrame = PySpakMSSQLConfig().returnDataFrameReader() \
                    .option("dbtable", table_name) .load()
        return table_df


if __name__ == "__main__":
    try:
        # Step 5-4-1. Reading Student and Subject Data Tables from a Cassandra Database
        print("Step 5-4-1. Reading Student and Subject Data Tables from a Cassandra Database")
        test :Recipe5_4 = Recipe5_4()
        
        print("使用各自SqlContext")
        studentsDf : DataFrame = test.load_and_get_table_df("pysparksqlbook", "students")
        studentsDf.show()
        
        subjectsDf : DataFrame = test.load_and_get_table_df("pysparksqlbook", "subjects")
        subjectsDf.show()
        
        from pyspark.sql import SparkSession
        spark :SparkSession = PySpakCassandraConfig().returnSparkSession();
        
        print("使用共同SparkSession")
        studentsDf : DataFrame = spark .read.format("org.apache.spark.sql.cassandra").options(keyspace="pysparksqlbook",table="students").load()
        studentsDf.show()
        
        subjectsDf : DataFrame  = spark .read.format("org.apache.spark.sql.cassandra").options(keyspace="pysparksqlbook",table="subjects").load()
        subjectsDf.show()
        
        subjectsDf : DataFrame = subjectsDf.drop("id")
        subjectsDf.show()
       
       
        #   Step 5-4-2. Performing an Inner Join on DataFrames        
        print("Step 5-4-2. Performing an Inner Join on DataFrames")
        
        innerDf  : DataFrame = studentsDf.join(subjectsDf, studentsDf.studentId == subjectsDf.studentId, how= "inner").orderBy(studentsDf.studentId )
        innerDf.show()
        
        print("註冊TempView，才能使用TempView")
        studentsDf.createOrReplaceTempView("students")
        subjectsDf.createOrReplaceTempView("subjects")
        
        testStudentDf  :DataFrame = spark.sql("select * from  students")
        testStudentDf.show()
        
        testSubjectsDf  :DataFrame = spark.sql("select * from  subjects")
        testSubjectsDf.show() 
        
        print("Step 5-4-2. Performing an Inner Join on TempView")
        testInnerDf  :DataFrame = spark.sql("select * from  students  inner join subjects on students.studentId = subjects.studentId")
        testInnerDf.show() 
        
        # Step 5-4-3. Performing a Left Outer Join on DataFrames
        print("Step 5-4-3. Performing a Left Outer Join on DataFrames")
        leftOuterDf  : DataFrame = studentsDf.join(subjectsDf, studentsDf.studentId == subjectsDf.studentId, how= "left")
        leftOuterDf.show()
        
        print("Step 5-4-3. Performing a Left Outer Join on TempView")
        testLeftOuterDf  :DataFrame = spark.sql("select * from  students  left join subjects on students.studentId = subjects.studentId")
        testLeftOuterDf.show() 
        
        # Step 5-4-4. Performing a Right Outer Join on DataFrames
        print("Step 5-4-4. Performing a Right Outer Join on DataFrames")
        rightOuterDf : DataFrame = studentsDf.join(subjectsDf, studentsDf.studentId == subjectsDf.studentId, how= "right")
        rightOuterDf.show()
        
        print("Step 5-4-4. Performing a Right Outer Join on TempView")
        testRightOuterDf  :DataFrame = spark.sql("select * from  students  right join subjects on students.studentId = subjects.studentId")
        testRightOuterDf.show() 
        
        # Step 5-4-5. Performing a Full Outer Join on DataFrames
        print("Step 5-4-5. Performing a Full Outer Join on DataFrames")
        outerDf = studentsDf.join(subjectsDf, studentsDf.studentId == subjectsDf.studentId, how= "outer")
        outerDf.show()
        
        print("Step 5-4-5. Performing a Full Outer Join on TempView")
        testOuterDf  :DataFrame = spark.sql("select * from  students  full join subjects on students.studentId = subjects.studentId")
        testOuterDf.show() 
        
        
        
    except Exception as e:   
        logging.error("%s", e, exc_info=False)
    finally:
        logging.info('end')
     
