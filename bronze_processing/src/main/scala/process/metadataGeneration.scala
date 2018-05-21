package org.frb.bronze.process

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame


object Metadata {
  val spark = SparkSession.builder.enableHiveSupport().getOrCreate()
  val sc = spark.sparkContext
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  import sqlContext.implicits._

  def generation (inputDF : DataFrame, tableName : String, columnName : Array[String], userName : String, timeStamp : Long) = {
    var generateMetaDataIterator : Int= 1
    val columnNameLength = columnName.length
    var iterationMetaData = 1
    var selectedColumn = inputDF.selectExpr("cast("+ columnName(iterationMetaData-1) +" as string) as testColumn")
    var lengthDF = selectedColumn.selectExpr("max(length(testColumn)) as Maximum_Length","min(length(testColumn)) as Minimum_Length","avg(length(testColumn)) as Average_Length","cast(min(testColumn) as string) as Minimum_Value","cast(max(testColumn) as string) as Maximum_Value")
    var distinctValues = selectedColumn.distinct.count
    var totalNumber = selectedColumn.count
    var totalMissing = selectedColumn.filter("testColumn is null").count
    var percentMissing = totalMissing/totalNumber 
    var phase1DF = lengthDF.withColumn("Number_Of_Unique_Values",lit(distinctValues)).withColumn("Table_Name",lit(tableName)).withColumn("Column_Name",lit(columnName(iterationMetaData-1))).withColumn("Data_Type",lit(columnName(iterationMetaData-1).toString)).withColumn("Percent_Missing",lit(percentMissing)).withColumn("Number_Of_Rows",lit(totalNumber))
    var intrimDF = phase1DF
    var outputDF = phase1DF
    iterationMetaData = 2
    while(iterationMetaData <= columnNameLength) {
      selectedColumn = inputDF.selectExpr("cast("+ columnName(iterationMetaData-1) +" as string) as testColumn")
      lengthDF = selectedColumn.selectExpr("max(length(testColumn)) as Maximum_Length","min(length(testColumn)) as Minimum_Length","avg(length(testColumn)) as Average_Length","cast(min(testColumn) as string) as Minimum_Value","cast(max(testColumn) as string) as Maximum_Value")
      distinctValues = selectedColumn.distinct.count
      phase1DF = lengthDF.withColumn("Number_Of_Unique_Values",lit(distinctValues)).withColumn("Table_Name",lit(tableName)).withColumn("Column_Name",lit(columnName(iterationMetaData-1))).withColumn("Data_Type",lit(columnName(iterationMetaData-1).toString)).withColumn("Percent_Missing",lit(percentMissing)).withColumn("Number_Of_Rows",lit(totalNumber))
      intrimDF = outputDF.union(phase1DF)
      outputDF = intrimDF
      iterationMetaData = iterationMetaData + 1
    }
    val metaDataOutputDF = outputDF.selectExpr("Table_Name","Column_Name","Data_Type","Number_Of_Rows","Number_Of_Unique_Values","Percent_Missing","Minimum_Value","Maximum_Value","Minimum_Length","Average_Length","Maximum_Length").withColumn("Update_User_Name",lit(userName)).withColumn("Update_Time_Stamp",lit(timeStamp))
    metaDataOutputDF.write.format("parquet").mode("append").save("s3://metadatafrb/metadata_generation/")
    spark.sql("REFRESH metadata_generation")
  }
}
