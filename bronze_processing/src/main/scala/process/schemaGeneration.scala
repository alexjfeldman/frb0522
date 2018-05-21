package org.frb.bronze.process

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

case class Intrim_Schema_Generation_DF(Column_Name : String, Data_Type : String)

object schemaGeneration {
  val spark = SparkSession.builder.getOrCreate()
  val sc = spark.sparkContext
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val hiveCtx = new org.apache.spark.sql.hive.HiveContext(sc)

  import sqlContext.implicits._

  def checkSchema (inputDF : DataFrame, tableName  : String, newColumnNames : Array[String], newColumnDataTypes : Array[String], userName : String, timeStamp : Long) : Boolean = {
    val checkSchemaBucketName = "frbdev"
    val checkSchemaDF : DataFrame =  spark.read.format("parquet").load("s3://metadatafrb/Table_Schema_Check/*").filter($"Table_Name"===tableName).sort($"Column_Number")   

    var runCheckBoolean = true
    var loopThrough = true
    if(checkSchemaDF.count == 0) {
      runCheckBoolean = false
      loopThrough = false}
 
    var fullSchemaDF = Seq(Intrim_Schema_Generation_DF("", "")).toDF()
    if(runCheckBoolean) {
      val maxTimeStamp = checkSchemaDF.selectExpr("max(update_time_stamp)").rdd.map(x=>x.mkString.toLong).collect
      val slimmedCheckSchemaDF = checkSchemaDF.filter($"Update_Time_Stamp"===maxTimeStamp(0))

      val oldColumnNames = checkSchemaDF.select($"Column_Name").rdd.collect.map(x=>x.mkString)
      val oldColumnDataTypes = checkSchemaDF.select($"Data_Type").rdd.collect.map(x=>x.mkString)

      var checkSchemaIterator = 1

      val totalNumberFromCheck : Long = oldColumnNames.length
      val totalNumberFromInput : Long = newColumnNames.length
      if(totalNumberFromCheck != totalNumberFromInput) {
        runCheckBoolean = false
        loopThrough = false} 
    
      while(loopThrough) {
        if(newColumnDataTypes(checkSchemaIterator-1) != oldColumnDataTypes(checkSchemaIterator-1) && newColumnNames(checkSchemaIterator-1) != oldColumnNames(checkSchemaIterator-1)) {
          runCheckBoolean = false
          loopThrough = false
        }
        if(checkSchemaIterator==totalNumberFromInput) {
          loopThrough = false
        }
          checkSchemaIterator = checkSchemaIterator + 1
      }
    }
    var outputSchemaDF = Seq(Intrim_Schema_Generation_DF("", "")).toDF()
    var tempSchemaDF = Seq(Intrim_Schema_Generation_DF("", "")).toDF()
    var intrimSchemaDF = Seq(Intrim_Schema_Generation_DF("", "")).toDF()

    var buildSchemaIterator = 2
    if(!runCheckBoolean) {
      outputSchemaDF = Seq(Intrim_Schema_Generation_DF(newColumnNames(0), newColumnDataTypes(0))).toDF().withColumn("Column_Number",lit(1))
      while(buildSchemaIterator<=newColumnNames.length) {
        tempSchemaDF = Seq(Intrim_Schema_Generation_DF(newColumnNames(buildSchemaIterator-1), newColumnDataTypes(buildSchemaIterator-1))).toDF().withColumn("Column_Number",lit(buildSchemaIterator))
        intrimSchemaDF = outputSchemaDF.union(tempSchemaDF)
        outputSchemaDF = intrimSchemaDF
        buildSchemaIterator = buildSchemaIterator + 1
      }
      fullSchemaDF = outputSchemaDF.withColumn("Table_Name",lit(tableName)).selectExpr("Table_Name","Column_Name","Column_Number","Data_Type")
      fullSchemaDF.withColumn("Update_User_Name",lit(userName)).withColumn("Update_Time_stamp",lit(timeStamp)).write.format("parquet").mode("append").save("s3://metadatafrb/Table_Schema_Check/")
      spark.sql("REFRESH Table_Schema_Check")   
    }
    runCheckBoolean
  }
}

