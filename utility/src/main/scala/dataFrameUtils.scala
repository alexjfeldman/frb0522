package org.frb.utilities

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

object DataFrameUtils {
  val spark = SparkSession.builder.getOrCreate()
  val sc = spark.sparkContext
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  import spark.implicits._
  import sqlContext.implicits._

  def readFromS3(inputReadLocation : String, fileType : String) : DataFrame = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    import spark.implicits._
    val inputDF = if(fileType.toUpperCase == "CSV") {spark.read.format("csv").option("header","true").option("inferSchema","true").load(inputReadLocation)
    } else if(fileType.toUpperCase == "PARQUET"){spark.read.format("parquet").option("inferSchema","true").load(inputReadLocation)
    } else {spark.read.format("json").load(inputReadLocation)}
    inputDF
  }
  
  def writeToS3Parquet(outputDF:DataFrame,bucketName:String,folderStructure:String, writeType:String,userName:String,timeStamp2:Long,spark:SparkSession,sc:SparkContext) {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val outputDF2 = outputDF.withColumn("Update_User_Name",lit(userName)).withColumn("Update_Time_Stamp",lit(timeStamp2))
    val outputWriteLocation = "s3://" + bucketName + "/" + folderStructure + "/"
    outputDF2.write.format("parquet").mode(writeType.toLowerCase).save(outputWriteLocation)
  }
}
