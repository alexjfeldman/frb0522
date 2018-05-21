package org.frb.bronze.process

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import commonregex._
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.frb.bronze.process._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.frb.utilities._

object fileCheck {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    var AESEncryptionMethod = "86753090867530908675309086753090".getBytes()

    val inputKeyDF = spark.read.format("text").load("/user/hadoop/FSIWest.pem")

    val PrivateKeyHolder = inputKeyDF.rdd.map(x=>x.mkString).collect

    var FirstRunBoolean = false
    var FirstRunBooleanKey = true
    var iterator = 2
    var outputString = ""
    var intrimString = ""
    var startString = ""

    while(iterator <= PrivateKeyHolder.length-1) {
      if(FirstRunBooleanKey) {
        outputString=PrivateKeyHolder(iterator-1)
        FirstRunBooleanKey=false
      } else {
        startString=PrivateKeyHolder(iterator-1)
        intrimString=outputString+startString
        outputString=intrimString
      }
      iterator = iterator + 1
    }

    val BinaryKey = outputString.getBytes

    val fileType : String = args(0)
    val fileLocation : String = args(1)
    val tableName : String = args(2)
    val userName : String = args(3)
    val timeStampWrite : Long = args(4).toLong

    val inputDF = DataFrameUtils.readFromS3(fileLocation,fileType)
    val columnNames = inputDF.columns
    val columneDataTypes = inputDF.schema.fields.map(x=>x.dataType).map(x=>x.toString)
    val booleanTest = schemaGeneration.checkSchema(inputDF, tableName,columnNames,columneDataTypes,userName, timeStampWrite)
    piiScoring.score(inputDF, booleanTest, tableName, columnNames, userName, timeStampWrite)
    Metadata.generation(inputDF, tableName, columnNames, userName, timeStampWrite)
    Masking.FPE(inputDF, tableName, timeStampWrite, userName, AESEncryptionMethod, BinaryKey, "encrypt")
    inputDF.write.format("parquet").mode("overwrite").save("s3://bronzeprocessing/Archive/" + tableName + "/" + timeStampWrite)    
  }
}

