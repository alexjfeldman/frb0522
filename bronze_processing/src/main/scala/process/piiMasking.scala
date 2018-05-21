package org.frb.bronze.process

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.frb.encryption._

object Masking {
  val spark = SparkSession.builder.getOrCreate()
  val sc = spark.sparkContext
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val hiveCtx = new org.apache.spark.sql.hive.HiveContext(sc)

  import sqlContext.implicits._

  def FPE (inputDF : DataFrame, tableName : String, timeStamp : Long, userName : String, AESKey : Array[Byte], binaryKey : Array[Byte], encryptionType : String) = {
    val piiDF = spark.read.format("parquet").option("inferSchema","true").load("s3://metadatafrb/PII_Scoring/*").filter($"Table_Name" === tableName)

    val tableBoolean = if(piiDF.count > 0) {true} else {false}
    val piiBoolean = if(piiDF.filter($"Pii_Flag" === "True").count > 0) {true}  else {false}
    val reviewBoolean = if(piiDF.filter($"Review_Flag" === "True").count > 0) {false} else {true}
    val piiColumn = piiDF.filter($"Pii_Flag" === "True").select($"Column_Name").rdd.map(x=>x.mkString).collect
    val piiEncryptionType = piiDF.filter($"Pii_Flag" === "True").select($"Encryption_Type").rdd.map(x=>x.mkString).collect

    var piiIterator = 1
    var outputDF = inputDF 
    var intrimDF = inputDF

    if(!reviewBoolean || !tableBoolean) {
      inputDF.withColumn("User_Name", lit(userName)).withColumn("Time_Stamp",lit(timeStamp)).write.format("parquet").mode("overwrite").save("s3://bronzeprocessing/Review/" + tableName + "/" + timeStamp)
    } else { if(!piiBoolean) {inputDF.withColumn("User_Name", lit(userName)).withColumn("Time_Stamp",lit(timeStamp)).write.format("parquet").mode("overwrite").save("s3://bronzeprocessing/NonPII/" + tableName + "/" + timeStamp)
    } else {
      while(piiIterator <= piiColumn.length) {
        print("###################################################################")
        print("Masking Column: " + piiColumn(piiIterator -1) + "; using: " + piiEncryptionType(piiIterator -1))
        print("###################################################################")
        intrimDF = EncryptionUtilities.FPEMasking(outputDF, piiColumn(piiIterator - 1), encryptionType.toLowerCase, piiEncryptionType(piiIterator - 1).toLowerCase, AESKey, binaryKey)
        piiIterator = piiIterator + 1
      }
      outputDF.withColumn("User_Name", lit(userName)).withColumn("Time_Stamp",lit(timeStamp)).write.format("parquet").mode("overwrite").save("s3://bronzeprocessing/Pyrite/" + tableName + "/" + timeStamp)
      }
    }
  }
}
