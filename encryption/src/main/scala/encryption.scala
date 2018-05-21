package org.frb.encryption

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

object EncryptionUtilities {
  val spark = SparkSession.builder.getOrCreate()
  val sc = spark.sparkContext
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  def FPEMasking (inputDF : DataFrame, columnName : String, EncryptionType : String, EncryptionFormat : String, AESEncryptionKey : Array[Byte] , TTLKey : Array[Byte]) : DataFrame = {
    val FPEBuilder =  if (EncryptionFormat.toLowerCase == "all") {com.idealista.fpe.builder.FormatPreservingEncryptionBuilder.ff1Implementation().withAllCharacterDomain().withDefaultPseudoRandomFunction(AESEncryptionKey).withDefaultLengthRange().build()
      } else if (EncryptionFormat.toLowerCase == "numeric") {com.idealista.fpe.builder.FormatPreservingEncryptionBuilder.ff1Implementation().withNumericDomain().withDefaultPseudoRandomFunction(AESEncryptionKey).withDefaultLengthRange().build()
      } else if (EncryptionFormat.toLowerCase == "numericwithdashes"){com.idealista.fpe.builder.FormatPreservingEncryptionBuilder.ff1Implementation().withSocialSecurityNumberWithDashesDomain().withDefaultPseudoRandomFunction(AESEncryptionKey).withDefaultLengthRange().build()
      } else {com.idealista.fpe.builder.FormatPreservingEncryptionBuilder.ff1Implementation().withDefaultDomain().withDefaultPseudoRandomFunction(AESEncryptionKey).withDefaultLengthRange().build()
      }
    val selectedColumnDF = if (EncryptionFormat.toLowerCase != "numeric") {inputDF.selectExpr("CASE WHEN length(" + columnName + ") < 2 THEN lpad(" + columnName  + ", 2, ' ') ELSE " + columnName + " END")} else {inputDF.selectExpr("CASE WHEN length(" + columnName + ") < 2 THEN lpad(CAST(" + columnName + "as String), 2, '0' ELSE CAST(" + columnName + " as String) END")} 
    val encryptionArray = if (EncryptionType.toLowerCase =="encrypt") {selectedColumnDF.rdd.map(x=>FPEBuilder.encrypt(x.mkString,TTLKey))
    } else {selectedColumnDF.select(columnName).rdd.map(x=>FPEBuilder.decrypt(x.mkString,TTLKey))} 
    val encryptionDF = if (EncryptionFormat.toLowerCase == "numeric") {encryptionArray.map(x=>x.toLong).toDF} else {encryptionArray.toDF}
    val encryptionDF2 = encryptionDF.withColumn("rowId", monotonically_increasing_id())
    val inputDF2 = inputDF.withColumn("rowId", monotonically_increasing_id())
    val outputDF = inputDF2.join(encryptionDF2,inputDF2("rowID")===encryptionDF2("rowId"),"inner").drop("rowId").drop(columnName).withColumnRenamed("value",columnName).select(inputDF.columns.head, inputDF.columns.tail: _*)
    outputDF
  }
}
