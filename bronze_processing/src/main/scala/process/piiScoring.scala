package org.frb.bronze.process

import commonregex._
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

case class PII_Scoring_DF(Table_Name : String, Column_Name : String, IP_Percentage : Long, Time_Percentage : Long, Phone_Percentage : Long, Link_Percentage : Long, Date_Percentage : Long, Email_Percentage : Long, PII_Flag : String)

object piiScoring {
  val spark = SparkSession.builder.getOrCreate()
  val sc = spark.sparkContext
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._


  def score (inputDF : DataFrame, runBoolean : Boolean, tableName : String, columnName : Array[String], userName : String, timeStamp : Long) = {
  
    var PIICheckIterator=2
    var TimeNotNull:Long=0
    var TimePercentage:Long=0
    var PhoneNotNull:Long=0
    var PhonePercentage:Long=0
    var LinkNotNull:Long=0
    var LinkPercentage:Long=0
    var DateNotNull:Long=0
    var DatePercentage:Long=0
    var IPNotNull:Long=0
    var IPPercentage:Long=0
    var EmailNotNull:Long=0
    var EmailPercentage:Long=0
    var TotalCount=inputDF.count
    var presentPII = "False"
    var reviewFlag = false
    var reviewNeeded = "False"
    val PII_Response = "Not Identified"

    var outputPIIScoringDF = Seq(PII_Scoring_DF("", "", 0, 0, 0, 0, 0, 0,"")).toDF()
    var tempPIIScoringDF = Seq(PII_Scoring_DF("", "", 0, 0, 0, 0, 0, 0,"")).toDF()
    var intrimPIIScoringDF = Seq(PII_Scoring_DF("", "", 0, 0, 0, 0, 0, 0,"")).toDF()

    val columnNameLength = columnName.length

    var buildSchemaIterator = 2
    if(!runBoolean) {
      TimeNotNull=inputDF.select(columnName(0)).rdd.map(X => CommonRegex.time.findAllIn(X.toString).toList.mkString).toDF.filter($"value"!=="").count
      TimePercentage=TimeNotNull/TotalCount
    
      PhoneNotNull=inputDF.select(columnName(0)).rdd.map(X => CommonRegex.phone.findFirstIn(X.toString).toList.mkString).toDF.filter($"value"!=="").count
      PhonePercentage=PhoneNotNull/TotalCount
    
      LinkNotNull=inputDF.select(columnName(0)).rdd.map(X => CommonRegex.link.findFirstIn(X.toString).toList.mkString).toDF.filter($"value"!=="").count
      LinkPercentage=LinkNotNull/TotalCount
    
      DateNotNull=inputDF.select(columnName(0)).rdd.map(X => CommonRegex.date.findAllIn(X.toString).toList.mkString).toDF.filter($"value"!=="").count
      DatePercentage=DateNotNull/TotalCount
    
      IPNotNull=inputDF.select(columnName(0)).rdd.map(X => CommonRegex.ip.findFirstIn(X.toString).toList.mkString).toDF.filter($"value"!=="").count
      IPPercentage=IPNotNull/TotalCount
    
      EmailNotNull=inputDF.select(columnName(0)).rdd.map(X => CommonRegex.email.findFirstIn(X.toString).toList.mkString).toDF.filter($"value"!=="").count
      EmailPercentage=EmailNotNull/TotalCount
    
      if(TimePercentage>=.35 || PhonePercentage>=.35 || LinkPercentage>=.35 || DatePercentage>=.35 || IPPercentage>=.35 || EmailPercentage>=.35) {
        presentPII="True"
        reviewFlag=true
      } else {presentPII = "False"}

      tempPIIScoringDF=Seq(PII_Scoring_DF(tableName , columnName(0), IPPercentage, TimePercentage, PhonePercentage, LinkPercentage, DatePercentage, EmailPercentage, presentPII)).toDF()
      outputPIIScoringDF=tempPIIScoringDF

      while(PIICheckIterator <= columnNameLength) {
        TimeNotNull=inputDF.select(columnName(PIICheckIterator -1)).rdd.map(X => CommonRegex.time.findAllIn(X.toString).toList.mkString).toDF.filter($"value"!=="").count
        TimePercentage=TimeNotNull/TotalCount
      
        PhoneNotNull=inputDF.select(columnName(PIICheckIterator -1)).rdd.map(X => CommonRegex.phone.findFirstIn(X.toString).toList.mkString).toDF.filter($"value"!=="").count
        PhonePercentage=PhoneNotNull/TotalCount
      
        LinkNotNull=inputDF.select(columnName(PIICheckIterator -1)).rdd.map(X => CommonRegex.link.findFirstIn(X.toString).toList.mkString).toDF.filter($"value"!=="").count
        LinkPercentage=LinkNotNull/TotalCount
      
        DateNotNull=inputDF.select(columnName(PIICheckIterator -1)).rdd.map(X => CommonRegex.date.findAllIn(X.toString).toList.mkString).toDF.filter($"value"!=="").count
        DatePercentage=DateNotNull/TotalCount
      
        IPNotNull=inputDF.select(columnName(PIICheckIterator -1)).rdd.map(X => CommonRegex.ip.findFirstIn(X.toString).toList.mkString).toDF.filter($"value"!=="").count
        IPPercentage=IPNotNull/TotalCount
      
        EmailNotNull=inputDF.select(columnName(PIICheckIterator -1)).rdd.map(X => CommonRegex.email.findFirstIn(X.toString).toList.mkString).toDF.filter($"value"!=="").count
        EmailPercentage=EmailNotNull/TotalCount
      
        if(TimePercentage>=.35 || PhonePercentage>=.35 || LinkPercentage>=.35 || DatePercentage>=.35 || IPPercentage>=.35 || EmailPercentage>=.35) {
          presentPII="True"
          reviewFlag=true
        } else {presentPII = "False"}
  
        tempPIIScoringDF=Seq(PII_Scoring_DF(tableName, columnName(PIICheckIterator -1), IPPercentage, TimePercentage, PhonePercentage, LinkPercentage, DatePercentage, EmailPercentage, presentPII)).toDF()
        intrimPIIScoringDF=outputPIIScoringDF.union(tempPIIScoringDF)
        outputPIIScoringDF=intrimPIIScoringDF
        PIICheckIterator+=1
      }
      reviewNeeded = if(reviewFlag) {"True"} else {"False"}
      intrimPIIScoringDF=outputPIIScoringDF.withColumn("Review_Flag",lit(reviewNeeded)).withColumn("Encryption_Type", lit(PII_Response)).withColumn("Update_User_Name",lit(userName)).withColumn("Update_Time_Stamp",lit(timeStamp))
      outputPIIScoringDF=intrimPIIScoringDF
      outputPIIScoringDF.write.format("parquet").mode("append").save("s3://metadatafrb/PII_Scoring/")
      spark.sql("REFRESH PII_Scoring")
    }
  }
}
