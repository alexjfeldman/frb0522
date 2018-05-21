package org.frb.utilities

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import commonregex._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.frb.utilities.DataFrameUtils._
import org.apache.spark.sql.DataFrame

case class PII_Scoring_DF(Column_Name : String, IP_Percentage : Long, Time_Percentage : Long, Phone_Percentage : Long, Link_Percentage : Long, Date_Percentage : Long, Email_Percentage : Long, PII_Flag : String)
case class Intrim_Schema_Generation_DF(Column_Name : String, Data_Type : String)

object metaDataCreation {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val inputBucketName : String = args(0)
    val inputFileType : String = args(1)
    val inputFilePath : String = args(2)
    val tableName : String = args(3)
    val userName : String = args(4)
    val timeStamp = args(5)

    val inputDF : DataFrame = DataFrameUtils.readFromS3(inputBucketName, inputFilePath,inputFileType,spark)
    val checkSchemaBucketName = "frbdev"
    val checkSchemaDFFolderStructure : String = "Table_Schema_Check/*"
    val checkSchemaDF : DataFrame =  spark.read.format("parquet").load("s3://frbdev/Table_Schema_Check_2/*").filter($"Table_Name"===tableName).sort($"Column_Number")

    var runCheckBoolean = true
    var loopThrough = true
    if(checkSchemaDF.count == 0) {
      runCheckBoolean = false
      loopThrough = false}
 
    var fullSchemaDF = Seq(Intrim_Schema_Generation_DF("", "")).toDF()
    val newColumnNames = inputDF.columns
    val newColumnDataTypes = inputDF.schema.fields.map(x=>x.dataType).map(x=>x.toString)
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
        if(newColumnDataTypes(checkSchemaIterator-1) != oldColumnDataTypes(checkSchemaIterator-1) || newColumnNames(checkSchemaIterator-1) != oldColumnNames(checkSchemaIterator-1)) {
          runCheckBoolean = false
          loopThrough = false
        }
        if(checkSchemaIterator==totalNumberFromInput) {
          loopThrough = false
        }
          checkSchemaIterator = checkSchemaIterator + 1
      }
    }

    var columnName=inputDF.columns
    val columnNameLength = columnName.length

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

    var outputSchemaDF = Seq(Intrim_Schema_Generation_DF("", "")).toDF()
    var tempSchemaDF = Seq(Intrim_Schema_Generation_DF("", "")).toDF()
    var intrimSchemaDF = Seq(Intrim_Schema_Generation_DF("", "")).toDF()
    var outputPIIScoringDF = Seq(PII_Scoring_DF("", 0, 0, 0, 0, 0, 0,"")).toDF()
    var tempPIIScoringDF = Seq(PII_Scoring_DF("", 0, 0, 0, 0, 0, 0,"")).toDF()
    var intrimPIIScoringDF = Seq(PII_Scoring_DF("", 0, 0, 0, 0, 0, 0,"")).toDF()

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
      fullSchemaDF.withColumn("Update_User_Name",lit(userName)).withColumn("Update_Time_stamp",lit(timeStamp)).write.format("parquet").mode("append").save("s3://frbdev/Table_Schema_Check_2/")
      spark.sql("msck repair table Table_Schema_Check")     

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

      tempPIIScoringDF=Seq(PII_Scoring_DF(columnName(0), IPPercentage, TimePercentage, PhonePercentage, LinkPercentage, DatePercentage, EmailPercentage, presentPII)).toDF()
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
  
        tempPIIScoringDF=Seq(PII_Scoring_DF(columnName(PIICheckIterator -1), IPPercentage, TimePercentage, PhonePercentage, LinkPercentage, DatePercentage, EmailPercentage, presentPII)).toDF()
        intrimPIIScoringDF=outputPIIScoringDF.union(tempPIIScoringDF)
        outputPIIScoringDF=intrimPIIScoringDF
        PIICheckIterator+=1
      }
      reviewNeeded = if(reviewFlag) {"True"} else {"False"}
      intrimPIIScoringDF=outputPIIScoringDF.withColumn("Review_Flag",lit(reviewNeeded)).withColumn("Encryption_Type", lit(PII_Response)).withColumn("Update_User_Name",lit(userName)).withColumn("Update_Time_Stamp",lit(timeStamp)).withColumn("Table_Name",lit(tableName))
      outputPIIScoringDF=intrimPIIScoringDF
      outputPIIScoringDF.write.format("parquet").mode("overwrite").save("s3://frbdev/PII_Scoring_2/"+tableName)
      val addPartitionStatement = "alter table PII_Scoring add if not exists partition(table_name='" + tableName + "') location 's3://frbdev/PII_Scoring_2/" + tableName + "'"
      spark.sql(addPartitionStatement)
      spark.sql("msck repair table PII_Scoring")
    }

    var generateMetaDataIterator : Int= 1
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
    
    val metaDataOutputDF = outputDF.select($"Table_Name",$"Column_Name",$"Data_Type",$"Number_Of_Rows",$"Number_Of_Unique_Values",$"Percent_Missing",$"Minimum_Value",$"Maximum_Value",$"Minimum_Length",$"Average_Length",$"Maximum_Length").withColumn("Update_User_Name",lit(userName)).withColumn("Update_Time_Stamp",lit(timeStamp))
    metaDataOutputDF.write.format("parquet").mode("append").save("s3://frbdev/metadata_generation_2/")
    spark.sql("msck repair table metadata_generation")

  }
}
