package com.coke.nsrV3.commons

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders, Row, SparkSession}

class Functions {

  // Creating a spark session
  def spark_Session(): SparkSession = {
    System.setProperty("hadoop.home.dir", "c:/winutils")
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark
  }

  // method to read the data from config table
  def loadConfigTable(url: String, table: String, username: String, Password: String): DataFrame = {

    val config_table = spark_Session().read.format("jdbc")
      .options(Map("url" -> url, "dbtable" -> table, "user" -> username, "password" -> Password)).load()
    config_table
  }

  // Currently local file. "Here we need to mention azure file name and path"
  def headerCheck(config_DF: DataFrame, file: String): Boolean = {

    // As the config file location and properties are always same we can pass as properties

    config_DF.createOrReplaceTempView("configdata")
    val header_DF = spark_Session().sql("select _c5 as filename,_c2 as header from configdata")
    val header_present = header_DF.filter(header_DF("filename").equalTo(file))
      .select("header").as(Encoders.STRING).collectAsList().get(0)
    //.rdd.map(x=>x.mkString).collect()(0)
    if (header_present == "Y") true
    else false
  }

  // method to read the data from files
  def readData(file: String, if_Header: Boolean): DataFrame = {

    val header = s""""${if_Header}""""
    // check for passing boolean . It should be in "". How to implement "" in parameters
    val input_file_data = spark_Session().read.option("header", header).csv("filePath")
    //val input =   inputdata1.withColumn("index",monotonically_increasing_id())
    val window = Window.orderBy("_c1")
    // c0 is the place holde.needed any mandatory primary column
    val result = input_file_data.withColumn("index", row_number().over(window))
    result
  }

  def showData(dataFrame: DataFrame): Unit = {

    dataFrame.show()
  }

  def printSchema(dataFrame: DataFrame): Unit = {
    dataFrame.printSchema()
  }

  def nullcheck(dataFrame: DataFrame): Unit = {

    val DF_with_out_nulls  = dataFrame.na.drop(Array("Col1","Col2"))
    DF_with_out_nulls

  }

  def columnCheck(dataFrame: DataFrame): Unit = {


  }

  def DF_with_no_duplicates(dataFrame: DataFrame): Dataset[Row] = {

    val data = Window.partitionBy("_c2").orderBy("_c2")
    val DF_withnoDuplicates = dataFrame.withColumn("row_number", row_number().over(data))
      .filter("row_number==1")
    val final_DF = DF_withnoDuplicates.drop("row_number")
    final_DF
  }


  def DF_with_Duplicates(dataFrame: DataFrame,column: Column): Dataset[Row] = {

    val data = Window.partitionBy(column).orderBy(column)
    val DF_Duplicatedata = dataFrame.withColumn("row_number", row_number().over(data))
      .filter("row_number > 1")
    // select([c for c in df.columns if c not in {'column_1', 'column_2', 'column_3'}])
    DF_Duplicatedata
  }
}
