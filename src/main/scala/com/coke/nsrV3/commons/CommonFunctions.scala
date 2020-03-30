package com.coke.nsrV3.commons
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame, Encoders, SparkSession}

object CommonFunctions {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "c:/winutils")
    val spark = SparkSession.builder().master("local[*]")
      .appName("Sales data load").getOrCreate()


    def readdata():DataFrame ={

      val inputdata = spark.read.csv("file:///C:/volume_sales.csv")
      //val input =   inputdata1.withColumn("index",monotonically_increasing_id())
      val window = Window.orderBy("_c2")
      // c0 is the place holde.needed any mandatory primary column
      val result = inputdata.withColumn("index", row_number().over(window))

      result
    }

    val inputdata1 = readdata()



    showData(inputdata1)

    // println(headerCheck("mfruitug_yyyymmdd_hhmmss_revenue_customer.csv"))

    // Data Load
    def loadData(url: String, table: String, username: String, Password: String): DataFrame = {

      System.setProperty("hadoop.home.dir", "c:/winutils")
      val spark = SparkSession.builder().master("local[*]")
        .appName("Database connection").getOrCreate()

      val inputdata = spark.read.format("jdbc")
        .options(Map("url" -> url, "dbtable" -> table, "user" -> username, "password" -> Password)).load()
      inputdata

    }

    // header check
    def headerCheck(file: String) = {
      val config_DF = spark.read.csv("file:///C:/config.csv")
      config_DF.createOrReplaceTempView("configdata")
      val header_DF = spark.sql("select _c5 as filename,_c2 as header from configdata")
      val header_present = header_DF.filter(header_DF("filename").equalTo(file))
        .select("header").as(Encoders.STRING).collectAsList().get(0)
      //.rdd.map(x=>x.mkString).collect()(0)
      if (header_present == "Y") true
      else false
    }
  }


  def showData(dataFrame: DataFrame) = {

    dataFrame.show()
  }

  def printSchema(dataFrame: DataFrame) = {
    dataFrame.printSchema()
  }


  def isNull(dataFrame: DataFrame,nonnullcolumns:Seq[String]): DataFrame = {

    // If we need to drop all the columns which has null
    val nonnullDF = dataFrame.na.drop("any")
    // If we need to drop any specific columns
    val nonullDF1 = dataFrame.na.drop(nonnullcolumns)

    nonnullDF

  }

  def DF_withnoduplicates(dataFrame: DataFrame) = {


    val DF_withnoDuplicates = dataFrame.filter("index ==1")
       // select([c for c in df.columns if c not in {'column_1', 'column_2', 'column_3'}])
    DF_withnoDuplicates
  }


  def DF_withduplicates(dataFrame: DataFrame) = {


    val DF_Duplicatedata = dataFrame.filter("index > 1")
    // select([c for c in df.columns if c not in {'column_1', 'column_2', 'column_3'}])
    DF_Duplicatedata
  }

}
