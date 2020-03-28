package com.coke.nsrV3.commons

import java.sql.DriverManager

import org.apache.log4j.{BasicConfigurator, Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CommonFunctions {

  def main(args: Array[String]): Unit = {

/*    val dataFile = loadData("Connection URL","Database table","Username","Password")
    showData(dataFile)
    printSchema(dataFile)
    isNull(dataFile)*/


    val logger = Logger.getLogger(this.getClass.getName)


    System.setProperty("hadoop.home.dir","c:/winutils")
    val spark = SparkSession.builder().master("local[*]")
      .appName("Database connection").getOrCreate()
/*    logger.info("Spark intialized")
    logger.info("Git trail")
    logger.info("Again updating")
    val inputdata1 = spark.read.csv("file:///C:/revenue_shipfrom.csv")
    logger.info("Data loaded")
    inputdata1.show()*/
println("The header value is  "+headerCheck("andina-rp_yyyymmdd_hhmmss_revenue_customer.csv"))


    def headerCheck(file:String):Boolean = {
      val config_file = spark.read.csv("file:///C:/config.csv")

      config_file.createOrReplaceTempView("configtable")
     // config_file.show()

      val headertable = spark.sql("select _c5 as filename,_c2 as header from configtable")
      headertable.show()
      headertable.createOrReplaceTempView("headerboolean")

      val headerbool = spark.sql("select header from headerboolean where filename = '{}'".format(file))
      if (headerbool == 'Y') true
      else false






    }






  }


  def loadData(url:String,table:String,username:String,Password:String):DataFrame = {

    System.setProperty("hadoop.home.dir","c:/winutils")
    val spark = SparkSession.builder().master("local[*]")
      .appName("Database connection").getOrCreate()

    val inputdata = spark.read.format("jdbc")
      .options(Map("url" -> url, "dbtable" -> table,"user" -> username, "password" ->Password))
      .load()

    val inputdata1 = spark.read.csv("file:///C://revenue_shipfrom.csv")
    return inputdata1


  }

  def showData(dataFrame: DataFrame) = {

    dataFrame.show()
  }

  def printSchema(dataFrame: DataFrame) = {
    dataFrame.printSchema()
  }


  def isNull(dataFrame: DataFrame) : DataFrame= {

    // If we need to drop all the columns which has null
    val nonnullDF = dataFrame.na.drop("any")
    // If we need to drop any specific columns
    val nonullDF1 = dataFrame.na.drop(Array("Column name"))

    return nonnullDF


  }



}
