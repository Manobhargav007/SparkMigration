package com.coke.nsrV3.commons

import java.sql.DriverManager

import org.apache.log4j.{BasicConfigurator, Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CommonFunctions extends App {



  //val dataFile = loadData("Connection URL","Database table","Username","Password")
  //showData(dataFile)
  //printSchema(dataFile)
  //isNull(dataFile)


  val logger = Logger.getLogger(this.getClass.getName)


  System.setProperty("hadoop.home.dir","c:/winutils")
  val spark = SparkSession.builder().master("local[*]")
    .appName("Database connection").getOrCreate()
  logger.info("Spark intialized")
  logger.info("Git trail")
  val inputdata1 = spark.read.csv("file:///C:/revenue_shipfrom.csv")
  logger.info("Data loaded")
  inputdata1.show()




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
