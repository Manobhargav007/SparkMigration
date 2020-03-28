package com.coke.nsrV3.commons
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

object CommonFunctions {

  def main(args: Array[String]): Unit = {


    //val logger = Logger.getLogger(this.getClass.getName)


    System.setProperty("hadoop.home.dir", "c:/winutils")
    val spark = SparkSession.builder().master("local[*]")
      .appName("Database connection").getOrCreate()
    /*  logger.info("Spark intialized")
        logger.info("Git trail")
        logger.info("Again updating")
        val inputdata1 = spark.read.csv("file:///C:/revenue_shipfrom.csv")
        logger.info("Data loaded")
        inputdata1.show()*/
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
 def headerCheck(file:String) = {
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


  def isNull(dataFrame: DataFrame): DataFrame = {

    // If we need to drop all the columns which has null
    val nonnullDF = dataFrame.na.drop("any")
    // If we need to drop any specific columns
    val nonullDF1 = dataFrame.na.drop(Array("Column name"))

   nonnullDF


  }


}
