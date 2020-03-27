package com.coke.nsrV3

/**
 * Hello world!
 *
 */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class SparkBasic {

    System.setProperty("hadoop.home.dir","c:/winutils")
    val spark = SparkSession
      .builder().master("local[*]")
      .appName("SparkSessionZipsExample")
      .getOrCreate()
println("This is coming")


}