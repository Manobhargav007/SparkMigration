package com.coke.nsrV3.Logging
import org.apache.log4j.Logger

object Logging {

  val logger = Logger.getLogger(this.getClass.getName)
  def main(args: Array[String]): Unit = {
    logger.info("Logger : Welcome to log4j")
}




}