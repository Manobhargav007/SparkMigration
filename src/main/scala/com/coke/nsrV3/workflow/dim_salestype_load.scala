package com.coke.nsrV3.workflow

import com.coke.nsrV3.commons.Functions

object dim_salestype_load extends Functions {

  // Start the spark session

  spark_Session()
  val config_DF = loadConfigTable("//", "", "", "")
  val if_Header = headerCheck(config_DF, "file/location")
  val inputdata = readData("", if_Header)
  showData(inputdata)
  printSchema(inputdata)
  //val data_with_no_duplicates =DF_with_no_duplicates(inputdata,"C1")
  //val duplicate_data =DF_with_Duplicates(inputdata)
  //val DF_with_out_nulls = nullcheck(duplicate_data)



}
