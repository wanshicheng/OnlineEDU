package com.skillip.onlineedu.member

import org.apache.spark.sql.SparkSession

object BaseAd2Dwd {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]")
      .appName(" Member ETL")
      .enableHiveSupport()
      .getOrCreate()

    val inputRDD = spark.sparkContext.textFile("hdfs://hadoop101:9000/user/skillip/ods/baseadlog.log")
//    inputRDD.map()
  }
}
