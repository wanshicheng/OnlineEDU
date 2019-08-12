package com.skillip.onlineedu.member

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SparkSession

object MyTest {
  def main(args: Array[String]): Unit = {
//    val str = "13079893158"
//    val ss = str.replaceAll("(\\d{3})\\d{4}(\\d{4})", "$1****$2")
//    println(ss)

//    val sFormat = new SimpleDateFormat("yyyy-MM-dd")
//    val dFormat = new SimpleDateFormat("yyyyMMdd")

    val format = new SimpleDateFormat("yyyy-MM-dd")
    println(format.parse("2017-09-09").getTime.toString)

  }
}
