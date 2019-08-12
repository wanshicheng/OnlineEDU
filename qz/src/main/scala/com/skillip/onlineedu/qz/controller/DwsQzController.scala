package com.skillip.onlineedu.qz.controller
import com.skillip.onlineedu.common.util.HiveUtil
import com.skillip.onlineedu.qz.service.DwdQzService
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DwsQzController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "wanshicheng")
    val conf = new SparkConf().setAppName("dwd_qz_import").setMaster("local[*]")
    val spark = SparkSession.builder()
      .config(conf)
      .config("spark.debug.maxToStringFields", "100")
      .enableHiveSupport()
      .getOrCreate()
    val ssc = spark.sparkContext

    HiveUtil.openDynamicPartition(spark)
    HiveUtil.openCompression(spark)
    HiveUtil.useSnappyCompression(spark)

    DwdQzService.importQzChapter(spark, "20190722")
  }
}
