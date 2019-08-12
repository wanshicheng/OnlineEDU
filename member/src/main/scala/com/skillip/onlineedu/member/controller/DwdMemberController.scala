package com.skillip.onlineedu.member.controller

import com.skillip.onlineedu.common.util.HiveUtil
import com.skillip.onlineedu.member.service.EtlDataService
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DwdMemberController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "wanshicheng")
    val conf = new SparkConf().setAppName("dwd_member_import").setMaster("local[*]")
    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
    val ssc = spark.sparkContext

    HiveUtil.openDynamicPartition(spark)
    HiveUtil.openCompression(spark)
    HiveUtil.useSnappyCompression(spark)

//    EtlDataService.etlBaseWebsite(ssc, spark)
//    EtlDataService.etlPcentermempaymoney(ssc, spark)
//    EtlDataService.etlVipLevel(ssc, spark)
    EtlDataService.etlBaseAd(ssc, spark)
    spark.close()
  }
}
