package com.skillip.onlineedu.member.controller

import com.skillip.onlineedu.common.util.HiveUtil
import com.skillip.onlineedu.member.service.DwsMemberService
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DwsMemberController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "wanshicheng")
    val conf = new SparkConf()
      .setAppName("dws_member_import")
      .setMaster("local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    HiveUtil.openDynamicPartition(spark)
    HiveUtil.openCompression(spark)
    HiveUtil.useSnappyCompression(spark)

//    DwsMemberService.importMemberUseApi(spark, "20190722")
//    DwsMemberService.importMember(spark, "20190722")
    DwsMemberService.importMemberZip(spark, "20190722")

  }
}
