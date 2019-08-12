package com.skillip.onlineedu.member.controller

import com.skillip.onlineedu.common.util.HiveUtil
import com.skillip.onlineedu.member.service.AdsMemberService
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AdsMemberController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "wanshicheng")
    val sparkConf = new SparkConf().setAppName("ads_member_controller").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区

    AdsMemberService.queryDetailApi(sparkSession, "20190722")
  }
}
