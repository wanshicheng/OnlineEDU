package com.skillip.onlineedu.qz.controller

import com.skillip.onlineedu.common.util.HiveUtil
import com.skillip.onlineedu.qz.service.EtlQzService
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DwdQzController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "wanshicheng")
    val conf = new SparkConf().setAppName("dwd_qz_import").setMaster("local[*]")
    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
    val ssc = spark.sparkContext

    HiveUtil.openDynamicPartition(spark)
    HiveUtil.openCompression(spark)
    HiveUtil.useSnappyCompression(spark)

//    EtlQzService.etlQzChapterLog(ssc, spark)
//    EtlQzService.etlQzChapterListLog(ssc, spark)
//    EtlQzService.etlQzPointLog(ssc, spark)
//      EtlQzService.etlQzPointQuestion(ssc, spark)
//    EtlQzService.etlQzSiteCource(ssc, spark)
//    EtlQzService.etlQzCourse(ssc, spark)
//    EtlQzService.etlQzCourseEdusubjectLog(ssc, spark)
//    EtlQzService.etlQzWebsiteLog(ssc, spark)
//    EtlQzService.etlQzMajorLog(ssc, spark)
//    EtlQzService.etlQzBusinessLog(ssc, spark)
//    EtlQzService.etlQzPaperView(ssc, spark)
//    EtlQzService.etlQzCenterPaper(ssc, spark)
//    EtlQzService.etlQzPaper(ssc, spark)
//    EtlQzService.etlQzCenter(ssc, spark)
//    EtlQzService.etlQzQuestionLog(ssc, spark)
//    EtlQzService.etlQzQuestionTypeLog(ssc, spark)
    EtlQzService.etlQzMemberPaperQuestion(ssc, spark)
  }
}
