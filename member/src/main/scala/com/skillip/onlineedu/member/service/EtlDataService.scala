package com.skillip.onlineedu.member.service

import com.alibaba.fastjson.JSON
import com.skillip.onlineedu.common.util.ParseJsonData
import com.skillip.onlineedu.member.bean.{BaseAd, BaseWebsite, Pcentermempaymoney, VipLevel}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}


object EtlDataService {
  def etlBaseAd(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD = ssc.textFile("hdfs://hadoop101:9000/user/skillip/ods/baseadlog.log")
    import spark.implicits._
    inputRDD.filter(json => ParseJsonData.getJsonData(json).isDefined)
      .map(json => {
        JSON.parseObject(json, classOf[BaseAd])
      }).toDS().write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_ad")
  }

  def etlBaseWebsite(ssc:SparkContext, spark: SparkSession) = {
    val inputRDD = ssc.textFile("hdfs://hadoop101:9000/user/skillip/ods/baswewebsite.log")
    import spark.implicits._
    inputRDD.filter(json => ParseJsonData.getJsonData(json).isDefined)
      .map(json => {
        JSON.parseObject(json, classOf[BaseWebsite])
      }).toDS().write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_website")
  }

  def etlPcentermempaymoney(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD = ssc.textFile("hdfs://hadoop101:9000/user/skillip/ods/pcentermempaymoney.log")
    import spark.implicits._
    inputRDD.filter(json => ParseJsonData.getJsonData(json).isDefined)
      .map(json => {
        JSON.parseObject(json, classOf[Pcentermempaymoney])
      }).toDS().write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_pcentermempaymoney")
  }

  def etlVipLevel(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD = ssc.textFile("hdfs://hadoop101:9000/user/skillip/ods/pcenterMemViplevel.log")
    import spark.implicits._
    inputRDD.filter(json => ParseJsonData.getJsonData(json).isDefined)
      .map(json => {
        JSON.parseObject(json, classOf[VipLevel])
      }).toDS().write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_vip_level") // 如果 vipLevel 不是样例类，还没法用 tods
  }
}

