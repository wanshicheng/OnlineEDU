package com.skillip.onlineedu.member

import java.text.SimpleDateFormat

import com.alibaba.fastjson.{JSON, JSONPObject}
import com.skillip.onlineedu.member.bean.MemberRegtype
import org.apache.spark.sql.{SaveMode, SparkSession}

object MemberRegtype2Dwd {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "wanshicheng")
    val spark = SparkSession.builder()
      .appName("Member Regtype to DWD")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    val inputRDD = spark.sparkContext.textFile("hdfs://hadoop101:9000/user/skillip/ods/memberRegtype.log")
    import spark.implicits._
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set hive.exec.dynamic.partition=true")
//    inputRDD.map(json => {
//      val memberRegtype = JSON.parseObject(json, classOf[MemberRegtype])
//      memberRegtypeETL(memberRegtype)
//    }).toDS().write.mode("append").insertInto("dwd.dwd_member_regtype")

    inputRDD.map(json => {
      JSON.parseObject(json, classOf[MemberRegtype])

    })
//      .take(10).foreach(println)
      .toDS().write.mode(SaveMode.Append).insertInto("dwd.dwd_member_regtype")
  }

//  def memberRegtypeETL(memberRegtype: MemberRegtype) = {
//    val format = new SimpleDateFormat("yyyy-MM-dd")
//    val createtime = format.parse(memberRegtype.createtime).getTime.toString
//
//    new MemberRegtype(memberRegtype.uid,
//      memberRegtype.appkey,
//      memberRegtype.appregurl,
//      memberRegtype.bdp_uuid,
//      createtime,
//      memberRegtype.domain,
//      memberRegtype.isranreg,
//      memberRegtype.regsource,
//      memberRegtype.regsourcename,
//      memberRegtype.websiteid,
//      memberRegtype.dt,
//      memberRegtype.dn)
//  }
}
