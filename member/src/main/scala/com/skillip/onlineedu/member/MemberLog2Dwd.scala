package com.skillip.onlineedu.member

import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.skillip.onlineedu.member.bean.Member
import org.apache.spark.sql.SparkSession

object MemberLog2Dwd {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]")
      .appName(" Member ETL")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val inputRDD = spark.sparkContext.textFile("hdfs://hadoop101:9000/user/skillip/ods/baseadlog.log")
//    inputRDD.map(ml => {
//      memberLogETL(ml)
//    }).toDS().write.mode("append").insertInto("dwd.dwd_member")
//    inputRDD.map(row => {
////      val json = row.toString()
////      val mb = JSON.parseObject(json, classOf[MemberLog])
////      mb
//      row.getString(3)
//    }).take(10).foreach(println)
  }

  def memberLogETL(memberLog: Member) = {
    val fullname = memberLog.fullname.charAt(0) + "XX"
    val phone = memberLog.phone.replaceAll("(\\d{3})\\d{4}(\\d{4})", "$1****$2")
    val format = new SimpleDateFormat("")

    val sFormat = new SimpleDateFormat("yyyy-MM-dd")
    val dFormat = new SimpleDateFormat("yyyyMMdd")
    val birthday = dFormat.format(sFormat.parse(memberLog.birthday))
    val register = dFormat.format(sFormat.parse(memberLog.register))
    val ml = Member(memberLog.uid,
      memberLog.ad_id,
      birthday,
      memberLog.email,
      fullname,
      memberLog.iconurl,
      memberLog.lastlogin,
      memberLog.mailaddr,
      memberLog.memberlevel,
      "******",
      memberLog.paymoney,
      phone: String,
      memberLog.qq,
      register,
      memberLog.regupdatetime,
      memberLog.unitname,
      memberLog.userip,
      memberLog.zipcode,
      memberLog.dt,
      memberLog.dn)
    ml
  }
}
