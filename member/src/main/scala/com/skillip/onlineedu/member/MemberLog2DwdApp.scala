package com.skillip.onlineedu.member

import java.text.SimpleDateFormat

import com.alibaba.fastjson.{JSON, JSONObject}
import com.skillip.onlineedu.member.bean.Member
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object MemberLog2DwdApp {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "wanshicheng")
//    val conf = new SparkConf().setAppName("Member ETL").setMaster("local[*]")
//    val sc = new SparkContext(conf)
//    val inputRDD = sc.textFile("hdfs://hadoop101:9000/user/skillip/ods/member.log")

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(" Member ETL")
      .enableHiveSupport()
//      .config("spark.sql.warehouse.dir", "hdfs://hadoop101:9000/user/hive/warehouse/dwd.db")
      .getOrCreate()
    val inputRDD = spark.sparkContext.textFile("hdfs://hadoop101:9000/user/skillip/ods/member.log")
    import spark.implicits._
    val objRDD = inputRDD.map(str => {
      val ml = JSON.parseObject(str, classOf[Member])
      memberLogETL(ml)
    })
//    objRDD.toDF().createOrReplaceTempView("tmp_member")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set hive.exec.dynamic.partition=true")
//    spark.sql(
//
//        |INSERT INTO TABLE dwd.dwd_member
//        |SELECT * FROM tmp_member
//      """.stripMargin)
//    objRDD.toDF().formatted("snappy")
    objRDD.toDF().as[Member].write.mode(SaveMode.Append).insertInto("dwd.dwd_member")
//    objRDD.take(10).foreach(println)
  }

//  def insertIntoHive(spark: SparkSession, dbName:String, tableName: String, df: DataFrame) = {
//    spark.sql(s"USE $dbName")
//    spark.sql("INSERT INTO")
//    df.write.saveAsTable(tableName)
//  }


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
