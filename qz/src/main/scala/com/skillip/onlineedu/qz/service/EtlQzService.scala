package com.skillip.onlineedu.qz.service

import java.text.DecimalFormat

import com.alibaba.fastjson.JSON
import com.skillip.onlineedu.common.util.ParseJsonData
import com.skillip.onlineedu.qz.bean.{QzPaperView, QzPoint, QzQuestion}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

object EtlQzService {
  def etlQzChapterLog(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD = ssc.textFile("hdfs://hadoop101:9000/user/skillip/ods/QzChapter.log")
    import spark.implicits._
    inputRDD.filter(json => ParseJsonData.getJsonData(json).isDefined)
      .mapPartitions(partition => {
        partition.map(json => {
          val obj = ParseJsonData.getJsonData(json).get

          val chapterid = obj.getIntValue("chapterid")
          val chapterlistid = obj.getIntValue("chapterlistid")
          val chaptername = obj.getString("chaptername")
          val sequence = obj.getString("sequence")
          val showstatus = obj.getString("showstatus")
          val creator = obj.getString("creator")
          val createtime = obj.getString("createtime")
          val courseid = obj.getIntValue("courseid")
          val chapternum = obj.getIntValue("chapternum")
          val outchapterid = obj.getIntValue("outchapterid")
          val dt = obj.getString("dt")
          val dn = obj.getString("dn")

          (chapterid, chapterlistid, chaptername, sequence, showstatus, creator, createtime, courseid, chapternum, outchapterid,
            dt, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_chapter")
  }

  def etlQzChapterListLog(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD = ssc.textFile("hdfs://hadoop101:9000/user/skillip/ods/QzChapterList.log")
    import spark.implicits._
    inputRDD.filter(json => ParseJsonData.getJsonData(json).isDefined)
      .mapPartitions(partition => {
        partition.map(json => {
          val obj = ParseJsonData.getJsonData(json).get

          val chapterlistid = obj.getIntValue("chapterlistid")
          val chapterlistname = obj.getString("chapterlistname")
          val courseid = obj.getIntValue("courseid")
          val chapterallnum = obj.getIntValue("chapterallnum")
          val sequence = obj.getString("sequence")
          val status = obj.getString("status")
          val creator = obj.getString("creator")
          val createtime = obj.getString("createtime")
          val dt = obj.getString("dt")
          val dn = obj.getString("dn")

          (chapterlistid, chapterlistname, courseid, chapterallnum, sequence, status,
            creator, createtime, dt, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_chapter_list")
  }


  private val format = new DecimalFormat("0.0")

  def etlQzPointLog(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD = ssc.textFile("hdfs://hadoop101:9000/user/skillip/ods/QzPoint.log")
    import spark.implicits._
    inputRDD.filter(json => ParseJsonData.getJsonData(json).isDefined)
      .mapPartitions(partition => {
        partition.map(json => {
          val obj = ParseJsonData.getJsonData(json).get

          val pointid = obj.getIntValue("pointid")
          val courseid = obj.getIntValue("courseid")
          val pointname = obj.getString("pointname")
          val pointyear = obj.getString("pointyear")
          val chapter = obj.getString("chapter")
          val creator = obj.getString("creator")
          val createtme = obj.getString("createtme")
          val status = obj.getString("status")
          val modifystatus = obj.getString("modifystatus")
          val excisenum = obj.getIntValue("excisenum")
          val pointlistid = obj.getIntValue("pointlistid")
          val chapterid = obj.getIntValue("chapterid")
          val sequence = obj.getString("sequence")
          val pointdescribe = obj.getString("pointdescribe")
          val pointlevel = obj.getString("pointlevel")
          val typelist = obj.getString("typelist")
          val df = format
          val score = df.format(obj.getDoubleValue("score")).toDouble
          val thought = obj.getString("thought")
          val remid = obj.getString("remid")
          val pointnamelist = obj.getString("pointnamelist")
          val typelistids = obj.getString("typelistids")
          val pointlist = obj.getString("pointlist")
          val dt = obj.getString("dt")
          val dn = obj.getString("dn")

          QzPoint(pointid, courseid, pointname, pointyear, chapter, creator, createtme, status, modifystatus,
            excisenum, pointlistid, chapterid, sequence, pointdescribe, pointlevel, typelist, score, thought,
            remid, pointnamelist, typelistids, pointlist, dt, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_point")
  }


  def etlQzPointQuestion(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD = ssc.textFile("hdfs://hadoop101:9000/user/skillip/ods/QzPointQuestion.log")
    import spark.implicits._
    inputRDD.filter(json => ParseJsonData.getJsonData(json).isDefined)
      .mapPartitions(partition => {
        partition.map(json => {
          val obj = ParseJsonData.getJsonData(json).get
          val pointid = obj.getIntValue("pointid")
          val questionid = obj.getIntValue("questionid")
          val questype = obj.getIntValue("questype")
          val creator = obj.getString("creator")
          val createtime = obj.getString("createtime")
          val dt = obj.getString("dt")
          val dn = obj.getString("dn")

          (pointid, questionid, questype, creator, createtime, dt, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_point_question")
  }


  def etlQzSiteCource(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD = ssc.textFile("hdfs://hadoop101:9000/user/skillip/ods/QzSiteCourse.log")
    import spark.implicits._
    inputRDD.filter(json => ParseJsonData.getJsonData(json).isDefined)
      .mapPartitions(partition => {
        partition.map(json => {
          val obj = ParseJsonData.getJsonData(json).get
          val sitecourseid = obj.getIntValue("sitecourseid")
          val siteid = obj.getIntValue("siteid")
          val courseid = obj.getIntValue("courseid")
          val sitecoursename = obj.getString("sitecoursename")
          val coursechapter = obj.getString("coursechapter")
          val sequence = obj.getString("sequence")
          val status = obj.getString("status")
          val creator = obj.getString("creator")
          val createtime = obj.getString("createtime")
          val helppaperstatus = obj.getString("helppaperstatus")
          val servertype = obj.getString("servertype")
          val boardid = obj.getIntValue("boardid")
          val showstatus = obj.getString("showstatus")
          val dt = obj.getString("dt")
          val dn = obj.getString("dn")

          (sitecourseid, siteid, courseid, sitecoursename, coursechapter, sequence, status, creator, createtime, helppaperstatus, servertype, boardid, showstatus,
            dt, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_site_course")
  }


  def etlQzCourse(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD = ssc.textFile("hdfs://hadoop101:9000/user/skillip/ods/QzCourse.log")
    import spark.implicits._
    inputRDD.filter(json => ParseJsonData.getJsonData(json).isDefined)
      .mapPartitions(partition => {
        partition.map(json => {
          val obj = ParseJsonData.getJsonData(json).get
          val courseid = obj.getIntValue("courseid")
          val majorid = obj.getInteger("majorid")
          val coursename = obj.getString("coursename")
          val coursechapter = obj.getString("coursechapter")
          val sequence = obj.getString("sequence")
          val isadvc = obj.getString("isadvc")
          val creator = obj.getString("creator")
          val createtime = obj.getString("createtime")
          val status = obj.getString("status")
          val chapterlistid = obj.getIntValue("chapterlistid")
          val pointlistid = obj.getIntValue("pointlistid")
          val dt = obj.getString("dt")
          val dn = obj.getString("dn")

          (courseid, majorid, coursename, coursechapter, sequence, isadvc, creator, createtime, status, chapterlistid, pointlistid,
            dt, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_course")
  }

  def etlQzCourseEdusubjectLog(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD = ssc.textFile("hdfs://hadoop101:9000/user/skillip/ods/QzCourseEduSubject.log")
    import spark.implicits._
    inputRDD.filter(json => ParseJsonData.getJsonData(json).isDefined)
      .mapPartitions(partition => {
        partition.map(json => {
          val obj = ParseJsonData.getJsonData(json).get
          val courseeduid = obj.getIntValue("courseeduid")
          val edusubjectid = obj.getIntValue("edusubjectid")
          val courseid = obj.getIntValue("courseid")
          val creator = obj.getString("creator")
          val createtime = obj.getString("createtime")
          val majorid = obj.getIntValue("majorid")
          val dt = obj.getString("dt")
          val dn = obj.getString("dn")

          (courseeduid, edusubjectid, courseid, creator, createtime, majorid, dt, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_course_edusubject")
  }


  def etlQzWebsiteLog(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD = ssc.textFile("hdfs://hadoop101:9000/user/skillip/ods/QzWebsite.log")
    import spark.implicits._
    inputRDD.filter(json => ParseJsonData.getJsonData(json).isDefined)
      .mapPartitions(partition => {
        partition.map(json => {
          val obj = ParseJsonData.getJsonData(json).get
          val siteid = obj.getIntValue("siteid")
          val sitename = obj.getString("sitename")
          val domain = obj.getString("domain")
          val sequence = obj.getString("sequence")
          val multicastserver = obj.getString("multicastserver")
          val templateserver = obj.getString("templateserver")
          val status = obj.getString("status")
          val creator = obj.getString("creator")
          val createtime = obj.getString("createtime")
          val multicastgateway = obj.getString("multicastgateway")
          val multicastport = obj.getString("multicastport")
          val dt = obj.getString("dt")
          val dn = obj.getString("dn")

          (siteid, sitename, domain, sequence, multicastserver, templateserver, status, creator, createtime, multicastgateway, multicastport,
            dt, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_website")
  }

  def etlQzMajorLog(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD = ssc.textFile("hdfs://hadoop101:9000/user/skillip/ods/QzMajor.log")
    import spark.implicits._
    inputRDD.filter(json => ParseJsonData.getJsonData(json).isDefined)
      .mapPartitions(partition => {
        partition.map(json => {
          val obj = ParseJsonData.getJsonData(json).get
          val majorid = obj.getIntValue("majorid")
          val businessid = obj.getIntValue("businessid")
          val siteid = obj.getIntValue("siteid")
          val majorname = obj.getString("majorname")
          val shortname = obj.getString("shortname")
          val status = obj.getString("status")
          val sequence = obj.getString("sequence")
          val creator = obj.getString("creator")
          val createtime = obj.getString("createtime")
          val column_sitetype = obj.getString("column_sitetype")
          val dt = obj.getString("dt")
          val dn = obj.getString("dn")

          (majorid, businessid, siteid, majorname, shortname, status, sequence, creator, createtime, column_sitetype,
            dt, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_major")
  }

  def etlQzBusinessLog(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD = ssc.textFile("hdfs://hadoop101:9000/user/skillip/ods/QzBusiness.log")
    import spark.implicits._
    inputRDD.filter(json => ParseJsonData.getJsonData(json).isDefined)
      .mapPartitions(partition => {
        partition.map(json => {
          val obj = ParseJsonData.getJsonData(json).get
          val businessid = obj.getIntValue("businessid")
          val businessname = obj.getString("businessid")
          val sequence = obj.getString("sequence")
          val status = obj.getString("status")
          val creator = obj.getString("creator")
          val createtime = obj.getString("createtime")
          val siteid = obj.getIntValue("siteid")
          val dt = obj.getString("dt")
          val dn = obj.getString("dn")

          (businessid, businessname, sequence, status, creator, createtime, siteid, dt, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_business")
  }

  def etlQzPaperView(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD = ssc.textFile("hdfs://hadoop101:9000/user/skillip/ods/QzPaperView.log")
    import spark.implicits._
    inputRDD.filter(json => ParseJsonData.getJsonData(json).isDefined)
      .mapPartitions(partition => {
        partition.map(json => {
          val obj = ParseJsonData.getJsonData(json).get
          val paperviewid = obj.getIntValue("paperviewid")
          val paperid = obj.getIntValue("paperid")
          val paperviewname = obj.getString("paperviewname")
          val paperparam = obj.getString("paperparam")
          val openstatus = obj.getString("openstatus")
          val explainurl = obj.getString("explainurl")
          val iscontest = obj.getString("iscontest")
          val contesttime = obj.getString("contesttime")
          val conteststarttime = obj.getString("conteststarttime")
          val contestendtime = obj.getString("contestendtime")
          val contesttimelimit = obj.getString("contesttimelimit")
          val dayiid = obj.getIntValue("dayiid")
          val status = obj.getString("status")
          val creator = obj.getString("creator")
          val createtime = obj.getString("createtime")
          val paperviewcatid = obj.getIntValue("paperviewcatid")
          val modifystatus = obj.getString("modifystatus")
          val description = obj.getString("description")
          val papertype = obj.getString("papertype")
          val downurl = obj.getString("downurl")
          val paperuse = obj.getString("paperuse")
          val paperdifficult = obj.getString("paperdifficult")
          val testreport = obj.getString("testreport")
          val paperuseshow = obj.getString("paperuseshow")
          val dt = obj.getString("dt")
          val dn = obj.getString("dn")

          QzPaperView(paperviewid,
            paperid,
            paperviewname,
            paperparam,
            openstatus,
            explainurl,
            iscontest,
            contesttime,
            conteststarttime,
            contestendtime,
            contesttimelimit,
            dayiid: Int,
            status,
            creator,
            createtime,
            paperviewcatid,
            modifystatus,
            description,
            papertype,
            downurl,
            paperuse,
            paperdifficult,
            testreport,
            paperuseshow,
            dt,
            dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_paper_view")
  }


  def etlQzCenterPaper(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD = ssc.textFile("hdfs://hadoop101:9000/user/skillip/ods/QzCenterPaper.log")
    import spark.implicits._
    inputRDD.filter(json => ParseJsonData.getJsonData(json).isDefined)
      .mapPartitions(partition => {
        partition.map(json => {
          val obj = ParseJsonData.getJsonData(json).get
          val paperviewid = obj.getIntValue("paperviewid")
          val centerid = obj.getIntValue("centerid")
          val openstatus = obj.getString("openstatus")
          val sequence = obj.getString("sequence")
          val creator = obj.getString("creator")
          val createtime = obj.getString("createtime")
          val dt = obj.getString("dt")
          val dn = obj.getString("dn")

          (paperviewid, centerid, openstatus, sequence, creator, createtime, dt, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_center_paper")
  }





  def etlQzPaper(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD = ssc.textFile("hdfs://hadoop101:9000/user/skillip/ods/QzPaper.log")
    import spark.implicits._
    inputRDD.filter(json => ParseJsonData.getJsonData(json).isDefined)
      .mapPartitions(partition => {
        partition.map(json => {
          val obj = ParseJsonData.getJsonData(json).get
          val paperid = obj.getIntValue("paperid")
          val papercatid = obj.getIntValue("papercatid")
          val courseid = obj.getIntValue("courseid")
          val paperyear = obj.getString("paperyear")
          val chapter = obj.getString("chapter")
          val suitnum = obj.getString("suitnum")
          val papername = obj.getString("papername")
          val status = obj.getString("status")
          val creator = obj.getString("creator")
          val createtime = obj.getString("createtime")
          val totalscore = format.format(obj.getDouble("totalscore")).toDouble
          val chapterid = obj.getIntValue("chapterid")
          val chapterlistid = obj.getIntValue("chapterlistid")
          val dt = obj.getString("dt")
          val dn = obj.getString("dn")

          (paperid, papercatid, courseid, paperyear, chapter, suitnum, papername, status, creator, createtime, totalscore, chapterid,
          chapterlistid, dt, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_paper")
  }




  def etlQzCenter(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD = ssc.textFile("hdfs://hadoop101:9000/user/skillip/ods/QzCenter.log")
    import spark.implicits._
    inputRDD.filter(json => ParseJsonData.getJsonData(json).isDefined)
      .mapPartitions(partition => {
        partition.map(json => {
          val obj = ParseJsonData.getJsonData(json).get
          val centerid  = obj.getIntValue("centerid")
          val centername = obj.getString("centername")
          val centeryear = obj.getString("centeryear")
          val centertype = obj.getString("centertype")
          val openstatus = obj.getString("openstatus")
          val centerparam = obj.getString("centerparam")
          val description = obj.getString("description")
          val creator = obj.getString("creator")
          val createtime = obj.getString("createtime")
          val sequence = obj.getString("sequence")
          val provideuser = obj.getString("provideuser")
          val centerviewtype = obj.getString("centerviewtype")
          val stage = obj.getString("stage")
          val dt = obj.getString("dt")
          val dn = obj.getString("dn")

          (centerid, centername, centeryear, centertype, openstatus, centerparam, description, creator, createtime, sequence, provideuser,
          centerviewtype, stage, dt, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_center")
  }



  def etlQzQuestionLog(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD = ssc.textFile("hdfs://hadoop101:9000/user/skillip/ods/QzQuestion.log")
    import spark.implicits._
    inputRDD.filter(json => ParseJsonData.getJsonData(json).isDefined)
      .mapPartitions(partition => {
        partition.map(json => {
          val obj = ParseJsonData.getJsonData(json).get
          val questionid = obj.getIntValue("questionid")
          val parentid = obj.getIntValue("parentid")
          val questypeid = obj.getIntValue("questypeid")
          val quesviewtype = obj.getIntValue("quesviewtype")
          val content = obj.getString("content")
          val answer = obj.getString("answer")
          val analysis = obj.getString("analysis")
          val limitminute = obj.getString("limitminute")
          val score = format.format(obj.getDouble("score")).toDouble
          val splitscore = format.format(obj.getDouble("splitscore")).toDouble
          val status = obj.getString("status")
          val optnum = obj.getIntValue("optnum")
          val lecture = obj.getString("lecture")
          val creator = obj.getString("creator")
          val createtime = obj.getString("createtime")
          val modifystatus = obj.getString("modifystatus")
          val attanswer = obj.getString("attanswer")
          val questag = obj.getString("questag")
          val vanalysisaddr = obj.getString("vanalysisaddr")
          val difficulty = obj.getString("difficulty")
          val quesskill = obj.getString("quesskill")
          val vdeoaddr = obj.getString("vdeoaddr")
          val dt = obj.getString("dt")
          val dn = obj.getString("dn")

          QzQuestion(questionid, parentid, questypeid, quesviewtype, content, answer, analysis, limitminute, score, splitscore, status, optnum, lecture, creator, createtime,
            modifystatus, attanswer, questag, vanalysisaddr, difficulty, quesskill, vdeoaddr, dt, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_question")
  }


  def etlQzCenterPaper1(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD = ssc.textFile("hdfs://hadoop101:9000/user/skillip/ods/QzCenterPaper.log")
    import spark.implicits._
    inputRDD.filter(json => ParseJsonData.getJsonData(json).isDefined)
      .mapPartitions(partition => {
        partition.map(json => {
          val obj = ParseJsonData.getJsonData(json).get
          val paperviewid = obj.getIntValue("paperviewid")
          val centerid = obj.getIntValue("centerid")
          val openstatus = obj.getString("openstatus")
          val sequence = obj.getString("sequence")
          val creator = obj.getString("creator")
          val createtime = obj.getString("createtime")
          val dt = obj.getString("dt")
          val dn = obj.getString("dn")

          (paperviewid, centerid, openstatus, sequence, creator, createtime, dt, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_center_paper")
  }



  def etlQzQuestionTypeLog(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD = ssc.textFile("hdfs://hadoop101:9000/user/skillip/ods/QzQuestionType.log")
    import spark.implicits._
    inputRDD.filter(json => ParseJsonData.getJsonData(json).isDefined)
      .mapPartitions(partition => {
        partition.map(json => {
          val obj = ParseJsonData.getJsonData(json).get
          val quesviewtype = obj.getIntValue("quesviewtype")
          val viewtypename = obj.getString("viewtypename")
          val questypeid = obj.getIntValue("questypeid")
          val description = obj.getString("description")
          val status = obj.getString("status")
          val creator = obj.getString("creator")
          val createtime = obj.getString("createtime")
          val papertypename = obj.getString("papertypename")
          val sequence = obj.getString("sequence")
          val remark = obj.getString("remark")
          val splitscoretyp = obj.getString("splitscoretyp")
          val dt = obj.getString("dt")
          val dn = obj.getString("dn")

          (quesviewtype, viewtypename, questypeid, description, status, creator, createtime, papertypename, sequence, remark, splitscoretyp, dt, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_question_type")
  }


  def etlQzMemberPaperQuestion(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD = ssc.textFile("hdfs://hadoop101:9000/user/skillip/ods/QzMemberPaperQuestion.log")
    import spark.implicits._
    inputRDD.filter(json => ParseJsonData.getJsonData(json).isDefined)
      .mapPartitions(partition => {
        partition.map(json => {
          val obj = ParseJsonData.getJsonData(json).get
          val userid = obj.getIntValue("userid")
          val paperviewid = obj.getIntValue("paperviewid")
          val chapterid = obj.getIntValue("chapterid")
          val sitecourseid = obj.getIntValue("sitecourseid")
          val questionid = obj.getIntValue("questionid")
          val majorid = obj.getIntValue("majorid")
          val useranswer = obj.getString("useranswer")
          val istrue = obj.getString("istrue")
          val lasttime = obj.getString("lasttime")
          val opertype = obj.getString("opertype")
          val paperid = obj.getIntValue("paperid")
          val spendtime = obj.getIntValue("spendtime")
          val score = format.format(obj.getDouble("score")).toDouble
          val question_answer = obj.getIntValue("question_answer")
          val dt = obj.getString("dt")
          val dn = obj.getString("dn")

          (userid, paperviewid, chapterid, sitecourseid, questionid, majorid, useranswer, istrue, lasttime, opertype, paperid, spendtime, score, question_answer, dt, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_member_paper_question")
  }

  def etlQzCenterPaper111(ssc: SparkContext, spark: SparkSession) = {
    val inputRDD = ssc.textFile("hdfs://hadoop101:9000/user/skillip/ods/QzCenterPaper.log")
    import spark.implicits._
    inputRDD.filter(json => ParseJsonData.getJsonData(json).isDefined)
      .mapPartitions(partition => {
        partition.map(json => {
          val obj = ParseJsonData.getJsonData(json).get
          val paperviewid = obj.getIntValue("paperviewid")
          val centerid = obj.getIntValue("centerid")
          val openstatus = obj.getString("openstatus")
          val sequence = obj.getString("sequence")
          val creator = obj.getString("creator")
          val createtime = obj.getString("createtime")
          val dt = obj.getString("dt")
          val dn = obj.getString("dn")

          (paperviewid, centerid, openstatus, sequence, creator, createtime, dt, dn)
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_center_paper")
  }

}
