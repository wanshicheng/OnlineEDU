package com.skillip.onlineedu.qz.dao

import org.apache.spark.sql.SparkSession

object DwdQzDao {
  def getDwdQzPoint(spark: SparkSession) = {
    spark.sql(
      """
        |SELECT
        |pointid,
        |courseid,
        |pointname,
        |pointyear,
        |chapter,
        |creator,
        |createtme,
        |status,
        |modifystatus,
        |excisenum,
        |pointlistid,
        |chapterid,
        |sequence,
        |pointdescribe,
        |pointlevel,
        |typelist,
        |score AS point_score,
        |thought,
        |remid,
        |pointnamelist,
        |typelistids,
        |pointlist,
        |dt,
        |dn
        |FROM dwd.dwd_qz_point
      """.stripMargin)
  }

  def getDwdQzChapterList (spark: SparkSession) = {
    spark.sql(
      """
        |SELECT
        |chapterlistid,
        |chapterlistname,
        |chapterallnum,
        |dn
        |FROM dwd.dwd_qz_chapter_list
      """.stripMargin)
  }

  def getDwdQzChapter(spark: SparkSession) = {
    spark.sql(
      """
        |SELECT
        |chapterid,
        |chapterlistid,
        |chaptername,
        |chapternum,
        |showstatus,
        |outchapterid,
        |creator as chapter_creator,
        |createtime as chapter_createtime,
        |courseid as chapter_courseid,
        |dn
        |FROM dwd.dwd_qz_chapter
      """.stripMargin)
  }

  def getDwdQzPointQuestion(spark: SparkSession) = {
    spark.sql(
      """
        |SELECT
        |pointid,
        |questionid,
        |questype,
        |dn
        |FROM dwd.dwd_qz_point_question
      """.stripMargin)
  }

  def getDwdQzSiteCourse(spark: SparkSession) = {
    spark.sql(
      """
        |SELECT
        |sitecourseid,
        |courseid,
        |sitecoursename,
        |coursechapter,
        |sequence,
        |status,
        |creator,
        |createtime,
        |helppaperstatus,
        |servertype,
        |boardid,
        |showstatus,
        |dt,
        |dn
        |FROM
        |dwd.dwd_qz_site_course
      """.stripMargin)
  }

  def getDwdQzCourse(spark: SparkSession) = {
    spark.sql(
      """
        |SELECT
        |courseid,
        |majorid,
        |coursename,
        |coursechapter,
        |isadvc,
        |chapterlistid,
        |pointlistid,
        |dn
      """.stripMargin)
  }

  def getDwdQzCourseEdusubject(spark: SparkSession) = {
    spark.sql(
      """
        |SELECT
        |courseeduid,
        |edusubjectid,
        |courseid,
        |creator,
        |createtime,
        |majorid,
        |dn
      """.stripMargin)
  }
}
