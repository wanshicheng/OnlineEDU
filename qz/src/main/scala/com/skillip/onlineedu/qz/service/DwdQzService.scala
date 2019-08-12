package com.skillip.onlineedu.qz.service

import com.skillip.onlineedu.qz.bean.QzChapter
import com.skillip.onlineedu.qz.dao.DwdQzDao
import org.apache.spark.sql.{SaveMode, SparkSession}

object DwdQzService {
  def importQzChapter(spark: SparkSession, dt: String) = {
    import spark.implicits._
    val dwdQzPoint = DwdQzDao.getDwdQzPoint(spark)
    val dwdQzChapterList = DwdQzDao.getDwdQzChapterList(spark)
    val dwdQzChapter = DwdQzDao.getDwdQzChapter(spark)
    val dwdQzPointQuestion = DwdQzDao.getDwdQzPointQuestion(spark)


    dwdQzChapter.join(dwdQzChapterList, Seq("chapterlistid", "dn"), "inner")
      .join(dwdQzPoint, Seq("chapterid", "dn"), "inner")
      .join(dwdQzPointQuestion, Seq("pointid", "dn"), "inner")
//      .show(10)
      .select("chapterid", "chapterlistid", "chaptername", "sequence", "showstatus", "status", "chapter_creator", "chapter_createtime",
    "chapter_courseid", "chapternum", "chapterallnum", "outchapterid", "chapterlistname", "pointid", "questionid", "questype",
    "pointname", "pointyear", "chapter", "excisenum", "pointlistid", "pointdescribe", "pointlevel", "typelist",
    "point_score", "thought", "remid", "pointnamelist", "typelistids", "pointlist", "dt", "dn")
      .as[QzChapter].write.mode(SaveMode.Overwrite).insertInto("dws.dws_qz_chapter")
  }

  def importQzCourse(spark: SparkSession, dt: String) = {
    val dwdQzSiteCourse = DwdQzDao.getDwdQzSiteCourse(spark)
    val dwdQzCourse = DwdQzDao.getDwdQzCourse(spark)
  }
}
