package com.skillip.onlineedu.qz.bean

case class QzPoint(poIntid: Int,
                   courseid: Int,
                   poIntname: String,
                   poIntyear: String,
                   chapter: String,
                   creator: String,
                   createtme: String,
                   status: String,
                   modifystatus: String,
                   excisenum: Int,
                   poIntlistid: Int,
                   chapterid: Int,
                   sequence: String,
                   poIntdescribe: String,
                   poIntlevel: String,
                   typelist: String,
                   score: Double,
                   thought: String,
                   remid: String,
                   poIntnamelist: String,
                   typelistids: String,
                   poIntlist: String,
                   dt: String,
                   dn: String)

case class QzPaperView(paperviewid: Int,
                       paperid: Int,
                       paperviewname: String,
                       paperparam: String,
                       openstatus: String,
                       explainurl: String,
                       iscontest: String,
                       contesttime: String,
                       conteststarttime: String,
                       contestendtime: String,
                       contesttimelimit: String,
                       dayiid: Int,
                       status: String,
                       creator: String,
                       createtime: String,
                       paperviewcatid: Int,
                       modifystatus: String,
                       description: String,
                       papertype: String,
                       downurl: String,
                       paperuse: String,
                       paperdifficult: String,
                       testreport: String,
                       paperuseshow: String,
                       dt: String,
                       dn: String)


case class QzQuestion(
                     questionid: Int,
                     parentid: Int,
                     questypeid: Int,
                     quesviewtype: Int,
                     content: String,
                     answer: String,
                     analysis: String,
                     limitminute: String,
                     score: Double,
                     splitscore: Double,
                     status: String,
                     optnum: Int,
                     lecture: String,
                     creator: String,
                     createtime: String,
                     modifystatus: String,
                     attanswer: String,
                     questag: String,
                     vanalysisaddr: String,
                     difficulty: String,
                     quesskill: String,
                     vdeoaddr: String,
                     dt: String,
                     dn: String
                   )