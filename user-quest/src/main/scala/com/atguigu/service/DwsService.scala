package com.atguigu.service

import com.atguigu.dao.{DwdQzChapterDao, UserPaperDetailDao}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DwsService {

    /**
      * 维度退化
      * @param sparkSession
      * @param dt
      */

    def importChpater(sparkSession: SparkSession, dt: String) = {

        import sparkSession.implicits._

        val chapter: DataFrame = DwdQzChapterDao.getDwdQzChapter(sparkSession, dt)
        val point: DataFrame = DwdQzChapterDao.getDwdQzPoint(sparkSession, dt)
        val question: DataFrame = DwdQzChapterDao.getDwdQzPointQuestion(sparkSession, dt)

        val list: DataFrame = DwdQzChapterDao.getDwdQzChapterList(sparkSession, dt)

        val result: DataFrame = chapter.join(list, Seq("chapterlistid", "dn")).join(point, Seq("chapterid", "dn"))
                .join(question, Seq("pointid", "dn"))
        result.select("chapterid", "chapterlistid", "chaptername", "sequence", "showstatus", "showstatus",
            "chapter_creator", "chapter_createtime", "chapter_courseid", "chapternum", "chapterallnum", "outchapterid",
            "chapterlistname", "pointid", "questionid", "questype", "pointname", "pointyear", "chapter", "excisenum",
            "pointlistid", "pointdescribe", "pointlevel", "typelist", "point_score", "thought", "remid", "pointnamelist",
            "typelistids", "pointlist", "dt", "dn").coalesce(1)
                .write.mode(SaveMode.Append).insertInto("dws.dws_qz_chapter")



    }

    def importCourse(sparkSession: SparkSession,dt:String)={


    }

    //创建宽表
    def saveDwsUserPaperDetail(sparkSession: SparkSession, dt: String) = {
        val dwdQzMemberPaperQuestion = UserPaperDetailDao.getDwdQzMemberPaperQuestion(sparkSession, dt).drop("paperid")
                .withColumnRenamed("question_answer", "user_question_answer")
        val dwsQzChapter = UserPaperDetailDao.getDwsQzChapter(sparkSession, dt).drop("courseid")
        val dwsQzCourse = UserPaperDetailDao.getDwsQzCourse(sparkSession, dt).withColumnRenamed("sitecourse_creator", "course_creator")
                .withColumnRenamed("sitecourse_createtime", "course_createtime").drop("majorid")
                .drop("chapterlistid").drop("pointlistid")
        val dwsQzMajor = UserPaperDetailDao.getDwsQzMajor(sparkSession, dt)
        val dwsQzPaper = UserPaperDetailDao.getDwsQzPaper(sparkSession, dt).drop("courseid")
        val dwsQzQuestion = UserPaperDetailDao.getDwsQzQuestion(sparkSession, dt)
        dwdQzMemberPaperQuestion.join(dwsQzCourse, Seq("sitecourseid", "dn")).
                join(dwsQzChapter, Seq("chapterid", "dn")).join(dwsQzMajor, Seq("majorid", "dn"))
                .join(dwsQzPaper, Seq("paperviewid", "dn")).join(dwsQzQuestion, Seq("questionid", "dn"))
                .select("userid", "courseid", "questionid", "useranswer", "istrue", "lasttime", "opertype",
                    "paperid", "spendtime", "chapterid", "chaptername", "chapternum",
                    "chapterallnum", "outchapterid", "chapterlistname", "pointid", "questype", "pointyear", "chapter", "pointname"
                    , "excisenum", "pointdescribe", "pointlevel", "typelist", "point_score", "thought", "remid", "pointnamelist",
                    "typelistids", "pointlist", "sitecourseid", "siteid", "sitecoursename", "coursechapter", "course_sequence", "course_status"
                    , "course_creator", "course_createtime", "servertype", "helppaperstatus", "boardid", "showstatus", "majorid", "coursename",
                    "isadvc", "chapterlistid", "pointlistid", "courseeduid", "edusubjectid", "businessid", "majorname", "shortname",
                    "major_status", "major_sequence", "major_creator", "major_createtime", "businessname", "sitename",
                    "domain", "multicastserver", "templateserver", "multicastgateway", "multicastport", "paperviewid", "paperviewname", "paperparam",
                    "openstatus", "explainurl", "iscontest", "contesttime", "conteststarttime", "contestendtime", "contesttimelimit",
                    "dayiid", "paper_status", "paper_view_creator", "paper_view_createtime", "paperviewcatid", "modifystatus", "description", "paperuse",
                    "testreport", "centerid", "paper_sequence", "centername", "centeryear", "centertype", "provideuser", "centerviewtype",
                    "paper_stage", "papercatid", "paperyear", "suitnum", "papername", "totalscore", "question_parentid", "questypeid",
                    "quesviewtype", "question_content", "question_answer", "question_analysis", "question_limitminute", "score",
                    "splitscore", "lecture", "question_creator", "question_createtime", "question_modifystatus", "question_attanswer",
                    "question_questag", "question_vanalysisaddr", "question_difficulty", "quesskill", "vdeoaddr", "question_description",
                    "question_splitscoretype", "user_question_answer", "dt", "dn").coalesce(1)
                .write.mode(SaveMode.Append).insertInto("dws.dws_user_paper_detail")



}}
