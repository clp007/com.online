package com.atguigu.service

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.bean.Module._
import com.atguigu.util.ParseJsonData
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}


//清洗导入
object DwdService {



    //1
    def Qz_Question(ssc: SparkSession, sc: SparkContext) = {
        import ssc.implicits._

        //json解析的时候还有两个分区字段
        sc.textFile("/user/atguigu/ods/QzQuestion.log").filter(s => {
            val obj: JSONObject = ParseJsonData.getJsonData(s)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(part => {
            part.map(str => {

                val ques: QzQuestion = JSON.parseObject(str, classOf[QzQuestion])
                import java.math._
                val score = ques.score.toString
                ques.score = new BigDecimal(score).setScale(1, BigDecimal.ROUND_HALF_UP) //向零四射五入
                ques.splitscore = new BigDecimal(score).setScale(1, BigDecimal.ROUND_HALF_UP)
                ques
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_question")


    }

    //解析章节数据 2
    def etlQzChapter(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._ //隐式转换
        ssc.textFile("/user/atguigu/ods/QzChapter.log").filter(item => {
            val obj = ParseJsonData.getJsonData(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = ParseJsonData.getJsonData(item)
                val chapterid = jsonObject.getIntValue("chapterid")
                val chapterlistid = jsonObject.getIntValue("chapterlistid")
                val chaptername = jsonObject.getString("chaptername")
                val sequence = jsonObject.getString("sequence")
                val showstatus = jsonObject.getString("showstatus")
                val status = jsonObject.getString("status")
                val creator = jsonObject.getString("creator")
                val createtime = jsonObject.getString("createtime")
                val courseid = jsonObject.getIntValue("courseid")
                val chapternum = jsonObject.getIntValue("chapternum")
                val outchapterid = jsonObject.getIntValue("outchapterid")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (chapterid, chapterlistid, chaptername, sequence, showstatus, creator, createtime,
                        courseid, chapternum, outchapterid, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_chapter")
    }

    //解析章节列表 3
    def etlQzChapterList(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("/user/atguigu/ods/QzChapterList.log").filter(item => {
            val obj = ParseJsonData.getJsonData(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = ParseJsonData.getJsonData(item)
                val chapterlistid = jsonObject.getIntValue("chapterlistid")
                val chapterlistname = jsonObject.getString("chapterlistname")
                val courseid = jsonObject.getIntValue("courseid")
                val chapterallnum = jsonObject.getIntValue("chapterallnum")
                val sequence = jsonObject.getString("sequence")
                val status = jsonObject.getString("status")
                val creator = jsonObject.getString("creator")
                val createtime = jsonObject.getString("createtime")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (chapterlistid, chapterlistname, courseid, chapterallnum, sequence, status, creator, createtime, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_chapter_list")
    }

    //解析做题数据 4
    def etlQzPoint(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("/user/atguigu/ods/QzPoint.log").filter(item => {
            val obj = ParseJsonData.getJsonData(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                import java.math.BigDecimal
                val qzpoint = JSON.parseObject(item, classOf[QzPoint])

                val score = qzpoint.score.toString()

                qzpoint.score = new BigDecimal(score).setScale(1, BigDecimal.ROUND_HALF_UP) //保留1位小数 并四舍五入

                qzpoint
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_point")
    }


    //知识点下的题数据
    def etlQzPointQuestion(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("/user/atguigu/ods/QzPointQuestion.log").filter(item => {
            val obj = ParseJsonData.getJsonData(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = ParseJsonData.getJsonData(item)
                val pointid = jsonObject.getIntValue("pointid")
                val questionid = jsonObject.getIntValue("questionid")
                val questype = jsonObject.getIntValue("questype")
                val creator = jsonObject.getString("creator")
                val createtime = jsonObject.getString("createtime")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (pointid, questionid, questype, creator, createtime, dt, dn)
            })
        }).toDF().write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_point_question")
    }

    //网站课程
    def etlQzSiteCourse(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("/user/atguigu/ods/QzSiteCourse.log").filter(item => {
            val obj = ParseJsonData.getJsonData(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = ParseJsonData.getJsonData(item)
                val sitecourseid = jsonObject.getIntValue("sitecourseid")
                val siteid = jsonObject.getIntValue("siteid")
                val courseid = jsonObject.getIntValue("courseid")
                val sitecoursename = jsonObject.getString("sitecoursename")
                val coursechapter = jsonObject.getString("coursechapter")
                val sequence = jsonObject.getString("sequence")
                val status = jsonObject.getString("status")
                val creator = jsonObject.getString("creator")
                val createtime = jsonObject.getString("createtime")
                val helppaperstatus = jsonObject.getString("helppaperstatus")
                val servertype = jsonObject.getString("servertype")
                val boardid = jsonObject.getIntValue("boardid")
                val showstatus = jsonObject.getString("showstatus")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (sitecourseid, siteid, courseid, sitecoursename, coursechapter, sequence, status, creator
                        , createtime, helppaperstatus, servertype, boardid, showstatus, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_site_course")
    }

    //课程数据
    def etlQzCourse(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("/user/atguigu/ods/QzCourse.log").filter(item => {
            val obj = ParseJsonData.getJsonData(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = ParseJsonData.getJsonData(item)
                val courseid = jsonObject.getIntValue("courseid")
                val majorid = jsonObject.getIntValue("majorid")
                val coursename = jsonObject.getString("coursename")
                val coursechapter = jsonObject.getString("coursechapter")
                val sequence = jsonObject.getString("sequnece")
                val isadvc = jsonObject.getString("isadvc")
                val creator = jsonObject.getString("creator")
                val createtime = jsonObject.getString("createtime")
                val status = jsonObject.getString("status")
                val chapterlistid = jsonObject.getIntValue("chapterlistid")
                val pointlistid = jsonObject.getIntValue("pointlistid")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (courseid, majorid, coursename, coursechapter, sequence, isadvc, creator, createtime, status
                        , chapterlistid, pointlistid, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_course")
    }

    //课程辅导数据
    def etlQzCourseEdusubject(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("/user/atguigu/ods/QzCourseEduSubject.log").filter(item => {
            val obj = ParseJsonData.getJsonData(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = ParseJsonData.getJsonData(item)
                val courseeduid = jsonObject.getIntValue("courseeduid")
                val edusubjectid = jsonObject.getIntValue("edusubjectid")
                val courseid = jsonObject.getIntValue("courseid")
                val creator = jsonObject.getString("creator")
                val createtime = jsonObject.getString("createtime")
                val majorid = jsonObject.getIntValue("majorid")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (courseeduid, edusubjectid, courseid, creator, createtime, majorid, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_course_edusubject")
    }

    //解析网站
    def etlQzWebsite(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("/user/atguigu/ods/QzWebsite.log").filter(item => {
            val obj = ParseJsonData.getJsonData(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = ParseJsonData.getJsonData(item)
                val siteid = jsonObject.getIntValue("siteid")
                val sitename = jsonObject.getString("sitename")
                val domain = jsonObject.getString("domain")
                val sequence = jsonObject.getString("sequence")
                val multicastserver = jsonObject.getString("multicastserver")
                val templateserver = jsonObject.getString("templateserver")
                val status = jsonObject.getString("status")
                val creator = jsonObject.getString("creator")
                val createtime = jsonObject.getString("createtime")
                val multicastgateway = jsonObject.getString("multicastgateway")
                val multicastport = jsonObject.getString("multicastport")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (siteid, sitename, domain, sequence, multicastserver, templateserver, status, creator, createtime,
                        multicastgateway, multicastport, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_website")
    }

    //解析主修数据
    def etlQzMajor(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("/user/atguigu/ods/QzMajor.log").filter(item => {
            val obj = ParseJsonData.getJsonData(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = ParseJsonData.getJsonData(item)
                val majorid = jsonObject.getIntValue("majorid")
                val businessid = jsonObject.getIntValue("businessid")
                val siteid = jsonObject.getIntValue("siteid")
                val majorname = jsonObject.getString("majorname")
                val shortname = jsonObject.getString("shortname")
                val status = jsonObject.getString("status")
                val sequence = jsonObject.getString("sequence")
                val creator = jsonObject.getString("creator")
                val createtime = jsonObject.getString("createtime")
                val columm_sitetype = jsonObject.getString("columm_sitetype")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (majorid, businessid, siteid, majorname, shortname, status, sequence, creator, createtime, columm_sitetype, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_major")
    }

    /**
      * 解析做题业务
      *
      * @param ssc
      * @param sparkSession
      */
    def etlQzBusiness(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("/user/atguigu/ods/QzBusiness.log").filter(item => {
            val obj = ParseJsonData.getJsonData(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = ParseJsonData.getJsonData(item)
                val businessid = jsonObject.getIntValue("businessid")
                val businessname = jsonObject.getString("businessname")
                val sequence = jsonObject.getString("sequence")
                val status = jsonObject.getString("status")
                val creator = jsonObject.getString("creator")
                val createtime = jsonObject.getString("createtime")
                val siteid = jsonObject.getIntValue("siteid")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (businessid, businessname, sequence, status, creator, createtime, siteid, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_business")
    }


    def etlQzPaperView(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("/user/atguigu/ods/QzPaperView.log").filter(item => {
            val obj = ParseJsonData.getJsonData(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val qzPaperView = JSON.parseObject(item, classOf[QzPaperView])
                qzPaperView
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_paper_view")
    }


    def etlQzCenterPaper(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("/user/atguigu/ods/QzCenterPaper.log").filter(item => {
            val obj = ParseJsonData.getJsonData(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = ParseJsonData.getJsonData(item)
                val paperviewid = jsonObject.getIntValue("paperviewid")
                val centerid = jsonObject.getIntValue("centerid")
                val openstatus = jsonObject.getString("openstatus")
                val sequence = jsonObject.getString("sequence")
                val creator = jsonObject.getString("creator")
                val createtime = jsonObject.getString("createtime")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (paperviewid, centerid, openstatus, sequence, creator, createtime, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_center_paper")
    }


    def etlQzPaper(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("/user/atguigu/ods/QzPaper.log").filter(item => {
            val obj = ParseJsonData.getJsonData(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = ParseJsonData.getJsonData(item)
                val paperid = jsonObject.getIntValue("paperid")
                val papercatid = jsonObject.getIntValue("papercatid")
                val courseid = jsonObject.getIntValue("courseid")
                val paperyear = jsonObject.getString("paperyear")
                val chapter = jsonObject.getString("chapter")
                val suitnum = jsonObject.getString("suitnum")
                val papername = jsonObject.getString("papername")
                val status = jsonObject.getString("status")
                val creator = jsonObject.getString("creator")
                val craetetime = jsonObject.getString("createtime")
                val totalscore = BigDecimal.apply(jsonObject.getString("totalscore")).setScale(1, BigDecimal.RoundingMode.HALF_UP)
                val chapterid = jsonObject.getIntValue("chapterid")
                val chapterlistid = jsonObject.getIntValue("chapterlistid")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (paperid, papercatid, courseid, paperyear, chapter, suitnum, papername, status, creator, craetetime, totalscore, chapterid,
                        chapterlistid, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_paper")
    }

    def etlQzCenter(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("/user/atguigu/ods/QzCenter.log").filter(item => {
            val obj = ParseJsonData.getJsonData(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(parititons => {
            parititons.map(item => {
                val jsonObject = ParseJsonData.getJsonData(item)
                val centerid = jsonObject.getIntValue("centerid")
                val centername = jsonObject.getString("centername")
                val centeryear = jsonObject.getString("centeryear")
                val centertype = jsonObject.getString("centertype")
                val openstatus = jsonObject.getString("openstatus")
                val centerparam = jsonObject.getString("centerparam")
                val description = jsonObject.getString("description")
                val creator = jsonObject.getString("creator")
                val createtime = jsonObject.getString("createtime")
                val sequence = jsonObject.getString("sequence")
                val provideuser = jsonObject.getString("provideuser")
                val centerviewtype = jsonObject.getString("centerviewtype")
                val stage = jsonObject.getString("stage")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (centerid, centername, centeryear, centertype, openstatus, centerparam, description, creator, createtime,
                        sequence, provideuser, centerviewtype, stage, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_center")
    }


    def etlQzQuestionType(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("/user/atguigu/ods/QzQuestionType.log").filter(item => {
            val obj = ParseJsonData.getJsonData(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = ParseJsonData.getJsonData(item)
                val quesviewtype = jsonObject.getIntValue("quesviewtype")
                val viewtypename = jsonObject.getString("viewtypename")
                val questiontypeid = jsonObject.getIntValue("questypeid")
                val description = jsonObject.getString("description")
                val status = jsonObject.getString("status")
                val creator = jsonObject.getString("creator")
                val createtime = jsonObject.getString("createtime")
                val papertypename = jsonObject.getString("papertypename")
                val sequence = jsonObject.getString("sequence")
                val remark = jsonObject.getString("remark")
                val splitscoretype = jsonObject.getString("splitscoretype")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (quesviewtype, viewtypename, questiontypeid, description, status, creator, createtime, papertypename, sequence,
                        remark, splitscoretype, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_question_type")
    }


    /**
      * 解析用户做题情况数据
      *
      * @param ssc
      * @param sparkSession
      */
    def etlQzMemberPaperQuestion(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("/user/atguigu/ods/QzMemberPaperQuestion.log").filter(item => {
            val obj = ParseJsonData.getJsonData(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            import java.math.BigDecimal
            partitions.map(item => {
                val memberQues = JSON.parseObject(item, classOf[QzMemberPaperQuestion])

                val score = memberQues.score.toString
                memberQues.score = new BigDecimal(score).setScale(1, BigDecimal.ROUND_HALF_UP)

                memberQues

            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_member_paper_question")
    }

}
