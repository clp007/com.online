package com.atguigu.controller

import com.atguigu.service.DwdService
import com.atguigu.util.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DwdInsertController {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("DwdInsertController").setMaster("local[*]")

        val sparkSession: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

        val ssc: SparkContext = sparkSession.sparkContext

        ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
        ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")

        HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
        HiveUtil.openCompression(sparkSession)

        DwdService.Qz_Question(sparkSession,ssc)//插入做题日志数据
        DwdService.etlQzChapter(ssc, sparkSession)
        DwdService.etlQzChapterList(ssc, sparkSession)
        DwdService.etlQzPoint(ssc, sparkSession)
        DwdService.etlQzPointQuestion(ssc, sparkSession)
        DwdService.etlQzSiteCourse(ssc, sparkSession)
        DwdService.etlQzCourse(ssc, sparkSession)
        DwdService.etlQzCourseEdusubject(ssc, sparkSession)
        DwdService.etlQzWebsite(ssc, sparkSession)
        DwdService.etlQzMajor(ssc, sparkSession)
        DwdService.etlQzBusiness(ssc, sparkSession)
        DwdService.etlQzPaperView(ssc, sparkSession)
        DwdService.etlQzCenterPaper(ssc, sparkSession)
        DwdService.etlQzPaper(ssc, sparkSession)
        DwdService.etlQzCenter(ssc, sparkSession)
        DwdService.etlQzQuestionType(ssc, sparkSession)
        DwdService.etlQzMemberPaperQuestion(ssc, sparkSession)


    }

}
