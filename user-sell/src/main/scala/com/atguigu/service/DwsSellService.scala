package com.atguigu.service

import com.atguigu.bean.{DwdCourseShoppingCart, DwdSaleCourse}
import com.atguigu.dao.DwdSellDao
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object DwsSellService {


    def importSellCourseDetail(sparkSession: SparkSession, dt: String) = {
        val dwdSaleCourse = DwdSellDao.getDwdSaleCourse(sparkSession).where(s"dt='${dt}'")
        val dwdCourseShoppingCart = DwdSellDao.getDwdCourseShoppingCart(sparkSession).where(s"dt='${dt}'")
                .drop("coursename")
                .withColumnRenamed("discount", "cart_discount")
                .withColumnRenamed("createtime", "cart_createtime")
        val dwdCoursePay = DwdSellDao.getDwdCoursePay(sparkSession).where(s"dt='${dt}'")
                .withColumnRenamed("discount", "pay_discount")
                .withColumnRenamed("createtime", "pay_createtime")
        dwdSaleCourse.join(dwdCourseShoppingCart, Seq("courseid", "dt", "dn"), "right")
                .join(dwdCoursePay, Seq("orderid", "dt", "dn"), "left")
                .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
                    , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
                    "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn")
                .write.mode(SaveMode.Overwrite).insertInto("dws.dws_salecourse_detail")
    }

    def importSellCourseDetail2(sparkSession: SparkSession, dt: String) = {
        //解决数据倾斜 问题  将小表进行扩容 大表key加上随机散列值
        val dwdSaleCourse = DwdSellDao.getDwdSaleCourse(sparkSession).where(s"dt='${dt}'")
        val dwdCourseShoppingCart = DwdSellDao.getDwdCourseShoppingCart(sparkSession).where(s"dt='${dt}'")
                .withColumnRenamed("discount", "cart_discount")
                .withColumnRenamed("createtime", "cart_createtime")
        val dwdCoursePay = DwdSellDao.getDwdCoursePay(sparkSession).where(s"dt='${dt}'")
                .withColumnRenamed("discount", "pay_discount")
                .withColumnRenamed("createtime", "pay_createtime").coalesce(1)
        import sparkSession.implicits._
        //大表的key 拼随机前缀
        val newdwdCourseShoppingCart = dwdCourseShoppingCart.mapPartitions(partitions => {
            partitions.map(item => {
                val courseid = item.getAs[Int]("courseid")
                val randInt = Random.nextInt(100)
                DwdCourseShoppingCart(courseid, item.getAs[String]("orderid"),
                    item.getAs[String]("coursename"), item.getAs[java.math.BigDecimal]("cart_discount"),
                    item.getAs[java.math.BigDecimal]("sellmoney"), item.getAs[java.sql.Timestamp]("cart_createtime"),
                    item.getAs[String]("dt"), item.getAs[String]("dn"), courseid + "_" + randInt)
            })
        })
        //小表进行扩容
        val newdwdSaleCourse = dwdSaleCourse.flatMap(item => {
            val list = new ArrayBuffer[DwdSaleCourse]()
            val courseid = item.getAs[Int]("courseid")
            val coursename = item.getAs[String]("coursename")
            val status = item.getAs[String]("status")
            val pointlistid = item.getAs[Int]("pointlistid")
            val majorid = item.getAs[Int]("majorid")
            val chapterid = item.getAs[Int]("chapterid")
            val chaptername = item.getAs[String]("chaptername")
            val edusubjectid = item.getAs[Int]("edusubjectid")
            val edusubjectname = item.getAs[String]("edusubjectname")
            val teacherid = item.getAs[Int]("teacherid")
            val teachername = item.getAs[String]("teachername")
            val coursemanager = item.getAs[String]("coursemanager")
            val money = item.getAs[java.math.BigDecimal]("money")
            val dt = item.getAs[String]("dt")
            val dn = item.getAs[String]("dn")
            for (i <- 0 until 100) {
                list.append(DwdSellDao(courseid, coursename, status, pointlistid, majorid, chapterid, chaptername, edusubjectid,
                    edusubjectname, teacherid, teachername, coursemanager, money, dt, dn, courseid + "_" + i))
            }
            list.toIterator
        })
        newdwdSaleCourse.join(newdwdCourseShoppingCart.drop("courseid").drop("coursename"),
            Seq("rand_courseid", "dt", "dn"), "right")
                .join(dwdCoursePay, Seq("orderid", "dt", "dn"), "left")
                .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
                    , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
                    "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn")
                .write.mode(SaveMode.Overwrite).insertInto("dws.dws_salecourse_detail")
    }


    def importSellCourseDetail3(sparkSession: SparkSession, dt: String) = {
        //解决数据倾斜问题 采用广播小表
        val dwdSaleCourse = DwdSellDao.getDwdSaleCourse(sparkSession).where(s"dt='${dt}'")
        val dwdCourseShoppingCart = DwdSellDao.getDwdCourseShoppingCart(sparkSession).where(s"dt='${dt}'")
                .drop("coursename")
                .withColumnRenamed("discount", "cart_discount")
                .withColumnRenamed("createtime", "cart_createtime")
        val dwdCoursePay = DwdSellDao.getDwdCoursePay(sparkSession).where(s"dt='${dt}'")
                .withColumnRenamed("discount", "pay_discount")
                .withColumnRenamed("createtime", "pay_createtime")
        import org.apache.spark.sql.functions._
        broadcast(dwdSaleCourse).join(dwdCourseShoppingCart, Seq("courseid", "dt", "dn"), "right")
                .join(dwdCoursePay, Seq("orderid", "dt", "dn"), "left")
                .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
                    , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
                    "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn")
                .write.mode(SaveMode.Overwrite).insertInto("dws.dws_salecourse_detail")
    }

    def importSellCourseDetail4(sparkSession: SparkSession, dt: String) = {
        //解决数据倾斜问题 采用广播小表
        val dwdSaleCourse = DwdSellDao.getDwdSaleCourse(sparkSession).where(s"dt='${dt}'")
        val dwdCourseShoppingCart = DwdSellDao.getDwdCourseShoppingCart2(sparkSession).where(s"dt='${dt}'")
                .drop("coursename")
                .withColumnRenamed("discount", "cart_discount")
                .withColumnRenamed("createtime", "cart_createtime")
        val dwdCoursePay = DwdSellDao.getDwdCoursePay2(sparkSession).where(s"dt='${dt}'")
                .withColumnRenamed("discount", "pay_discount")
                .withColumnRenamed("createtime", "pay_createtime")
        import org.apache.spark.sql.functions._
        val tmpdata = dwdCourseShoppingCart.join(dwdCoursePay, Seq("orderid"), "left")
        val result = broadcast(dwdSaleCourse).join(tmpdata, Seq("courseid"), "right")
        result.select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
            , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
            "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dwd.dwd_sale_course.dt", "dwd.dwd_sale_course.dn")
                .write.mode(SaveMode.Overwrite).insertInto("dws.dws_salecourse_detail")
    }

}
