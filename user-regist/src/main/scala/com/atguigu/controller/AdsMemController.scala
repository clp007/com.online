package com.atguigu.controller

import com.atguigu.service.AdsMemService
import com.atguigu.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AdsMemController {

    def main(args: Array[String]): Unit = {


        val sparkConf = new SparkConf().setAppName("ads_member_controller").setMaster("local[*]")
        val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
        val ssc = sparkSession.sparkContext
        ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
        ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
        HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
        AdsMemService.queryApi(sparkSession, "20190722")

        //AdsMemService.queryDetailSql(sparkSession, "20190722")


    }

}
