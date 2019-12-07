package com.atguigu.service

import com.atguigu.bean.QueryResult
import com.atguigu.dao.DwsMemberDao
import org.apache.spark.sql.{Dataset, SparkSession}

object AdsMemService {

    def queryApi(sparkSession: SparkSession,dt:String)={
        import sparkSession.implicits._

        val result: Dataset[QueryResult] = DwsMemberDao.queryIdlMemberData(sparkSession).as[QueryResult].where(s"dt='${dt}'")

        import org.apache.spark.sql.functions._
        result.groupBy("appregurl","dn","dt").agg(count("uid")).as("count(uid)")
                .select("appregurl","count(uid)","dn","dt").coalesce(1)
                .show(6)


    }





}
