package com.atguigu.controller

import com.atguigu.bean.MemberZipper
import org.apache.spark.sql.{Dataset, SparkSession}

object DwsMemService {

    def importMember(sparkSession: SparkSession,tiem:String)={

        import sparkSession.implicits._
        val history: Dataset[MemberZipper] = sparkSession.sql(" select * from dws.dws_member_zipper").as[MemberZipper]



    }

}
