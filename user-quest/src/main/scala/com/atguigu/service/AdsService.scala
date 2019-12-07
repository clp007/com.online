package com.atguigu.service

import org.apache.spark.sql._

object AdsService {


    def getTarget(sparkSession: SparkSession, dt: String) = {

        import org.apache.spark.sql.functions._


        //需求8：统计各试卷各分段的用户id，分段有0-20,20-40,40-60，60-80,80-100
        sparkSession.sql("select *from dws.dws_user_paper_detail")
                .where(s"dt=$dt").select("paperviewid", "paperviewname", "userid", "score", "dt", "dn")
                .withColumn("score_segment",
                    when(col("score").between(0, 20), "0-10")
                            .when(col("score") > 20 && col("score") <= 40, "20-40")
                            .when(col("score") > 40 && col("score") <= 60, "40-60")
                            .when(col("score") > 60 && col("score") <= 80, "60-80")
                            .when(col("score") > 80 && col("score") <= 100, "80-100"))
                .drop("score").groupBy("paperviewid", "paperviewname", "score_segment", "dt", "dn")
                //col() 对象可以调用很多方法 as起别名
                .agg(concat_ws(",",collect_set(col("userid").cast("string")))).as("userids")
                .select("paperviewid", "paperviewname", "score_segment", "userids","dt", "dn")
                .orderBy("papervieid", "score_segment")
                .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_paper_scoresegment_user")


        //需求9：统计试卷未及格的人数，及格的人数，试卷的及格率 及格分数60

        val passDetail: DataFrame = sparkSession.sql("select * from dws.dws_user_paper_detail").cache()

        val unpass: Dataset[Row] = passDetail.select("paperviewid", "paperviewname", "dn", "dt")
                .where("score between 0 and 60")
                .where(s"dt=$dt").groupBy("paperviewid", "paperviewname", "dn", "dt")
                .agg(count("paperviewid")).as("unpass")

        val pass: Dataset[Row] = passDetail.select("paperviewid", "dn")
                .where(s"st=$dt").where("score >60")
                .groupBy("paperviewid", "dn")
                .agg(count("paperviewid")).as("pass")

        //join之后
        unpass.join(pass).withColumn("rate", (col("pass") /(col("pass") + col("unpass")))
                .cast("decimal(4,1)")).select("paperviewid", "paperviewname", "rate", "dn", "dt")
                .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_user_paper_detail")


        //需求10：统计各题的错误数，正确数，错题率



    }


}
