package com.atguigu.dao

import org.apache.spark.sql.SparkSession

object DwsMemberDao {


    //宽表中查询数据
    def queryIdlMemberData(sparkSession: SparkSession) = {
        sparkSession.sql("select uid,ad_id,memberlevel,register,appregurl,regsource,regsourcename,adname," +
                "siteid,sitename,vip_level,cast(paymoney as decimal(10,4)) as paymoney,dt,dn from dws.dws_member ")
    }


}
