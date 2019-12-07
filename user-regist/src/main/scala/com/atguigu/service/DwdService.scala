package com.atguigu.service

import com.atguigu.bean.{MemberZipper, MemberZipperResult}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}


object DwdMem {


    def importMember(sparkSession: SparkSession, time: String) = {
        import sparkSession.implicits._ //隐式转换
        //查询全量数据 刷新到宽表
        sparkSession.sql("select uid,first(ad_id),first(fullname),first(iconurl),first(lastlogin)," +
                "first(mailaddr),first(memberlevel),first(password),sum(cast(paymoney as decimal(10,4))),first(phone),first(qq)," +
                "first(register),first(regupdatetime),first(unitname),first(userip),first(zipcode)," +
                "first(appkey),first(appregurl),first(bdp_uuid),first(reg_createtime)," +
                "first(isranreg),first(regsource),first(regsourcename),first(adname),first(siteid),first(sitename)," +
                "first(siteurl),first(site_delete),first(site_createtime),first(site_creator),first(vip_id),max(vip_level)," +
                "min(vip_start_time),max(vip_end_time),max(vip_last_modify_time),first(vip_max_free),first(vip_min_free),max(vip_next_level)," +
                "first(vip_operator),dt,dn from" +
                "(select a.uid,a.ad_id,a.fullname,a.iconurl,a.lastlogin,a.mailaddr,a.memberlevel," +
                "a.password,e.paymoney,a.phone,a.qq,a.register,a.regupdatetime,a.unitname,a.userip," +
                "a.zipcode,a.dt,b.appkey,b.appregurl,b.bdp_uuid,b.createtime as reg_createtime,b.isranreg,b.regsource," +
                "b.regsourcename,c.adname,d.siteid,d.sitename,d.siteurl,d.delete as site_delete,d.createtime as site_createtime," +
                "d.creator as site_creator,f.vip_id,f.vip_level,f.start_time as vip_start_time,f.end_time as vip_end_time," +
                "f.last_modify_time as vip_last_modify_time,f.max_free as vip_max_free,f.min_free as vip_min_free," +
                "f.next_level as vip_next_level,f.operator as vip_operator,a.dn " +
                s"from dwd.dwd_member a left join dwd.dwd_member_regtype b on a.uid=b.uid " +
                "and a.dn=b.dn left join dwd.dwd_base_ad c on a.ad_id=c.adid and a.dn=c.dn left join " +
                " dwd.dwd_base_website d on b.websiteid=d.siteid and b.dn=d.dn left join dwd.dwd_pcentermempaymoney e" +
                s" on a.uid=e.uid and a.dn=e.dn left join dwd.dwd_vip_level f on e.vip_id=f.vip_id and e.dn=f.dn where a.dt='${time}')r  " +
                "group by uid,dn,dt").coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_member")

        //每日新增表
        val day: Dataset[MemberZipper] = sparkSession.sql(
            s"""select s.uid,sum(cast(a.payment as decimal(10,4))) as paymoney,max(b.vip_level)
as vip_level, from_unixtime(unix_timestamp('$time','yyyyMMdd'),'yyyy-MM-dd')
as start_time, '9999-12-31' as end_time, first(a.dn) as dn
from dwd.dwd_pcentermempaymoney a join dwd.dwd_vip_level b on
a.vip_id=b.vip_id where a.dt='$time' group by uid
            """.stripMargin).as[MemberZipper]
        //历史表
        val his: Dataset[MemberZipper] = sparkSession.sql(""" select * from dws.dws_member_zipper""").as[MemberZipper]

        //以什么关键字？聚合才更好
        his.union(day).groupByKey(t => t.uid + "_" + t.start_time).mapGroups {
            case (key, iter) =>
                val keys = key.split("_")
                val id = keys(0)
                val start = keys(1)
                //此时排序，将最新的数据放到最后
                val list = iter.toList.sortBy(_.start_time)
                if (list.size > 1 && "9999-12-31" == list(list.size - 2).end_time) {
                    val old = list(list.size - 2)
                    //把最新的时间更新到拉链表
                    old.end_time = list.last.start_time
                    //钱进行累加,Translates the decimal String
                    list.last.paymoney = (BigDecimal.apply(old.paymoney) + BigDecimal.apply(list.last.paymoney)).toString()
                }

                //返回list结果集
                list
            //使用flatMap将数据扁平化，一条一条写
        }.flatMap(_.toList).coalesce(2).write.mode(SaveMode.Append).insertInto("dws.dws_member_zipper")

    }

    //调用API接口完成操作
    def import2Mem(sparkSession: SparkSession,dt:String)={

    }


}
