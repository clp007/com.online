package com.atguigu.consumer

import java.sql.{Connection, ResultSet}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.atguigu.util.{DataSourceUtil, QueryCallback, SqlProxy}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object RightRate {


    /*需求1：要求Spark Streaming 保证数据不丢失，每秒1000条处理速度，需要手动维护偏移量

            需求4：计算知识点掌握度 去重后的做题个数/当前知识点总题数（已知30题）*当前知识点的正确率*/

    private val groupid = "qz_test"


    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("RightRate").setMaster("local[*]")
                .set("spark.streaming.kafka.maxRatePerPartition", "100")
                .set("spark.streaming.backpressure.enabled", "true")
                .set("spark.streaming.stopGracefullyOnShutdown", "true")

        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        val sc: SparkContext = ssc.sparkContext
        sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
        sc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")

        val brokers = "hadoop101:9092,hadoop102:9092,hadoop103:9092"
        val kafka = Map(
            "bootstrap.servers" -> brokers, //服务器地址
            "key.deserializer" -> classOf[StringDeserializer], //kv的反序列化器
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> groupid, //消费组
            "auto.offset.reset" -> "earliest", //自动从哪里开始消费，最后,在没有其他记录offset的情况下使用
            //自动消费
            //"enable.auto.commit" -> (true: java.lang.Boolean)
            //提交，当Streaming收到消息，kafka就认为成功，
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        //手动维护偏移量
        //查询对象
        val proxy: SqlProxy = new SqlProxy
        //topic对象映射
        val topics = mutable.HashMap[TopicPartition, Long]()

        //拿到连接
        val client: Connection = DataSourceUtil.getConnection

        proxy.executeQuery(client, "select * from `offset_manager` where groupid=?"
            , Array(groupid), new QueryCallback {
                override def process(result: ResultSet): Unit = {
                    try {
                        while (result.next()) {
                            //编号从1开始,1groupID，2topic，3partition，4offset
                            val topic = result.getString(2)
                            val partition = result.getInt(3)
                            val tp: TopicPartition = new TopicPartition(topic, partition)
                            val offset = result.getLong(4)
                            topics.put(tp, offset)

                        }
                        //没有数据就关闭游标，类似stream
                        result.close()
                    } catch {
                        case e: Exception => e.printStackTrace()
                    } finally {
                        //关闭客户端
                        proxy.shutdown(client)
                    }

                }

            })

        //消费前先查询数据库
        val stream = if (topics.isEmpty) {
            KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent
                , ConsumerStrategies.Subscribe[String, String](Array("qz_log"), kafka))

        } else {

            KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent
                , ConsumerStrategies.Subscribe[String, String](Array("qz_log"), kafka, topics))
        }
        //需求2：同一个用户做在同一门课程同一知识点下做题需要去重，并且需要记录去重后的做题id与个数。
        //数据来了先去历史表中查看是否做过？没有存入历史数据库，元数据和现在的数据拼接
        //
        val ds: DStream[(String, String, String, String, String, String)] = stream.filter(record => {
            val s = record.value().split("\t")
            s.length == 6
        }).mapPartitions(part => {
            part.map(r => {
                val s = r.value().split("\t")
                val uid = s(0)
                val courseid = s(1)
                val pointid = s(2)
                val questid = s(3)
                val istrue = s(4)
                val createtime = s(5)
                (uid, courseid, pointid, questid, istrue, createtime)
            })
        })
        //多个分区同时查询数据库可能造成数据库的不安全
        ds.foreachRDD(rdd => {
            //Driver
            val grouprdd = rdd.groupBy(t => t._1 + "-" + t._2 + "-" + t._3)
            grouprdd.foreachPartition(part => {
                //executor
                val proxy = new SqlProxy
                val client = DataSourceUtil.getConnection

                try {
                    //更新历史数据
                    part.foreach {
                        case (key, iter) => {
                            updateSql(key, iter, proxy, client)
                        }
                    }
                } catch {
                    case e: Exception => e.printStackTrace()
                } finally {
                    proxy.shutdown(client)
                }
            })
        })

        //手动提交，原始流，不能转换
        stream.foreachRDD(rdd => {
            //在driver端提交偏移量
            val proxy = new SqlProxy
            val client = DataSourceUtil.getConnection
            try {
                //由topic，partition，offset共同组成的偏移量数组
                val offset: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                //提交kafka
                //rdd.asInstanceOf[CanCommitOffsets].commitAsync(offset)
                for (elem <- offset) {
                    proxy.executeUpdate(client,
                        """replace into offset_manager (groupid,topic
                          |,`partition`,untilOffset) values(?,?,?,?)""".stripMargin, Array(groupid,
                            elem.topic, elem.partition.toInt, elem.untilOffset))
                    //这里的partition是关键字，以后尽量都加上
                }

            } catch {
                case e: Exception => e.printStackTrace()
            } finally {
                proxy.shutdown(client)
            }
        })

        def updateSql(key: String, iter: Iterable[(String, String, String, String, String, String)],
                      proxy: SqlProxy, client: Connection) = {

            //查历史数据库
            val sql =
                """select questionids from `qz_point_history` where userid=?
                  |and courseid=? and pointid=? """.stripMargin
            val arr = key.split("-")
            val userid = arr(0).toInt
            val courseid = arr(1).toInt
            val pointid = arr(2).toInt
            val donequest = iter.toArray
            //做了的题目的数据
            val quest = donequest.map(t => t._4).distinct //去重后的quest

            val array = Array(userid, courseid, pointid) //?

            var histQuestid = Array[String]() //接受内部类对象
            proxy.executeQuery(client, sql, Array(userid, courseid, pointid), new QueryCallback {
                override def process(result: ResultSet): Unit = {

                    //遍历结果集
                    while (result.next()) {
                        histQuestid = result.getString(1).split(",")

                    }
                    result.close()
                }
            })
            //把旧的和新的合在一起并去重
            val newquest: Array[String] = quest.union(histQuestid).distinct
            //
            val qz_count: Int = newquest.length
            //做过的题数
            val result: String = newquest.mkString(",")
            //写入数据库的新内容
            var qz_sum: Int = donequest.length
            var qz_istrue: Int = donequest.filter(t => t._5 == "1").size
            //正确的个数
            //最开始的时间
            val createtime = donequest.map(t => t._6).min

            //jdk8格式化时间方法
            val updatetime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                    .format(LocalDateTime.now())

            val sql2 =
                """insert into  `qz_point_history`(userid,courseid,pointid,questionids,
                  |createtime,updatetime) values(?,?,?,?,?,?) on duplicate key update questionids=?
                  |,updatetime=?""".stripMargin
            proxy.executeUpdate(client, sql2, Array(userid, courseid, pointid, result, createtime,
                updatetime, result, updatetime))

            var qz_sumHis = 0
            var qz_istrueHis = 0

            val sql3 =
                """select qz_sum,qz_istrue from qz_point_detail where userid=? and
                  |courseid=? and pointid=?
                """.stripMargin
            proxy.executeQuery(client, sql3, Array(userid, courseid, pointid), new QueryCallback {
                override def process(result: ResultSet): Unit = {
                    while (result.next()) {
                        qz_sumHis = result.getInt(1)
                        qz_istrueHis = result.getInt(2)
                    }
                }
            })

            //需求3：计算知识点正确率 正确率计算公式：做题正确总个数/做题总数 保留两位小数
            //合起来
            qz_sum += qz_istrueHis
            qz_istrue += qz_istrueHis

            val correct_rate = qz_istrue.toDouble / qz_sum.toDouble
            //完成率，总共30个题，完成了
            val dones = qz_count / 30
            val mastery_rate = dones * correct_rate //掌握率
            val sql4 =
                """insert into qz_point_detail(userid,courseid,pointid,qz_sum,qz_count,
                  |qz_istrue,correct_rate,mastery_rate,createtime,updatetime) values(?,?,?,?,
                  |?,?,?,?,?,?) on duplicate key update qz_sum=?,qz_count=?,qz_istrue=?,
                  |correct_rate=?,mastery_rate=?,updatetime=?""".stripMargin
            proxy.executeUpdate(client, sql4, Array(userid, courseid, pointid, qz_sum, qz_count
                , qz_istrue, correct_rate, mastery_rate, createtime, updatetime, qz_sum, qz_count
                , qz_istrue, correct_rate, mastery_rate, updatetime))

        }


        ssc.start()
        ssc.awaitTermination()


    }
}
