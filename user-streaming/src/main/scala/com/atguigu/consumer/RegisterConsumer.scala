package com.atguigu.consumer


import java.sql.{Connection, ResultSet}

import com.atguigu.util.{DataSourceUtil, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}



object RegisterConsumer {

    private val groupid = "register_group_test1"

    def main(args: Array[String]): Unit = {

        System.setProperty("HADOOP_USER_NAME", "atguigu")

        //设置消费速度和优雅的关闭
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
                .set("spark.streaming.kafka.maxRatePerPartition", "100")
                .set("spark.streaming.stopGracefullyOnShutdown", "true")


        val ssc = new StreamingContext(conf, Seconds(3))

        //获取上下文环境
        val context: SparkContext = ssc.sparkContext


        val brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092"

        val kafkaParams = Map(
            "bootstrap.servers" -> brokers, //服务器地址
            "key.deserializer" -> classOf[StringDeserializer], //kv的反序列化器
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> groupid, //消费组
            "auto.offset.reset" -> "earliest", //自动从哪里开始消费，最后,在没有其他记录offset的情况下使用
            //自动消费
            "enable.auto.commit" -> (true: java.lang.Boolean)
            //提交，当Streaming收到消息，kafka就认为成功，
            //"enable.auto.commit" -> (false: java.lang.Boolean)
        )
        context.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
        context.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
        //检查点
        //ssc.checkpoint("/user/atguigu/sparkstreaming/checkpoint")


      /*  val sqlProxy = new SqlProxy()
        val offsetMap = new mutable.HashMap[TopicPartition, Long]()
        val client: Connection = DataSourceUtil.getConnection
        try {
            sqlProxy.executeQuery(client, "select * from `offset_manager` where groupid=?"
                , Array(groupid), new QueryCallback {
                    override def process(result: ResultSet): Unit = {

                        while (result.next()) {
                            new TopicPartition(result.getString(2), result.getInt(3))
                        }
                    }
                })
        }*/


        //
        val record: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Array("register_topic"), kafkaParams))

        val stream: DStream[(String, Int)] = record.filter(item => item.value().split("\t").length == 3).mapPartitions(
            part => part.map(r => {
                val value = r.value().split("\t")
                val name = value(1)
                name match {
                    case "1" => "PC"
                    case "2" => "APP"
                    case _ => "Other"

                }
                (name, 1)
            })
        )
        stream.cache()


        val func=(
            (seq:Seq[Int],state:Option[Int])=>{
                val current= seq.sum //批次内求和
                val all= state.getOrElse(0) //历史
                Some(current+all)
            }
        )



        //stream.updateStateByKey(func).print
        //必须定义类型
        stream.reduceByKeyAndWindow((x:Int,y:Int)=>x+y,Seconds(60),Seconds(6)).print()



        ssc.start()
        ssc.awaitTermination()


    }
}
