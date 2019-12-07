package com.atguigu.consumer

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RegisterConsumer {


    def main(args: Array[String]): Unit = {


        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")

        val ssc = new StreamingContext(conf,Seconds(3))

        ssc.checkpoint("./ck1")

        val brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
        val topic = "register_topic"
        val group = "bigdata"
        val deserialization="org.apache.kafka.common.serialization.StringDeserializer"
        val kafkaParams =Map("zookeeper.connect"->"hadoop101:2181,hadoop102:2181,hadoop103:2181")
    }

}
