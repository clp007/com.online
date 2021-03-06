package producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

object Producer {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("registerProducer").setMaster("local[*]")
        val ssc = new SparkContext(sparkConf)
        System.setProperty("hadoop.home.dir","D:\\soft\\hadoop-common-2.2.0-bin-master")
        ssc.textFile(this.getClass.getResource("/qz.log").getPath, 10)
                .foreachPartition(partition => {
                    val props = new Properties()
                    props.put("bootstrap.servers", "192.168.1.102:9092,192.168.1.103:9092,hadoop104:9092")
                    props.put("acks", "1")
                    props.put("batch.size", "16384")
                    props.put("linger.ms", "10")
                    props.put("buffer.memory", "33554432")
                    props.put("key.serializer",
                        "org.apache.kafka.common.serialization.StringSerializer")
                    props.put("value.serializer",
                        "org.apache.kafka.common.serialization.StringSerializer")
                    val producer = new KafkaProducer[String, String](props)
                    partition.foreach(item => {
                        val msg = new ProducerRecord[String, String]("qz_log",item)
                        producer.send(msg)
                    })
                    producer.flush()
                    producer.close()
                })
    }

}
