package com.ww.sparkstreaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.KafkaUtils

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

//SparkStreaming2.4.0与Kafka2.1.1集成
object KafkaIntegration {
  def main(args: Array[String]): Unit = {

    val checkpointDir = "./sparkstreamingtmp/"

    //官方推荐的Driver高可用方法，生产环境都是这样用
    def functionToCreateContext(): StreamingContext = {
      //sparkstreaming至少要开启2个线程，1个线程(receiver-task)接收数据，另外N个线程处理数据。
      val conf = new SparkConf().setMaster("local[2]").setAppName("ReduceByKeyAndWindow")
        //序列化要使用Kryo，默认的Java序列化报错
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

      //receiver-task接收流式数据后，每隔Block interval(默认200毫秒)生成1个block，存储到executor的内存中，
      // 为保证高可用，还会有副本。注：有M个block就有M个task的并行度。
      // StreamingContext每隔Batch inverval(入参Seconds秒)时间间隔，blocks组成一个RDD。
      val streamingContext = new StreamingContext(conf, Seconds(5))


      //防止receiver异常等情况从Kafka获取数据丢失，拉取到的数据checkpoint持久化
      streamingContext.checkpoint(checkpointDir)

      streamingContext
    }

    //启动时，Driver检查checkpoint目录，如果没有数据则创建，如果有数据加载
    val ssc = StreamingContext.getOrCreate(checkpointDir, functionToCreateContext _)
    ssc.checkpoint(checkpointDir)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "stream_1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean) //生产中肯定要自己管理offset才能保证不丢消息
    )
    val topics = Array("test")

    //Spark2.2版本之后，只保留了pull方式拉取Kafka数据的方式
    val kafkaDS = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    ).cache() // KafkaUtils.createDirectStream返回值是Map，key是Kafka的offset，value是消费到的内容数据

    val wordcount = kafkaDS.map(record => record.value) //把Kafka消费数据的内容获取到
      .flatMap { line => line.split(",") }
      .map { word => (word, 1) }
      .reduceByKey(_ + _)

    //打印RDD里面前十个元素
    wordcount.print()

    /*
    输入：
    hello,world,hello,world,hello,ww

    输出：
-------------------------------------------
Time: 1552912015000 ms
-------------------------------------------
(ww,1)
(hello,3)
(world,2)


    */


    //启动应用
    streamingContext.start()
    //等待任务结束
    streamingContext.awaitTermination()
  }

}
