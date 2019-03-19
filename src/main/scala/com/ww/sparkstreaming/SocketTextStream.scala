package com.ww.sparkstreaming

import org.apache.spark._
import org.apache.spark.streaming._

object SocketTextStream {
  def main(args: Array[String]): Unit = {
    //sparkstreaming至少要开启2个线程，1个线程(receiver-task)接收数据，另外N个线程处理数据。
    val conf = new SparkConf().setMaster("local[2]").setAppName("SocketTextStream")

    //receiver-task接收流式数据后，每隔Block interval(默认200毫秒)生成1个block，存储到executor的内存中，
    // 为保证高可用，还会有副本。注：有M个block就有M个task的并行度。
    // StreamingContext每隔Batch inverval(入参Seconds秒)时间间隔，blocks组成一个RDD。
    val ssc = new StreamingContext(conf, Seconds(5))


    val lines = ssc.socketTextStream("127.0.0.1", 9999)
    val wordcount = lines.flatMap { line => line.split(",") }
      .map { word => (word, 1) }
      .reduceByKey(_ + _)


    //打印RDD里面前十个元素
    wordcount.print()
    /*
    输入：
cd D:\software\netcat1.12\
nc -l -p 9999
hello,world,hello,world,hello,ww

    输出：
-------------------------------------------
Time: 1552624970000 ms
-------------------------------------------
(ww,1)
(hello,3)
(world,2)
     */

    //启动应用
    ssc.start()
    //等待任务结束
    ssc.awaitTermination()
  }
}
