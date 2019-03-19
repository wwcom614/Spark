package com.ww.sparkstreaming

import org.apache.spark._
import org.apache.spark.streaming._

//滑动窗口统计
// Seconds参数设置每N秒生成一个RDD,滑动窗口Window操作可以实现
// 每隔a*N秒 实时统计 前b*N秒 的统计计数情况
object ReduceByKeyAndWindow {
  def main(args: Array[String]): Unit = {
    //sparkstreaming至少要开启2个线程，1个线程(receiver-task)接收数据，另外N个线程处理数据。
    val conf = new SparkConf().setMaster("local[2]").setAppName("ReduceByKeyAndWindow")

    //receiver-task接收流式数据后，每隔Block interval(默认200毫秒)生成1个block，存储到executor的内存中，
    // 为保证高可用，还会有副本。注：有M个block就有M个task的并行度。
    // StreamingContext每隔Batch inverval(入参Seconds秒)时间间隔，blocks组成一个RDD。
    val ssc = new StreamingContext(conf, Seconds(5))


    val lines = ssc.socketTextStream("127.0.0.1", 9999)
    val wordcount = lines.flatMap { line => line.split(",") }
      .map { word => (word, 1) }
      //每隔10秒，统计前15秒数据
      .reduceByKeyAndWindow((v1:Int,v2:Int)=>{v1+v2},Seconds(15),Seconds(10))


    //打印RDD里面前十个元素
    wordcount.print()


    //启动应用
    ssc.start()
    //等待任务结束
    ssc.awaitTermination()
  }
}
