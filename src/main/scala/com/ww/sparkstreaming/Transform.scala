package com.ww.sparkstreaming

import org.apache.spark._
import org.apache.spark.streaming._

/*
transform操作，应用在DStream上时，可以用于执行任意的RDD到RDD的转换操作。
通常用于实现DStream API中所没有提供的操作。。
例如：DStream API中，没有提供将一个DStream中的每个batch，与一个特定的RDD进行join的操作。
可以使用transform操作来实现该功能。
 */
object Transform {
  def main(args: Array[String]): Unit = {
    //sparkstreaming至少要开启2个线程，1个线程(receiver-task)接收数据，另外N个线程处理数据。
    val conf = new SparkConf().setMaster("local[2]").setAppName("Transform")

    //receiver-task接收流式数据后，每隔Block interval(默认200毫秒)生成1个block，存储到executor的内存中，
    // 为保证高可用，还会有副本。注：有M个block就有M个task的并行度。
    // StreamingContext每隔Batch inverval(入参Seconds秒)时间间隔，blocks组成一个RDD。
    val ssc = new StreamingContext(conf, Seconds(5))


    val lines = ssc.socketTextStream("127.0.0.1", 9999)

    val inputWordTuple = lines.flatMap { line => line.split(",") }
      .map { word => (word, 1) }

    //例如，想要流式计算不对这些字符做统计计数，类似业务上的黑名单过滤
    val filterCharactor = ssc.sparkContext.parallelize(List(".", "?", "!", "#", "@", "$", "%", "^", "&", "*")).map { ch => (ch, true) }

    //DStream API中，没有提供将一个DStream中的每个batch，与一个特定的RDD进行join的操作。
    //可以使用transform操作来实现该功能。
    val needWordDS = inputWordTuple.transform(inputRDD => {

      //输入数据(inputword,1) leftOuterJoin 特殊字符(filterCharactor,1)
      val leftJoinRDD = inputRDD.leftOuterJoin(filterCharactor)

      //leftJoinRDD.filter后得到要统计的word，格式是 String,(int,option[boolean])
      val needword = leftJoinRDD.filter(tuple => {
        if (tuple._2._2.isEmpty) {
          //不是filterCharactor，返回true，要统计
          true
        } else {
          //是filterCharactor，返回false，滤除不统计
          false
        }
      })
      //将格式转换为(word:String, count:int)的格式
      needword.map(tuple => (tuple._1, 1))
    })

    val wcDS = needWordDS.reduceByKey(_ + _)

    //打印RDD里面前十个元素
    wcDS.print()
    /*
    输入：
cd D:\software\netcat1.12\
nc -l -p 9999
hello,?,world,.,hello,#,*,world,$,hello,@,ww

    输出：
-------------------------------------------
Time: 1552640705000 ms
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
