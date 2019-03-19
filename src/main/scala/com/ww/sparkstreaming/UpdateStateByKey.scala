package com.ww.sparkstreaming

import org.apache.spark._
import org.apache.spark.streaming._

//按key累计计数UpdateStateByKey
object UpdateStateByKey {
  def main(args: Array[String]): Unit = {
    //sparkstreaming至少要开启2个线程，1个线程(receiver-task)接收数据，另外N个线程处理数据。
    val conf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateByKey")

    //receiver-task接收流式数据后，每隔Block interval(默认200毫秒)生成1个block，存储到executor的内存中，
    // 为保证高可用，还会有副本。注：有M个block就有M个task的并行度。
    // StreamingContext每隔Batch inverval(入参Seconds秒)时间间隔，blocks组成一个RDD。
    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.socketTextStream("127.0.0.1", 9999)

    //windows下运行使用
    System.setProperty("hadoop.home.dir", "D:/software/hadoop-3.0.0/bin/")

    //重要：使用updateStateByKey一定要设置一个checkpoint的目录。
    // 因为计算结果有中间状态，中间状态是长时间历史累计的，
    // 放在内存里不可靠，需要存储在磁盘上，避免task出问题时，前面的累计值可以从checkpoint获取到
    ssc.checkpoint("./sparkstreamingtmp/")

    val wordcount = lines.flatMap { line => line.split(",") }
      .map { word => (word, 1) }
      /*
      updateFunc: (Seq[Int], Option[S]) => Option[S]，是一个匿名函数。
      (Seq[Int], Option[S])是入参，Option[S]是返回值。
      (1)入参Seq[Int]：Seq代表的是一个集合，int代表的是(key,value)中的value的数据类型
      例如("ww",{1,1,1,1}),Seq[Int]是指的{1,1,1,1}
      (2)入参Option[S]：S代表的是累计值中间状态State的数据类型，S对于本wordcount代码来说，
      是int类型。中间状态存储的是单词累计出现的次数，如"ww"的累计状态 -> 4
      (3)返回值Option[S]，与累计的中间状态数据类型是一样的。
       */
      .updateStateByKey((values:Seq[Int],state:Option[Int]) => {
        val currentCount = values.sum  //获取本次单词出现的次数
        val count = state.getOrElse(0)  //获取上一次累计结果 也就是中间状态
        Some(currentCount + count)  //Some的意思是有可能有值，有可能没值None
      })


    //打印RDD里面前十个元素
    wordcount.print()
    /*
    输入：
cd D:\software\netcat1.12\
nc -l -p 9999
hello,world,hello,world,hello,ww
hello,world,hello,world,hello,ww
hello,world,hello,world,hello,ww
hello,world,hello,world,hello,ww
    输出：
-------------------------------------------
Time: 1552633285000 ms
-------------------------------------------
(hello,12)
(ww,4)
(world,8)
     */

    //启动应用
    ssc.start()
    //等待任务结束
    ssc.awaitTermination()
  }
}
