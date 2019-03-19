package com.ww.sparkstreaming

import org.apache.spark._
import org.apache.spark.streaming._

//按key累计计数MapWithState
//与updateStateByKey方法相比，使用mapWithState方法能够得到6倍的低延迟同时维护的key状态数量要多10倍。
//性能测试1：为同样数量（从0.25~1百万）的key保存其状态，然后以同样的速率（3万/s）对更新，mapWithState方法比updateStateByKey方法的处理时间快8倍
//性能测试2：更快的处理速度使得mapWithState 方法能够比updateStateByKey 方法管理多10倍的key（批处理间隔、集群大小、更新频率全部相同）
object MapWithState {
  def main(args: Array[String]): Unit = {
    //sparkstreaming至少要开启2个线程，1个线程(receiver-task)接收数据，另外N个线程处理数据。
    val conf = new SparkConf().setMaster("local[2]").setAppName("MapWithState")

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

    val wordTuple = lines.flatMap { line => line.split(",") }
      .map { word => (word, 1) }


    /**
      * mapWithState的入参：累计函数mappingFunc，函数有3个入参：
      * 入参1：word: String  代表的是key
      * 入参2：one: Option[Int] 代表的是value
      * 入参3：state: State[Int] 代表的是历史状态，也就是上次的累计结果
      */
    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      //本次统计结果 + 历史累计统计结果
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, sum)
      //累计结果更新到state中
      state.update(sum)
      //返回最终累计结果
      output
    }

    //MapWithState支持从某个初始RDD开始累计，而不是从0开始累计
    val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))

    //mapWithState的入参：累计函数mappingFunc
    val stateDstream = wordTuple.mapWithState(
      StateSpec.function(mappingFunc).initialState(initialRDD))

    //打印RDD里面前十个元素
    stateDstream.print()
    /*
    输入：
cd D:\software\netcat1.12\
nc -l -p 9999
hello,world,hello,world,hello,ww
hello,world,hello,world,hello,ww
    输出：
-------------------------------------------
Time: 1552633285000 ms
-------------------------------------------
(world,5)
(hello,7)
(ww,2)
     */

    //启动应用
    ssc.start()
    //等待任务结束
    ssc.awaitTermination()
  }
}
