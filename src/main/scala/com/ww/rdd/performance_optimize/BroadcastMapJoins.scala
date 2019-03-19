package com.ww.rdd.performance_optimize

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
数据倾斜的解决方案 之 小表join大表转为小表broadcast+map大表实现。
适用场景：在对RDD使用join类操作，或者是在Spark SQL中使用join语句时，
并且join操作中的一个RDD或表的数据量比较小（比如几百M或者一两G）。
实现原理：普通的join是会走shuffle过程的，而一旦shuffle，
就相当于会将相同key的数据拉取到一个shuffle read task中再进行join，此时就是reduce join。
但是如果一个RDD比较小，则可以采用广播小RDD全量数据+map算子来实现与join同样的效果，也就是map join，
将较小RDD中的数据直接通过collect算子拉取到Driver端的内存中来，然后对其创建一个Broadcast变量；
接着对另外一个RDD执行map类算子，在算子函数内，从Broadcast变量中获取较小RDD的全量数据，
与当前RDD的每一条数据按照连接key进行比对，如果连接key相同的话，那么就将两个RDD的数据用需要的方式连接起来。
此时不会发生shuffle操作，也就不会发生数据倾斜。
 */
object BroadcastMapJoins {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("BroadcastMapJoins").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")

    val littleTableRDD: RDD[(Integer, String)] = sc.parallelize(
      List((1, "ww1"), (2, "ww2")))
    val bigTableRDD: RDD[(Integer, Double)] = sc.parallelize(
      List((1, 100.00), (2, 95.52), (1, 98.37), (1, 66.86), (2, 55.92), (1, 44.33),
        (2, 33.12), (1, 22.98), (1, 11.04), (2, 60.88)))

    //一个RDD或表的数据量比较小，比如几百MB或者1~2GB
    //首先，将要join的数据量比较小的littleTableRDD的数据，collect到Driver中。
    // 小表数据量要小的原因1：因为collect到Driver单机上，内存一定要够用
    val collectlittleTable = littleTableRDD.collect

    //然后使用Spark的广播功能，将该小数据量RDD的数据转换成广播变量，发送到每个Executor中存储。
    //小表数据量要小的原因2：广播后，该小表在每个Executor都要占用内存空间，且广播也有网络传输性能开销。
    val broadcastLittleTable = sc.broadcast(collectlittleTable)

    bigTableRDD.map(bigTableTuple => {
      var littleTableMap: Map[Integer, String] = Map()
      //从Executor获取被广播的小表数据，存入一个Map中，便于后面join操作
      for (record <- broadcastLittleTable.value) {
        littleTableMap += (record._1 -> record._2)
      }

      //返回值key
      val key: Integer = bigTableTuple._1
      //返回值join之后，上述key的小表对应value值
      val littleTableValue: String = littleTableMap.get(key).get
      //返回值join之后，上述key的大表对应value值
      val bigTableValue = bigTableTuple._2
      (key, Tuple2(littleTableValue, bigTableValue))
    }).foreach(result => print("[" + result._1 + ":(" + result._2._1 + "," + result._2._2 + ")];"))
    //[1:(ww1,100.0)];[1:(ww1,44.33)];[2:(ww2,95.52)];[2:(ww2,33.12)];[1:(ww1,22.98)];[1:(ww1,98.37)];[1:(ww1,11.04)];[1:(ww1,66.86)];[2:(ww2,60.88)];[2:(ww2,55.92)];

    sc.stop()
  }
}
