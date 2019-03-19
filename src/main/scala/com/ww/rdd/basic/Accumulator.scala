package com.ww.rdd.basic

import org.apache.spark.{SparkConf, SparkContext}

object Accumulator {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("accumulator").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")

    //Spark2.0开始：
    //1.计数器可以展现在Spark的WEB UI上，所以起个名字界面区分不同计数器
    //2.计数器可以指定数据类型
    val accum = sc.longAccumulator("My Accumulator")
    sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).foreach(x => accum.add(x))
    println(accum.value)
    //55
    sc.stop()
  }
}
