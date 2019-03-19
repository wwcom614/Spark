package com.ww.rdd.performance_optimize

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
数据倾斜的解决方案 之 两阶段聚合（局部聚合+全局聚合）
适用场景：对RDD执行reduceByKey等聚合类shuffle算子或者在Spark SQL中使用group by语句进行分组聚合时。
实现原理：将原本相同的key通过附加随机前缀的方式，变成多个不同的key，
就可以让原本被一个task处理的数据分散到多个task上去做局部聚合，进而解决单个task处理数据量过多的问题。
接着去除掉随机前缀，再次进行全局聚合，就可以得到最终的结果。
*/
object Agg2PCs {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Agg2PCs").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")

    //数据key倾斜的RDD
    val rdd: RDD[(String, Long)] = sc.parallelize(
      List(("ww1", 1L), ("ww1", 2L), ("ww1", 3L), ("ww1", 4L), ("ww1", 5L),
        ("ww1", 6L), ("ww1", 7L), ("ww1", 8L), ("ww1", 9L), ("ww2", 10L)))

    val localAggRdd: RDD[(String, Long)] = rdd.map(e => {
      val prefix = (new util.Random).nextInt(10)
      (prefix + "_" + e._1, e._2)//第一步：给key倾斜的dataSkewRDD中每个key都打上一个随机前缀
    }).reduceByKey(_ + _).cache() //第二步：对打上随机前缀的key不再倾斜的randomPrefixRdd进行局部聚合
    print("【localAggRdd】：" )
    localAggRdd.foreach(e => print("["+e._1+":"+e._2+"];"))
    //【localAggRdd】：[2_ww2:10];[7_ww1:5];[0_ww1:5];[9_ww1:8];[2_ww1:7];[5_ww1:8];[6_ww1:12];

    val globalAggRdd: RDD[(String, Long)] = localAggRdd.map(e => {
      (e._1.split("_")(1), e._2) //第三步：局部聚合后，去除localAggRdd中每个key的随机前缀
    }).reduceByKey(_ + _) //第四步：对去除了随机前缀的removeRandomPrefixRdd进行全局聚合

    println("")
    print("【globalAggRdd】：" )
    globalAggRdd.foreach(e => print("["+e._1+":"+e._2+"];"))
    //【globalAggRdd】：[ww1:45];[ww2:10];

    sc.stop()
  }
}
