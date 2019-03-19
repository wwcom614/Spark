package com.ww.rdd.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD {

  /**
    * 尝试Pair RDD的transforms操作和actions操作
    */
  def pairMapRDD(sc: SparkContext): Unit = {
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("k01", 3), ("k02", 6), ("k03", 2), ("k01", 26)))
    val other: RDD[(String, Int)] = sc.parallelize(List(("k01", 29)), 1)

    // transforms操作
    val rddReduce1: RDD[(String, Int)] = rdd.reduceByKey((x, y) => x + y)
    println("【reduceByKey1】：" + rddReduce1.collect().mkString(",")) // 【reduceByKey1】：(k01,29),(k03,2),(k02,6)

    val rddReduce2: RDD[(String, Int)] = rdd.reduceByKey(_ + _)
    println("【reduceByKey2】：" + rddReduce1.collect().mkString(",")) // 【reduceByKey2】：(k01,29),(k03,2),(k02,6)

    val rddGroup: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    println("【groupByKey】：" + rddGroup.collect().mkString(",")) // 【groupByKey】：(k01,CompactBuffer(3, 26)),(k03,CompactBuffer(2)),(k02,CompactBuffer(6))

    val rddKeys: RDD[String] = rdd.keys
    println("【keys】：" + rddKeys.collect().mkString(",")) // 【keys】：k01,k02,k03,k01，包括重复的key

    val rddVals: RDD[Int] = rdd.values
    println("【values】：" + rddVals.collect().mkString(",")) // 【values】：3,6,2,26

    val rddSortAscend: RDD[(String, Int)] = rdd.sortByKey(true, 1)
    println("【rddSortAscend】：" + rddSortAscend.collect().mkString(",")) // 【rddSortAscend】：(k01,3),(k01,26),(k02,6),(k03,2)

    val rddSortDescend: RDD[(String, Int)] = rdd.sortByKey(false, 1)
    println("【rddSortDescend】：" + rddSortDescend.collect().mkString(",")) // 【rddSortDescend】：(k03,2),(k02,6),(k01,3),(k01,26)

    val rddFlatMapValues: RDD[(String, Int)] = rdd.flatMapValues { x => List(x + 10) }
    println("【flatMapValues】：" + rddFlatMapValues.collect().mkString(",")) //【flatMapValues】：(k01,13),(k02,16),(k03,12),(k01,36)

    val rddMapValues: RDD[(String, Int)] = rdd.mapValues { x => x + 10 }
    println("【mapValues】：" + rddMapValues.collect().mkString(",")) // 【mapValues】：(k01,13),(k02,16),(k03,12),(k01,36)

    //求每个学生的平均分
    val scoreList = Array(("ww1", 88), ("ww1", 95), ("ww2", 91), ("ww2", 93), ("ww2", 95), ("ww2", 98))
    val scoreRDD: RDD[(String, Int)] = sc.parallelize(scoreList, 2)
    println("【scoreRDD.partitions.size】：" + scoreRDD.partitions.size)
    //分区数，【scoreRDD.partitions.size】：2
    println("【scoreRDD.glom.collect】：" + scoreRDD.glom().collect().mkString(",")) //每个分区的内容

    val rddCombineByKey: RDD[(String, (Int, Int))] = scoreRDD.combineByKey(v => (v, 1),
      (param1: (Int, Int), v) => (param1._1 + v, param1._2 + 1),
      (p1: (Int, Int), p2: (Int, Int)) => (p1._1 + p2._1, p1._2 + p2._2))
    println("【combineByKey】：" + rddCombineByKey.collect().mkString(","))
    //【combineByKey】：(ww2,(377,4)),(ww1,(183,2))

    //在map中使用case是scala的用法
    val avgScore = rddCombineByKey.map { case (key, value) => (key, value._1 / value._2.toDouble) }
    println("【avgScore】：" + avgScore.collect().mkString(","))
    //【avgScore】：(ww2,94.25),(ww1,91.5)


    val rddJoin: RDD[(String, (Int, Int))] = rdd.join(other)
    println("【join】：" + rddJoin.collect().mkString(",")) // (k01,(3,29)),(k01,(26,29))

    val rddLeftOuterJoin: RDD[(String, (Int, Option[Int]))] = rdd.leftOuterJoin(other)
    println("【leftOuterJoin】：" + rddLeftOuterJoin.collect().mkString(",")) // (k01,(3,Some(29))),(k01,(26,Some(29))),(k03,(2,None)),(k02,(6,None))

    val rddRightOuterJoin: RDD[(String, (Option[Int], Int))] = rdd.rightOuterJoin(other)
    println("【rightOuterJoin】：" + rddRightOuterJoin.collect().mkString(",")) // (k01,(Some(3),29)),(k01,(Some(26),29))

    val rddSubtract: RDD[(String, Int)] = rdd.subtractByKey(other)
    println("【subtractByKey】：" + rddSubtract.collect().mkString(",")) // (k03,2),(k02,6)

    val rddCogroup: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd.cogroup(other)
    println("【cogroup】：" + rddCogroup.collect().mkString(",")) // (k01,(CompactBuffer(3, 26),CompactBuffer(29))),(k03,(CompactBuffer(2),CompactBuffer())),(k02,(CompactBuffer(6),CompactBuffer()))

    // Actions操作
    val resCountByKey = rdd.countByKey()
    println("【countByKey】：" + resCountByKey)
    // Map(k01 -> 2, k03 -> 1, k02 -> 1)

    val resColMap = rdd.collectAsMap()
    println("【collectAsMap】：" + resColMap)
    //Map(k02 -> 6, k01 -> 26, k03 -> 2)
    val resLookup = rdd.lookup("k01")
    println("【lookup】：" + resLookup) // WrappedArray(3, 26)
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark pair map").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    pairMapRDD(sc)
    sc.stop()
  }
}
