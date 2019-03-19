package com.ww.rdd.basic

import org.apache.spark.{SparkConf, SparkContext}

object ErrorTopN {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("error topN").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")

    val path = "D:/ideaworkspace/spark/sparksql/data/OriginalData.txt"
    val textFile = sc.textFile(path)
    val errors = textFile.map(line => (line.split("\t")(7), 1))

    errors.reduceByKey(_+_).map(x=>(x._2, x._1)).sortByKey(false).map(x=>(x._2, x._1)).take(3).foreach(println)
    /*
      (404,340)
      (500,335)
      (200,325)
    **/
    sc.stop()
  }
}
