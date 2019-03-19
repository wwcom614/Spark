package com.ww.rdd.basic

import org.apache.spark.{SparkConf, SparkContext}

object FluxAvg {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("error topN").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")

    val path = "D:/ideaworkspace/spark/sparksql/data/OriginalData.txt"
    val textFile = sc.textFile(path)
    val flux = textFile.map(line => line.split("\t")(8))

    val totalFlux = flux.map(flux=>flux.toDouble).reduce(_+_)
    println("【totalFlux】："+ totalFlux)
    //【totalFlux】：5043746.0
    val count = flux.count()
    println("【count】："+ count)
    //【count】：1000
    val avgFlux = totalFlux/count
    println("【avgFlux】："+ avgFlux)
    //【avgFlux】：5043.746
    sc.stop()
  }
}
