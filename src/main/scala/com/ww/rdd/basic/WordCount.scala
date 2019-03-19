package com.ww.rdd.basic

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("word count").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")

    val path = "./data/OriginalData/words.txt"
    val textFile = sc.textFile(path)
    val wc = textFile.flatMap(line=>line.split(" "))
      .map(_.toLowerCase).map((_,1)).reduceByKey(_+_)

    //持久化，详见我的博客https://www.cnblogs.com/wwcom123/p/10445394.html
    //序列化为字节数组后省存储空间，但序列化和反序列化耗CPU
    wc.persist(StorageLevel.MEMORY_ONLY)
    wc.cache()

    val sortwc = wc.map(x=>(x._2, x._1)).sortByKey(false).map(x=>(x._2, x._1))

    sortwc.collect().foreach(println)


    /*
(you,3)
(fine,2)
(world,2)
(are,1)
(thank,1)
(how,1)
(hello,1)
(and,1)
    */
    wc.repartition(1).saveAsTextFile("D:/ideaworkspace/spark/sparksql/data/wordcount/output/wcresult")
    sc.stop()
  }
}
