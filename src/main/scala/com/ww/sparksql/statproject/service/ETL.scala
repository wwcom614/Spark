package com.ww.sparksql.statproject.service

import com.ww.sparksql.statproject.utils.DateConvertUtil
import org.apache.spark.sql.SparkSession


//【数据流--2】：ETL
//从【数据流--1】生成的数据源文件OriginalData.txt，抽取分析所需字段，然后ETL清洗、转换为待进行Rdd2DataFrame前的文件。
object ETL {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ETL").master("local[2]").getOrCreate()

    val rdd = spark.sparkContext.textFile("./data/OriginalData/sparksql_statproject_OriginalData.txt")
    // rdd.collect().foreach(println)调试测试数据确认读取情况
    rdd.map(line => {
      val split = line.split("\t")
      val ip = split(0)
      val url = split(9).replaceAll("\"", "")
      val traffic = split(8)
      val time = split(3)
      DateConvertUtil.getTargetTime(time) + "\t" + url + "\t" + traffic + "\t" + ip
    }).repartition(1).saveAsTextFile("./data/ETL")

    spark.stop()
  }
}
