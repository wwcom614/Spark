package com.ww.sparksql.statproject.service

import com.ww.sparksql.statproject.utils.DataFrameSchemaUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

//【数据流--3】：Rdd->DataFrame
//在【数据流--2】ETL输出文件基础上，使用createDataFrame方法，基于DataFrameSchemaUtil工具类，schema好dataframe到parquet文件中，待统计分析stat.scala处理。
object Rdd2DataFrame {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]").appName("Rdd2DataFrame").getOrCreate()

    val rdd = spark.sparkContext.textFile("./data/ETL/")

    //rdd.collect().foreach(println)

    val df = spark.createDataFrame(rdd.map(x => DataFrameSchemaUtil.schemaFormatLog(x)), DataFrameSchemaUtil.schema)

    //df.printSchema()
    //df.show(false)
    df.coalesce(1).write.mode(SaveMode.Overwrite).format("parquet").partitionBy("day").save("./data/Rdd2DataFrame")
    spark.stop()
  }
}
