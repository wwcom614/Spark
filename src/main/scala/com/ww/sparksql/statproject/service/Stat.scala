package com.ww.sparksql.statproject.service

import com.ww.sparksql.statproject.dao.OperateMySQLDao
import com.ww.sparksql.statproject.model.{DayVideoAccess, DayVideoCityAccess, DayVideoTraffics}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

//agg等函数必须要引入该包
import org.apache.spark.sql.functions._

//window函数必须要引入该包
import org.apache.spark.sql.expressions._

//【数据流--4】：Stat
//基于【数据流--3】的Rdd2DataFrame.scala schema好的数据，使用SparkSQL实现3个统计，调用InsertMySQLDao将统计结果批量录入MySql。
object Stat {
  def main(args: Array[String]): Unit = {

    //分区字段的类型是自动识别的，目前支持numeric和string类型.关闭自动识别后，string类型会用于分区字段
    val spark = SparkSession.builder().config("spark.sql.sources.partitionColumnTypeInference.enabled", false).master("local[1]").appName("Stat").getOrCreate()

    val df = spark.read.format("parquet").load("./data/Rdd2DataFrame")
    //df.printSchema()
    //df.show(false)

    val day = "20181225"
    import spark.implicits._
    val commonDF = df.filter($"day" === day && $"cmsType" === "video")
    //将三种统计都需要的公用统计项抽取出来，cache复用，提升处理性能
    commonDF.cache()

    //插入某日统计数据之前，先删除该日数据
    OperateMySQLDao.deleteData(day)

    //统计1：某日URL点击TopN视频统计
    videoTopNStat(spark, commonDF)

    //统计2：某日分城市维度，URL点击TopN视频统计
    videoCityTopNStat(spark, commonDF)

    //统计3：某日访问流量TopN视频统计
    videoTrafficTopNStat(spark, commonDF)

    //用完了记得释放cache
    commonDF.unpersist(true)
    spark.stop()
  }

  //统计1：某日URL点击TopN视频统计
  def videoTopNStat(spark: SparkSession, commonDF: DataFrame): Unit = {
    //使用DataFrame API的方式进行统计
    import spark.implicits._
    val videoTopNDF = commonDF.groupBy("day", "cmsId").agg(count("cmsId") as ("times")).orderBy($"times".desc)


    //使用SparkSQL的方式进行统计
    //df.createOrReplaceTempView("dfView")
    //val videoTopNDF = spark.sql("select day,cmsId,count(1) as times from dfView where day='20161110' and cmsType='video' group by day,cmsId order by times desc")
    videoTopNDF.show(false)

    //将统计结果调用StatDao写入MySQL
    try {
      /*foreachPartition
      *1、对于我们写的function函数，就调用一次，一次传入一个partition所有的数据
      2、主要创建或者获取一个数据库连接就可以
      3、只要向数据库发送一次SQL语句和多组参数即可

      在实际生产环境中，清一色，都是使用foreachPartition操作；但是有个问题，跟mapPartitions操作一样，
      如果一个partition的数量真的特别特别大，比如真的是100万，那基本上就不太靠谱了。
      一下子进来，很有可能会发生OOM，内存溢出的问题。
      Foreach与ForeachPartition都是在每个partition中对iterator进行操作,
      不同的是,
      foreach是直接在每个partition中直接对iterator执行foreach操作,而传入的function只是在foreach内部使用,
      而foreachPartition是在每个partition中把iterator给传入的function,让function自己对iterator进行处理
      *
      * */

      videoTopNDF.foreachPartition(partitionOfRecords => {

        val list = new ListBuffer[DayVideoAccess]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")
          //先用list临时存储，然后将list将这个partition中的数据批量一次性插入数据库
          list.append(DayVideoAccess(day, cmsId, times))
        })

        OperateMySQLDao.insertDayVideoTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }


  //统计2：某日分城市维度，URL点击TopN视频统计
  def videoCityTopNStat(spark: SparkSession, commonDF: DataFrame) = {
    commonDF.createOrReplaceTempView("dfView")

    //使用DataFrame API的方式进行统计
    import spark.implicits._
    val videoCityTopNDF = commonDF.groupBy("day", "city", "cmsId").agg(count("cmsId") as ("times")).orderBy($"times".desc)

    //window函数在sparkSQL中的使用
    val top3DF = videoCityTopNDF.select(
      videoCityTopNDF("day"),
      videoCityTopNDF("city"),
      videoCityTopNDF("cmsId"),
      videoCityTopNDF("times"),
      //最后人为造出来一列，分每个城市的点击次数topN
      row_number().over(Window.partitionBy(videoCityTopNDF("city"))
        .orderBy(videoCityTopNDF("times").desc)
      ).as("times_rank")
    ).filter("times_rank <= 3") //.show(false)


    //使用SparkSQL的方式进行统计
    //val times_rank = "dense_rank() over(partition by day order by times desc) as times_rank"
    //val VideoCityTopDF = spark.sql(" select *," + times_rank + " from (select day,cmsId,city,count(1) as times from dfView where day='20161110' and cmsType='video' group by day,cmsId,city) limit 3")

    //videoCityTopNDF.show(false)

    try {
      top3DF.foreachPartition(partitionRecords => {
        val list = new ListBuffer[DayVideoCityAccess]

        partitionRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val city = info.getAs[String]("city")
          val times = info.getAs[Long]("times")
          val timesRank = info.getAs[Int]("times_rank")
          list.append(DayVideoCityAccess(day, cmsId, city, times, timesRank))
        })

        OperateMySQLDao.insertVideoCityTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  //统计3：某日访问流量TopN视频统计
  def videoTrafficTopNStat(spark: SparkSession, commonDF: DataFrame): Unit = {
    //使用DataFrame API的方式进行统计
    import spark.implicits._
    val videoTrafficTopNDF = commonDF.groupBy("day", "cmsId").agg(sum("traffic") as ("traffics")).orderBy($"traffics".desc) //.show(false)


    //将统计结果调用StatDao写入MySQL
    try {

      videoTrafficTopNDF.foreachPartition(partitionOfRecords => {

        val list = new ListBuffer[DayVideoTraffics]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val traffics = info.getAs[Long]("traffics")
          //先用list临时存储，然后将list将这个partition中的数据批量一次性插入数据库
          list.append(DayVideoTraffics(day, cmsId, traffics))
        })

        OperateMySQLDao.insertDayVideoTrafficsTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
