package com.ww.sparkstreaming.foreachrdd

import org.apache.spark._
import org.apache.spark.streaming._

//使用foreachRDD，每次批量获取一个partition的数据处理，而不是逐条处理，减少IO提升性能。
object ForEachRDDMySql {
  def main(args: Array[String]): Unit = {
    //sparkstreaming至少要开启2个线程，1个线程(receiver-task)接收数据，另外N个线程处理数据。
    val conf = new SparkConf().setMaster("local[2]").setAppName("SocketTextStream")

    //receiver-task接收流式数据后，每隔Block interval(默认200毫秒)生成1个block，存储到executor的内存中，
    // 为保证高可用，还会有副本。注：有M个block就有M个task的并行度。
    // StreamingContext每隔Batch inverval(入参Seconds秒)时间间隔，blocks组成一个RDD。
    val ssc = new StreamingContext(conf, Seconds(5))


    val lines = ssc.socketTextStream("127.0.0.1", 9999)
    val wordcount = lines.flatMap { line => line.split(",") }
      .map { word => (word, 1) }
      .reduceByKey(_ + _)


    //对于统计结果入MySQL等数据库场景，foreachRDD每次批量获取一个partition的数据建立一个数据库连接批量入库，
    // 减少了数据库连接的创建，生产中常用
    wordcount.foreachRDD(partitionOfRecords => {
      partitionOfRecords.foreachPartition(records => {
        //从数据库连接池中获取连接
        val connection = MysqlPool.getJdbcConn()
        while (records.hasNext) {
          val tuple = records.next()
          //注意sql语句中，varchar类型要加单引号
          val sql = "insert into word_count_result values( now(),'" + tuple._1 + "', " + tuple._2.toInt + ")"
          val statement = connection.createStatement()
          statement.executeUpdate(sql)
          print(sql)
        }
        //操作完数据库，释放连接，归还给数据库连接池
        MysqlPool.releaseConn(connection)
      })
    })

    /*
    输入：
    cd D:\software\netcat1.12\
    nc -l -p 9999
    hello,world,hello,world,hello,ww
    */

    /*数据库中结果正确
        count_time	word	count
        2019-03-18 12:42:06	hello	3
        2019-03-18 12:42:06	world	2
        2019-03-18 12:42:06	ww	1
    */

    //启动应用
    ssc.start()
    //等待任务结束
    ssc.awaitTermination()
  }
}
