package com.ww.sparkstreaming.foreachrdd

import java.sql.{Connection, DriverManager}

//写了个简单版的数据库连接池
object MysqlPool {
  //配置数据库允许的最大连接数
  private val maxConnections = 20

  //线程池每次批量申请的数据库连接数
  private val newConnections = 5

  //当前已经产生的连接数
  private var curConnections = 0

  import java.util

  //数据库连接池
  private val pool = new util.LinkedList[Connection]()

  //加载数据库驱动并控制连接数
  def loadDBDriver(): Unit = {
    //数据库连接池已没有空闲连接，并且当前已经产生的连接数<数据库允许的最大连接数，才允许加载驱动新建数据库连接
    if (pool.isEmpty() && curConnections < maxConnections) {
      Class.forName("com.mysql.cj.jdbc.Driver")
      //数据库连接池已没有空闲连接，并且当前已经产生的连接数>=数据库允许的最大连接数，拒绝加载数据库驱动连接，打印提示，等待2秒重新尝试
    } else if (pool.isEmpty() && curConnections >= maxConnections) {
      println("Jdbc Pool has no idle connections and now,please wait a moment.")
      Thread.sleep(2000)
      loadDBDriver()
    }
  }

  //应用操作完数据库后，释放Connection，把Connection数据库连接push回连接池
  def releaseConn(conn: Connection): Unit = {
    pool.push(conn)
  }

  //获取数据库连接
  def getJdbcConn(): Connection = {
    //考虑多线程，使用同步代码块
    AnyRef.synchronized({
      //每次批量生成newConnections个数据库连接
      for (i <- 1 to newConnections) {
        val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/sparkdb?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true&useSSL=false", "root", "ww");
        pool.push(conn)
        curConnections + 1
      }
      pool.poll()
    })
  }

  //测试上述简单数据库连接池
  def main(args: Array[String]): Unit = {
    for (num <- 1 to 20) {
      val conn = getJdbcConn()
      println("【当前连接数】：" + num + "，【Connection】：" + conn)
      if (num % 6 == 0) {
        println("【释放连接数】：" + num + "，【Connection】：" + conn)
        releaseConn(conn)
      }
    }
  }
}
/*
【当前连接数】：1，【Connection】：com.mysql.cj.jdbc.ConnectionImpl@d83da2e
【当前连接数】：2，【Connection】：com.mysql.cj.jdbc.ConnectionImpl@4b44655e
【当前连接数】：3，【Connection】：com.mysql.cj.jdbc.ConnectionImpl@78b729e6
【当前连接数】：4，【Connection】：com.mysql.cj.jdbc.ConnectionImpl@13c10b87
【当前连接数】：5，【Connection】：com.mysql.cj.jdbc.ConnectionImpl@f8c1ddd
【当前连接数】：6，【Connection】：com.mysql.cj.jdbc.ConnectionImpl@1e730495
【释放连接数】：6，【Connection】：com.mysql.cj.jdbc.ConnectionImpl@1e730495
【当前连接数】：7，【Connection】：com.mysql.cj.jdbc.ConnectionImpl@7692d9cc
【当前连接数】：8，【Connection】：com.mysql.cj.jdbc.ConnectionImpl@2805c96b
【当前连接数】：9，【Connection】：com.mysql.cj.jdbc.ConnectionImpl@38c5cc4c
【当前连接数】：10，【Connection】：com.mysql.cj.jdbc.ConnectionImpl@3c72f59f
【当前连接数】：11，【Connection】：com.mysql.cj.jdbc.ConnectionImpl@11a9e7c8
【当前连接数】：12，【Connection】：com.mysql.cj.jdbc.ConnectionImpl@7219ec67
【释放连接数】：12，【Connection】：com.mysql.cj.jdbc.ConnectionImpl@7219ec67
【当前连接数】：13，【Connection】：com.mysql.cj.jdbc.ConnectionImpl@2928854b
【当前连接数】：14，【Connection】：com.mysql.cj.jdbc.ConnectionImpl@61dd025
【当前连接数】：15，【Connection】：com.mysql.cj.jdbc.ConnectionImpl@6973bf95
【当前连接数】：16，【Connection】：com.mysql.cj.jdbc.ConnectionImpl@309e345f
【当前连接数】：17，【Connection】：com.mysql.cj.jdbc.ConnectionImpl@4df50bcc
【当前连接数】：18，【Connection】：com.mysql.cj.jdbc.ConnectionImpl@3c9d0b9d
【释放连接数】：18，【Connection】：com.mysql.cj.jdbc.ConnectionImpl@3c9d0b9d
【当前连接数】：19，【Connection】：com.mysql.cj.jdbc.ConnectionImpl@2bbf180e
【当前连接数】：20，【Connection】：com.mysql.cj.jdbc.ConnectionImpl@5276e6b0
 */