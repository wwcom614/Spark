package com.ww.sparksql.statproject.dao

import java.sql.{Connection, PreparedStatement}

import com.ww.sparksql.statproject.model.{DayVideoAccess, DayVideoCityAccess, DayVideoTraffics}
import com.ww.sparksql.statproject.utils.MySQLUtil
import com.ww.sparkstreaming.foreachrdd.MysqlPool

import scala.collection.mutable.ListBuffer

///用于将数据批量插入mysql
object OperateMySQLDao {

  //重新插入某日数据前，delete某日数据
  def deleteData(day: String): Unit = {
    val tables = Array("day_video_topn_stat", "day_video_city_topn_stat", "day_video_traffics_topn_stat")
    var connection: Connection = null
    var psmt: PreparedStatement = null

    try {
      connection = MySQLUtil.getConnection()
      for (table <- tables) {
        val deleteSQL = s"delete from $table where day = ?"
        psmt = connection.prepareStatement(deleteSQL)
        psmt.setString(1, day)
        psmt.executeUpdate()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtil.release(connection, psmt)
    }
  }


  //插入统计1结果到 day_video_topn_stat 表中
  def insertDayVideoTopN(list: ListBuffer[DayVideoAccess]): Unit = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySQLUtil.getConnection()

      //批量操作前，关闭自动提交
      connection.setAutoCommit(false)

      val sql = "insert into day_video_topn_stat(day ,cmsid ,times) values(?,?,?)"
      pstmt = connection.prepareCall(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.times)
        //加入批量提交Batch
        pstmt.addBatch()

      }
      //执行一次批量提交
      pstmt.executeBatch()
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtil.release(connection, pstmt)
    }
  }

  //插入统计2结果到 day_video_city_topn_stat 表中
  def insertVideoCityTopN(list: ListBuffer[DayVideoCityAccess]): Unit = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySQLUtil.getConnection()

      //批量操作前，关闭自动提交
      connection.setAutoCommit(false)

      val sql = "insert into day_video_city_topn_stat(day,cmsid,city,times,times_rank) values(?,?,?,?,?)"

      pstmt = connection.prepareCall(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setString(3, ele.city)
        pstmt.setLong(4, ele.times)
        pstmt.setInt(5, ele.timesRank)
        pstmt.addBatch()
      }
      pstmt.executeBatch()
      connection.commit()

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtil.release(connection, pstmt)
    }
  }

  //插入统计3结果到 day_video_traffics_topn_stat 表中
  def insertDayVideoTrafficsTopN(list: ListBuffer[DayVideoTraffics]): Unit = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySQLUtil.getConnection()

      //批量操作前，关闭自动提交
      connection.setAutoCommit(false)

      val sql = "insert into day_video_traffics_topn_stat(day ,cmsid ,traffics) values(?,?,?)"
      pstmt = connection.prepareCall(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.traffics)
        //加入批量提交Batch
        pstmt.addBatch()

      }
      //执行一次批量提交
      pstmt.executeBatch()
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtil.release(connection, pstmt)
    }
  }

}
