package com.ww.sparksql.statproject.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

//数据录入MySQL
object MySQLUtil {

  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/sparkdb?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true&useSSL=false")
  }

  def main(args: Array[String]) = {
    print(getConnection())
  }

  def release(connection: Connection, pstmt: PreparedStatement) = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }
}
