package com.ww.sparksql.statproject.utils

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

//将dd/MMM/yyyy:HH:mm:ss Z的日期格式，转换为yyyy-MM-dd HH:mm:ss的日期格式工具类
object DateConvertUtil {
  val ddMMMyyyy_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy HH:mm:ss Z", Locale.ENGLISH)

  val TARGET_TIME_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  //parse方法是String转Date
  def parseddMMMyyyyTimeFormat(strTime:String): Date ={
    try{
      ddMMMyyyy_TIME_FORMAT.parse(strTime.substring(strTime.indexOf("[")+1, strTime.lastIndexOf("]")))
    }catch {
      case e: Exception => null
    }
  }

  //format方法是Date转String
  def getTargetTime(strTime : String): String ={
    TARGET_TIME_FORMAT.format(parseddMMMyyyyTimeFormat(strTime))
  }
}
