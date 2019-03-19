package com.ww.sparksql.statproject.utils

import com.ww.sparksql.statproject.utils.Ip2CityUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

//将RDD基于该Schema转换为DataFrame
object DataFrameSchemaUtil {
  //定义输出的schema字段
  val schema = StructType(
    Array(
      StructField("url", StringType),
      //video还是audio还是book
      StructField("cmsType", StringType),
      StructField("cmsId", LongType),
      StructField("traffic", LongType),
      StructField("ip", StringType),
      StructField("city", StringType),
      StructField("time", StringType),
      StructField("day", StringType)
    )
  )

  //将formatjob输出的日志，schema后输出；含IP地址到city的转换
  def schemaFormatLog(log: String): Row = {
    try {
      val splits = log.split("\t")

      val url = splits(1)
      val traffic = splits(2).toLong
      val ip = splits(3)

      val domain = "http://www.videonet.com/"
      val cms = url.substring(url.indexOf(domain) + domain.length)
      val cmsTypeId = cms.split("/")
      var cmsType = ""
      var cmsId = 0l
      if (cmsTypeId.length > 1) {
        cmsType = cmsTypeId(0)
        cmsId = cmsTypeId(1).toLong
      }
      val city = Ip2CityUtil.getCityByIp(ip)
      val time = splits(0)
      val day = time.substring(0,10).replaceAll("-", "")

      Row(url, cmsType, cmsId, traffic, ip, city, time, day)
    } catch {
      case e: Exception => Row(0)
    }
  }
}
