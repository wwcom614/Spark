package com.ww.sparksql.statproject.utils

import com.ggstar.util.ip.IpHelper

//将IP地址转换为city
object Ip2CityUtil {
    def getCityByIp(ip: String): String ={
       IpHelper.findRegionByIp(ip)
    }

  //测试
  def main (args: Array[String] ): Unit = {
    val region = getCityByIp("58.30.15.255")
    println(region)
  }
}


