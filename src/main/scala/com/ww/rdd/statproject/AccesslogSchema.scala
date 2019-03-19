package com.ww.rdd.statproject

case class AccesslogSchema(
                            ipAddress: String, // 客户端ip地址
                            clientIndentd: String, //客户端标识符
                            userId: String, //用户ID
                            dateTime: String, //时间
                            method: String, //http请求方式
                            url: String, //用户访问URL
                            protocol: String, //协议
                            responseCode: Int, //网站返回Http响应码
                            flux: Long //访问流量
                          )

object AccesslogSchema {
  def parseLog(line: String): AccesslogSchema = {
    val logArray = line.split("#")
    val method_url_protocol = logArray(4).split(" ")

    AccesslogSchema(logArray(0), logArray(1), logArray(2), logArray(3), method_url_protocol(0), method_url_protocol(1), method_url_protocol(2), logArray(5).toInt, logArray(6).toLong);
  }

}