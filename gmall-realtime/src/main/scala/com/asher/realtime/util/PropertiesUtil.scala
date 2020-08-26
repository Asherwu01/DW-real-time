package com.asher.realtime.util

import java.io.InputStream
import java.util.Properties

/**
 * @Author Asher Wu
 * @Date 2020/8/18 8:47
 * @Version 1.0
 */
object PropertiesUtil {

  // 获取配置文件的输入流
  private val in: InputStream = ClassLoader.getSystemResourceAsStream("config.properties")
  private val properties = new Properties()
  // 加载配置文件到内存
  properties.load(in);

  def getProperty(propertyName: String): String = properties.getProperty(propertyName)

  def main(args: Array[String]): Unit = {
    println(getProperty("kafka.broker.list"))
  }
}
