package com.asher.realtime.app

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author Asher Wu
 * @Date 2020/8/19 19:06
 * @Version 1.0
 */
trait BaseApp {
  var appName:String

  def runTask(ssc:StreamingContext):Unit

  def main(args: Array[String]): Unit = {
    // 获取sparkstreaming运行时环境
    val conf = new SparkConf().setMaster("local[2]").setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(3))

    //子类要实现的功能
    runTask(ssc)

    // 启动流
    ssc.start()
    // 阻止进程退出
    ssc.awaitTermination()
  }
}
