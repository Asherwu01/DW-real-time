package com.asher.realtime.app

import com.alibaba.fastjson.JSON
import com.asher.realtime.bean.StartupLog
import com.asher.realtime.util.{MyKafkaUtil, RedisUtil}
import com.asher.util.Constant
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author Asher Wu
 * @Date 2020/8/18 10:17
 * @Version 1.0
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    // 1.获取sparkstreaming运行时环境
    val conf = new SparkConf().setMaster("local[2]").setAppName("DauAPP")
    val ssc = new StreamingContext(conf, Seconds(3))

    // 2.获取kafka消费者流
    val kafkaStream = MyKafkaUtil.getKafkaStream(ssc, Constant.STARTUP_LOG_TOPIC)
    val startupLogStream = kafkaStream.map(log => {
      val startupLog: StartupLog = JSON.parseObject(log, classOf[StartupLog])
      startupLog
    })

    // 3.去重,通过redis
    val filteredStartupLogStream = startupLogStream.mapPartitions(startupLogItr => {
      val client = RedisUtil.getRedisClient()

      val DeduplicationItr = startupLogItr.filter(log => {
        // key--"mids:date" value--mid
        val key = s"mids:${log.logDate}"
        val value = log.mid
        val isExist = client.sadd(key, value)

        isExist == 1
      })
      client.close()
      DeduplicationItr
    })
    // 4.存入hbase
    import org.apache.phoenix.spark._
    filteredStartupLogStream.foreachRDD(rdd =>{
      rdd.saveToPhoenix(
        "GMALL_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CHANNEL", "LOGTYPE", "VERSION", "TS", "LOGDATE", "LOGHOUR"),
        zkUrl = Option("hadoop102,hadoop103,hadoop104:2181"))
    })

    // 启动流
    ssc.start()
    // 阻止进程退出
    ssc.awaitTermination()
  }
}
