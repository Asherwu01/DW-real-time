package com.asher.realtime.util

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
/**
 * @Author Asher Wu
 * @Date 2020/8/18 8:58
 * @Version 1.0
 */
object MyKafkaUtil {
    var kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-stream-gmall",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
  //获取kafka消费者流
  def getKafkaStream(ssc: StreamingContext, topic: String) = {

    val topics = Array(topic)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record => record.value)
  }

  //获取kafka消费者流
  def getKafkaStream(ssc: StreamingContext, topic: String,groupId:String): DStream[String] = {
    kafkaParams += "group.id" -> groupId
    getKafkaStream(ssc,topic)
  }

}
