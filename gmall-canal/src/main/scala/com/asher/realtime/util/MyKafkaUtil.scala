package com.asher.realtime.util

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * Kafka工具类，将数据发送到Kafka
 *
 * @Author Asher Wu
 * @Date 2020/8/18 8:58
 * @Version 1.0
 */
object MyKafkaUtil {
  val properties = new Properties()
  properties.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
  properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](properties)

  def sendToKafKa(topicName: String, content: String): Unit = {
    producer.send(new ProducerRecord[String,String](topicName,content))
  }

}
