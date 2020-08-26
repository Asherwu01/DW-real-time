package com.asher.realtime.app

import com.alibaba.fastjson.JSON
import com.asher.realtime.bean.OrderInfo
import com.asher.realtime.util.MyKafkaUtil
import com.asher.util.Constant
import org.apache.spark.streaming.StreamingContext

/**
 * @Author Asher Wu
 * @Date 2020/8/19 19:05
 * @Version 1.0
 */
object OrderApp extends BaseApp {
  override var appName: String = "OrderApp"
  override def runTask(ssc: StreamingContext): Unit = {
    // 1.获取kafka消费者流
    val kafkaStream = MyKafkaUtil.getKafkaStream(ssc, Constant.ORDER_INFO_TOPIC)
    val orderInfoStream = kafkaStream.map(json => JSON.parseObject(json, classOf[OrderInfo]))

    // 2.存入hbase
    import org.apache.phoenix.spark._
    orderInfoStream.foreachRDD(rdd =>{
      rdd.saveToPhoenix(
        "GMALL_ORDER_INFO",
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        zkUrl = Option("hadoop102,hadoop103,hadoop104:2181"))
    })

  }

}
