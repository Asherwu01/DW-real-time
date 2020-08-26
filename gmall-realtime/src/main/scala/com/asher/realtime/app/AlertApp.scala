package com.asher.realtime.app

import com.asher.realtime.bean.{AlertInfo, EventLog}
import com.asher.realtime.util.MyKafkaUtil
import com.asher.util.Constant
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.json4s.jackson.JsonMethods
import java.util

/**
 * 需求：
 * 1.	同一设备(按照设备分组)
 * 2.	5分钟内(window)
 * 3.	三个不同账号登录
 * 4.	领取优惠券
 * 5.	并且没有浏览商品
 * 6.	同一设备每分钟只预警一次(同一设备, 每分钟只向 es 写一次记录)
 *
 * @Author Asher Wu
 * @Date 2020/8/21 11:10
 * @Version 1.0
 */
object AlertApp extends BaseApp {
  override var appName: String = "alertApp"
  implicit var format = org.json4s.DefaultFormats

  override def runTask(ssc: StreamingContext): Unit = {
    // 1.获取kafka消费者流 Event-log数据流,每次统计最近5min的数据
    val sourceStream = MyKafkaUtil
      .getKafkaStream(ssc, Constant.EVENT_LOG_TOPIC)
      .window(Minutes(5), Seconds(6))

    // 2.封装成case class, Json4s解析
    val eventLogStream = sourceStream.map(json => JsonMethods.parse(json).extract[EventLog])

    // 3.ETL 对同一个设备：三个不同账号登录 领取优惠券 没有浏览商品
    // 3.1按设备id分组
    val groupedEventLogStream = eventLogStream
      .map(eventLog => (eventLog.mid, eventLog))
      .groupByKey()

    // 3.2 计算领用优惠券的情况
    // {"logType":"startup","area":"shanghai","uid":"uid408","os":"android","appId":"gmall","channel":"wandoujia","mid":"mid_184","version":"1.1.2"}
    val alertInfoStream = groupedEventLogStream.map({
      case (mid, itr) =>
        // 保存领取优惠券的uid
        val uids: util.HashSet[String] = new util.HashSet[String]()
        // 保存优惠券对应的商品信息
        val items = new util.HashSet[String]()
        // 保存5min内的所有事件
        val events = new util.ArrayList[String]()

        // 5min内，是否浏览过商品，默认false，没浏览
        var isClicked = false

        itr.foreach(eventLog => { // 同一台设备的所有操作记录
          events.add(eventLog.eventId)
          eventLog.eventId match {
            case "coupon" =>
              uids.add(eventLog.uid)
              items.add(eventLog.itemId)
            case "clickItem" =>
              //浏览过商品,一个设备，只要有一个账号浏览过商品，就不报警
              isClicked = true
            case _ =>
          }
        })

        // 返回预警信息 (是否预警，预警信息)
        (uids.size() >= 3 && !isClicked, AlertInfo(mid, uids, items, events, System.currentTimeMillis()))
    })

    alertInfoStream.cache()
    alertInfoStream.print(1000)
    // 4.写入es,在kibana中通过进行可视化展示
    alertInfoStream
      .filter(_._1)
      .map(_._2)
      .foreachRDD(
        rdd => {
          // 写入 es
          import com.asher.realtime.util.ESUtil._
          rdd.saveToES("gmall_coupon_alert")
        }
      )
    alertInfoStream.print(1000)
  }
}
