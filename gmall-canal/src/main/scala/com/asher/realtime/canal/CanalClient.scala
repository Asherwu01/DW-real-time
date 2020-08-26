package com.asher.realtime.canal

import java.net.InetSocketAddress
import java.util

import com.alibaba.fastjson.JSONObject
import com.alibaba.otter.canal.client.CanalConnectors
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.{EntryType, EventType, RowChange}
import com.asher.realtime.util.MyKafkaUtil
import com.asher.util.Constant

import scala.collection.JavaConverters._

/**
 * @Author Asher Wu
 * @Date 2020/8/19 10:04
 * @Version 1.0
 */
object CanalClient {

  def handleRowDatas(rowDatas: util.List[CanalEntry.RowData], tableName: String, eventType: EventType) = {
    if (tableName == "order_info" && eventType == EventType.INSERT && rowDatas != null && !rowDatas.isEmpty) {
      handleRowDataUtil(rowDatas, Constant.ORDER_INFO_TOPIC)
    } else if (tableName == "order_detail" && eventType == EventType.INSERT && rowDatas != null && !rowDatas.isEmpty) {
      handleRowDataUtil(rowDatas, Constant.ORDER_DETAIL_TOPIC)
    }
  }

  def handleRowDataUtil(rowDatas: util.List[CanalEntry.RowData], topic: String): Unit = {
    for (rowData <- rowDatas.asScala) {
      val obj = new JSONObject()
      // rowData中的所有列
      val columns = rowData.getAfterColumnsList
      for (column <- columns.asScala) {
        val name = column.getName
        val value = column.getValue
        obj.put(name, value)
        // 将数据发送到kafka
      }
      sendToKafka(topic, obj.toJSONString)
    }

    def sendToKafka(topicName: String, content: String) = {
      MyKafkaUtil.sendToKafKa(topicName, content)
    }
  }

  def main(args: Array[String]): Unit = {
    // 1.获取canal的连接对象
    // args: SocketAddress address, String destination, String username, String password
    val connector = CanalConnectors.newSingleConnector(
      new InetSocketAddress("hadoop102", 11111),
      "example",
      "",
      ""
    )

    // 2.连接到canal
    connector.connect()

    // 3.拉取数据，订阅哪些库和表
    connector.subscribe("gmall_realtime.*")

    // 4.解析数据 Massage Entrys StoreValue RowChange RowData
    while (true) {
      val message = connector.get(100)
      // 4.1 数据都在entry中
      val entries = message.getEntries

      if (entries != null && entries.size() > 0) {
        // 4.2 解析entry
        for (entry <- entries.asScala) {
          if (entry != null && entry.hasEntryType && entry.getEntryType == EntryType.ROWDATA) {
            val storeValue = entry.getStoreValue
            val rowChange = RowChange.parseFrom(storeValue)
            val rowDatas = rowChange.getRowDatasList

            // 4.3存入Kafka  解析到RowData，转换成jsonStr, 存入Kafka
            handleRowDatas(rowDatas, entry.getHeader.getTableName, rowChange.getEventType)
          }
        }
      } else {
        println("没有数据，3s后重新拉取")
        Thread.sleep(3000)
      }
    }


  }

}