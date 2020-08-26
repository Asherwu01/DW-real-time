package com.asher.realtime.util

import com.asher.realtime.bean.AlertInfo
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, Index}
import org.apache.spark.rdd.RDD

/**
 * @Author Asher Wu
 * @Date 2020/8/22 16:05
 * @Version 1.0
 */
object ESUtil {
  // 获取es客户端工厂，设置相应配置
  private val factory = new JestClientFactory
  val esUri = "http://hadoop102:9200"
  private val config: HttpClientConfig = new HttpClientConfig.Builder(esUri)
    .maxTotalConnection(100)
    .connTimeout(1000 * 10)
    .readTimeout(1000 * 10)
    .build()

  factory.setHttpClientConfig(config)

  /**
   * 每次插入单挑数据
   *
   * @param index
   * @param source 一般写 样例类或json字符串
   * @param id     如果不传，自动生成
   */
  def insertSingle(index: String, source: Object, id: String = null) = {
    val client = factory.getObject

    val builder = new Index.Builder(source)
    val indexAction = builder.index(index)
      // type 固定为 "_doc"
      .`type`("_doc")
      .build()

    client.execute(indexAction)
    client.shutdownClient()
  }

  /**
   *
   * @param index
   * @param sources (id,User(name,age)) 或 User(name,age) 的迭代器
   */
  def insertBulk(index: String, sources: Iterator[Object]) = {
    val client: JestClient = factory.getObject
    val bulkBuilder = new Bulk.Builder()
      .defaultIndex(index)
      .defaultType("_doc")

    sources.foreach {
      case (id: String, source) =>
        val action = new Index.Builder(source)
          .id(id)
          .build()

        bulkBuilder.addAction(action)
      case source =>
        val action = new Index.Builder(source)
          .build()

        bulkBuilder.addAction(action)
    }

    client.execute(bulkBuilder.build())

    client.shutdownClient()
  }

  // 插入数据格式为 case class
  case class User(name: String, age: Int)

  /**
   * 隐式类
   * @param rdd
   */
  implicit class RichRDD(rdd: RDD[AlertInfo]) {
    def saveToES(index: String) = {
      rdd.foreachPartition((infoIt: Iterator[AlertInfo]) => {
        // 每分钟只记录一次预警   id:  mid_分钟
        ESUtil.insertBulk(index, infoIt.map(info => (s"${info.mid}:${System.currentTimeMillis() / 1000 / 60}", info)))
      })
    }
  }

  def main(args: Array[String]): Unit = {
    /*insertBulk("user", List(
                ("1", User("a", 20)),
                ("2", User("b", 30)),
                ("3", User("c", 40))).toIterator)*/

    insertBulk("user", List(
      User("a", 20),
      User("b", 30),
      ("3", User("c", 40))).toIterator)
  }


}
