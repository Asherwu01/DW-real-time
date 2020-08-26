package com.asher.realtime.app

import com.alibaba.fastjson.JSON
import com.asher.realtime.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.asher.realtime.util.{ESUtil, MyKafkaUtil, RedisUtil}
import com.asher.util.Constant
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

/**
 * @Author Asher Wu
 * @Date 2020/8/24 12:24
 * @Version 1.0
 */
object SaleApp extends BaseApp {
  override var appName: String = "saleApp"

  /**
   * 获取order_info 和 order_detail 的数据流
   *
   * @param ssc
   */
  def getOrderInfoAndOrderDetailStream(ssc: StreamingContext): (DStream[OrderInfo], DStream[OrderDetail]) = {
    val orderInfoStream: DStream[OrderInfo] = MyKafkaUtil
      .getKafkaStream(ssc, Constant.ORDER_INFO_TOPIC)
      .map(json => JSON.parseObject(json, classOf[OrderInfo]))

    val orderDetailStream: DStream[OrderDetail] = MyKafkaUtil
      .getKafkaStream(ssc, Constant.ORDER_DETAIL_TOPIC)
      .map(json => JSON.parseObject(json, classOf[OrderDetail]))

    (orderInfoStream, orderDetailStream)
  }


  /**
   * 传入两个流，并对这两个流 full join
   *
   * @param orderInfoStream
   * @param orderDetailStream
   * @return
   */
  def fullJoin(orderInfoStream: DStream[OrderInfo], orderDetailStream: DStream[OrderDetail]): DStream[SaleDetail] = {
    /**
     * 保存 k-v对 到 redis
     *
     * @param client
     * @param key        k
     * @param expireTime 过期时间
     * @param value      v
     * @return
     */
    def sendToRedis(client: Jedis, key: String, expireTime: Long, value: String) = {
      client.psetex(key, expireTime, value)
    }

    def cacheOrderInfo(client: Jedis, orderInfo: OrderInfo) = {
      // 缓存orderInfo到redis
      implicit val f = org.json4s.DefaultFormats
      sendToRedis(client, s"order_info:${orderInfo.id}", 30 * 60, Serialization.write(orderInfo))
    }

    // 缓存orderDetail到redis
    def cacheOrderDetail(client: Jedis, orderDetail: OrderDetail) = {
      // 缓存orderDetail到redis
      implicit val f = org.json4s.DefaultFormats
      sendToRedis(client, s"order_detail:${orderDetail.order_id}:${orderDetail.id}", 30 * 60, Serialization.write(orderDetail))
    }

    // 1.先把两个流转换成k-v，然后才能进行join
    val orderIdToOrderInfoStream: DStream[(String, OrderInfo)] = orderInfoStream.map(orderInfo => (orderInfo.id, orderInfo))
    val orderIdToOrderDetailStream: DStream[(String, OrderDetail)] = orderDetailStream.map(orderDetail => (orderDetail.order_id, orderDetail))

    // 2.full join连接两个流  itr: Iterator[(String,(Option[OrderInfo],Option[OrderDetail]))]
    orderIdToOrderInfoStream.fullOuterJoin(orderIdToOrderDetailStream).mapPartitions((itr: Iterator[(String, (Option[OrderInfo], Option[OrderDetail]))]) => {
      // 2.1建立redis连接
      val client = RedisUtil.getRedisClient()

      // 2.2操作redis,根据不同的情况，将order_info、order_detail组合在一起
      val result = itr.flatMap {
        // order_info和order_detail同时到达
        case (orderId, (Some(orderInfo), Some(orderDetail))) =>
          // 1.把orderInfo的信息写入到缓存
          cacheOrderInfo(client, orderInfo)
          // 2.组合orderInfo和orderDetail
          val saleDetail = SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
          // 3.从orderDetail的缓存中查找对应信息；之后删除查询到的数据
          // 3.1先从缓存中获取和order_id相关的所有信息
          // 3.2在根据key取出对应的value(orderDetail),组合orderInfo、orderDetail
          val ids = client.keys(s"order_detail:${orderId}*")
          import scala.collection.JavaConverters._
          val saleDetails = ids.asScala.map(id => {
            val json = client.get(id)
            val cachedOrderDetail = JSON.parseObject(json, classOf[OrderDetail])
            SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(cachedOrderDetail)
          })
          saleDetails += saleDetail

        // order_info和order_detail没同时到达的两种情况
        // 情况一：
        case (orderId, (Some(orderInfo), None)) =>
          // 1.把orderInfo的信息缓存
          cacheOrderInfo(client, orderInfo)
          // 2.从orderDetail的缓存中读取对应orderDetail;读完后删除读到的数据
          // 2.1先从缓存中获取和order_id相关的所有信息
          // 2.2在根据key取出对应的value(orderDetail),组合orderInfo、orderDetail
          val ids = client.keys(s"order_detail:${orderId}*")
          import scala.collection.JavaConverters._
          val saleDetails = ids.asScala.map(id => {
            val json = client.get(id)
            val cachedOrderDetail = JSON.parseObject(json, classOf[OrderDetail])
            SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(cachedOrderDetail)
          })
          saleDetails

        // 情况二：
        case (orderId, (None, Some(orderDetail))) =>
          // 1.从orderInfo缓存中读对应orderInfo，并组合orderInfo和orderDetail
          val orderInfoJsonStr = client.get(s"order_info:${orderId}")
          if (orderInfoJsonStr != null) {
            // 2.orderInfo在缓存中有，组合
            val cachedOrderInfo = JSON.parseObject(orderInfoJsonStr, classOf[OrderInfo])
            val saleDetail = SaleDetail().mergeOrderInfo(cachedOrderInfo).mergeOrderDetail(orderDetail)
            saleDetail :: Nil
          } else {
            // 3.orderInfo缓存中没有，orderDetail加入orderDetail缓存
            cacheOrderDetail(client, orderDetail)
            Nil
          }
      }

      // 2.3关闭redis
      client.close()
      result
    })

  }

  /**
   * 使用spark-sql读取mysql的数据
   * 补全saleDetailStream的user信息
   *
   * @param saleDetailStream
   * @param ssc
   * @return
   */
  def joinUserInfo(saleDetailStream: DStream[SaleDetail], ssc: StreamingContext): DStream[SaleDetail] = {
    // 1.获取spark-sql运行环境
    val sparkSession = SparkSession
      .builder()
      .config(ssc.sparkContext.getConf)
      .getOrCreate()

    // 2.通过id读取对应user信息
    def readUserInfos(ids: String): RDD[UserInfo] = {
      import sparkSession.implicits._
      sparkSession
        .read
        .format("jdbc")
        .option("user", "root")
        .option("password", "root")
        .option("url", "jdbc:mysql://hadoop102:3306/gmall_realtime?useSSL=false")
        .option("query", s"select * from user_info where id in (${ids})")
        .load()
        .as[UserInfo]
        .rdd
    }

    // 3.连接saleDetailStream 和 rdd
    saleDetailStream.map(saleDetail => (saleDetail.user_id, saleDetail)).transform((saleDetailRDD: RDD[(String, SaleDetail)]) => {
      // 3.1获取所有saleDetailRDD中userId
      val ids: String = saleDetailRDD.map(_._2.user_id).collect().mkString("'", "','", "'") // 1','2','3
      val userInfoRDD: RDD[(String, UserInfo)] = readUserInfos(ids).map(userInfo => (userInfo.id, userInfo))
      // 3.2 将对应userInfo信息合并到saleDetail
      saleDetailRDD.join(userInfoRDD).map {
        case (_, (seleDetail, userInfo)) =>
          seleDetail.mergeUserInfo(userInfo)
      }
    })
  }

  /**
   * 保存数据到es
   *
   * @param saleDetailStream
   */
  def saveToEs(saleDetailStream: DStream[SaleDetail]): Unit = {
    saleDetailStream.foreachRDD(saleDetailRdd =>{
      saleDetailRdd.foreachPartition(saleDetailItr=>{
        ESUtil.insertBulk("gmall_sale_detail",saleDetailItr.map(sale=>(s"${sale.order_id}:${sale.order_detail_id}:",sale)))
      })
    })
  }

  override def runTask(ssc: StreamingContext): Unit = {

    // 1.获取 order_info,order_detail 两个流
    val (orderInfoStream, orderDetailStream) = getOrderInfoAndOrderDetailStream(ssc)

    // 2.连接两个流，full join
    val saleDetailStream: DStream[SaleDetail] = fullJoin(orderInfoStream, orderDetailStream)


    // 3.使用spark sql 去mysql反查userInfo，补全user数据
    val result: DStream[SaleDetail] = joinUserInfo(saleDetailStream, ssc)

    // 4.把宽表数据(组合好的数据)写入es
    saveToEs(result)
  }
}

/*
在redis缓存order_info和order_detail的时候, 如何存?
    方便去做缓存
    方便从缓存取数据

order_detail(1):
    key                                                     value
    "order_detail:${order_id}:${order_detail_id}"           order_detail数据的json数据

    好处: 1.方便存取 2. 每个key可以分别设置不同的过期时间
    坏处: key可能过多

order_detail(2):
    key                                                     value
    "order_detail:${order_id}"                               hash
                                                            field                   value
                                                            oder_detail_id          order_detail数据的json数据

      好处:   1. key比较少. 一个订单才有一个key
      坏处:   1. 没有办法设置不同的过期时间


order_info:
   key                                  value
   "order_info:${order_id}"             order_info的json格式的数据



fastjson:
    直接把json字符串解析成样例类是没有问题

    但是, 如果把样例类对象序列化成json字符串是不行的! serialization.write

 */