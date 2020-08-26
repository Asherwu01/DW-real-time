package com.asher.realtime.util

import redis.clients.jedis.Jedis

/**
 * @Author Asher Wu
 * @Date 2020/8/18 11:12
 * @Version 1.0
 */
object RedisUtil {
  val host = "hadoop102"
  val port = 6379

  def getRedisClient() = {
    new Jedis(host, port)
  }
}
