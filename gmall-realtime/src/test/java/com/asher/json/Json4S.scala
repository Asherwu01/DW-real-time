package com.asher.json

import com.asher.realtime.bean.EventLog
import org.json4s.jackson.JsonMethods
import org.junit.Test

/**
 * @Author Asher Wu
 * @Date 2020/8/21 16:56
 * @Version 1.0
 */
class Json4S {

  @Test
  def testJson4S(): Unit = {
    implicit var format = org.json4s.DefaultFormats
    var json = "{\"logType\":\"event\",\"area\":\"beijing\",\"uid\":\"uid5774\",\"eventId\":\"coupon\",\"itemId\":38,\"os\":\"android\",\"nextPageId\":33,\"appId\":\"gmall\",\"mid\":\"mid_58\",\"pageId\":50,\"ts\":1598000978921}"
    println(json)
    val jValue = JsonMethods.parse(json)
    val eventLog = jValue.extract[EventLog]
    println(JsonMethods.parse(json).extract[EventLog])
  }
}
