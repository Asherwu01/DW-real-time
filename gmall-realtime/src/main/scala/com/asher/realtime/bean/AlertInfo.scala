package com.asher.realtime.bean

/**
 * @Author Asher Wu
 * @Date 2020/8/21 14:55
 * @Version 1.0
 */
case class AlertInfo(mid: String,
                     uids: java.util.HashSet[String],
                     itemIds: java.util.HashSet[String],
                     events: java.util.List[String],
                     ts: Long)

