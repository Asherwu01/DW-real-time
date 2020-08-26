package com.asher.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @Author Asher Wu
 * @Date 2020/8/18 11:18
 * @Version 1.0
 */
case class StartupLog(mid: String,
                      uid: String,
                      appId: String,
                      area: String,
                      os: String,
                      channel: String,
                      logType: String,
                      version: String,
                      ts: Long,
                      var logDate: String,
                      var logHour: String){
  private val date = new Date(ts)
  logDate = new SimpleDateFormat("yyyy-MM-dd").format(date)
  logHour = new SimpleDateFormat("HH").format(date)
}

