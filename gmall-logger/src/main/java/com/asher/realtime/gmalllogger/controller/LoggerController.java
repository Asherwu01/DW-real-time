package com.asher.realtime.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.asher.util.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author Asher Wu
 * @Date 2020/8/17 16:39
 * @Version 1.0
 */
@RestController
public class LoggerController {

    /**
     * 给日志添加时间戳，然后落盘，供离线分析
     *
     * @param log
     * @return
     */
    @PostMapping("/log")
    public String doLog(@RequestParam String log) {
        // 1.给日志添加时间戳
        String json = addTs(log);

        // 2.将日志落盘
        saveToDisk(json);

        // 3.把数据发送到kafka
        saveToKafka(json);

        // System.out.println(json);
        return "ok";
    }

    /**
     * 将数据存入kafka
     * 需要提供 k v 对应的序列化器
     * @param log
     */
    @Autowired
    KafkaTemplate<String, String> kafka;
    private void saveToKafka(String log) {
        if (log.contains("startup")) {
            kafka.send(Constant.STARTUP_LOG_TOPIC,log);
        }else if (log.contains(log)){
            kafka.send(Constant.EVENT_LOG_TOPIC,log);
        }
    }

    Logger logger = LoggerFactory.getLogger(LoggerController.class);

    /**
     * 将数据保存到磁盘
     *
     * @param log
     */
    private void saveToDisk(String log) {
        logger.info(log);
    }

    /**
     * 将json对象解析成json字符串
     *
     * @param log
     * @return
     */
    private String addTs(String log) {
        JSONObject jsonObject = JSON.parseObject(log);
        jsonObject.put("ts", System.currentTimeMillis());
        String jsonStr = jsonObject.toJSONString();
        return jsonStr;
    }
}
