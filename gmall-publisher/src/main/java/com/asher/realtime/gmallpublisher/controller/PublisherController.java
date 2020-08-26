package com.asher.realtime.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.asher.realtime.gmallpublisher.bean.Option;
import com.asher.realtime.gmallpublisher.bean.SaleInfo;
import com.asher.realtime.gmallpublisher.bean.Stat;
import com.asher.realtime.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import sun.rmi.runtime.Log;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;

/**
 * @Author Asher Wu
 * @Date 2020/8/18 20:33
 * @Version 1.0
 */
@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    /**
     * 获取日活
     * <p>
     * // http://localhost:8084/realtime-total?date=2020-08-18
     *
     * @return
     */
    @GetMapping("/realtime-total")
    public String realtimeTotal(String date) {
        Long totalDau = publisherService.getDau(date);

        List<Map<String,String>> result = new ArrayList<>();
        HashMap<String, String> map1 = new HashMap<>();
        map1.put("id", "dau");
        map1.put("name", "新增日活");
        map1.put("value", totalDau.toString());
        result.add(map1);

        Map<String, String> map2 = new HashMap<>();
        map2.put("id", "new_mid");
        map2.put("name", "新增设备");
        map2.put("value", "233");
        result.add(map2);

        Map<String, String> map3 = new HashMap<>();
        map3.put("id", "order_amount");
        map3.put("name", "新增交易额");
        map3.put("value", publisherService.getTotalAmount(date).toString());
        result.add(map3);

        // System.out.println(result);
        return JSON.toJSONString(result);
    }

    /**
     * 获取每小时日活明细  每小时订单明细
     *
     * http://localhost:8084/realtime-hour?id=dau&date=2020-08-18
     * http://localhost:8084/realtime-hour?id=order_amount&date=2020-08-19
     *
     * @param id
     * @param date
     * @return
     */
    @GetMapping("/realtime-hour")
    public String realtimeHour(String id, String date){

        if ("dau".equals(id)){
            // 日活
            Map<String, Long> today = publisherService.getHourDau(date);
            System.out.println(today);
            Map<String, Long> yesterday = publisherService.getHourDau(getYesterday(date));

            HashMap<String, Map<String, Long>> result = new HashMap<>();
            result.put("today", today);
            result.put("yesterday", yesterday);
            return JSON.toJSONString(result);

        }else if ("order_amount".equals(id)){
            // 订单累计金额
            Map<String, Double> today = publisherService.getHourAmount(date);
            Map<String, Double> yesterday = publisherService.getHourAmount(getYesterday(date));

            HashMap<String, Map<String, Double>> result = new HashMap<>();
            result.put("today", today);
            result.put("yesterday", yesterday);
            return JSON.toJSONString(result);
        }else{

        }

        return "ok";
    }

    // 获取当前日期的前一天
    private String getYesterday(String date) {
        return LocalDate.parse(date).minusDays(1).toString();
    }

    // 灵活查询
    // 接口: http://localhost:8084/sale_detail?date=2020-08-25&&startpage=1&&size=5&&keyword=手机小米
    @GetMapping("/sale_detail")
    public String sale_detail(@RequestParam String date,@RequestParam int startpage,@RequestParam int size,@RequestParam String keyword) throws IOException {
        // 1.从service层获取sale_detail数据
        Map<String, Object> saleDetailAndAgg = publisherService.getSaleDetailAndAgg(date, keyword, startpage, size);

        // 2.封装最终的结果
        SaleInfo result = new SaleInfo();
        // 2.1 设置总数
        Long total = (Long)saleDetailAndAgg.get("total");
        result.setTotal(total);

        // 2.2 设置详情
        List<Map> saleDetail = (List<Map>)saleDetailAndAgg.get("details");
        result.setDetail(saleDetail);

        // 2.3 设置性别饼图
        Map<String, Long> genderAgg = (Map<String, Long>)saleDetailAndAgg.get("genderAgg");
        Stat genderStat = new Stat();
        genderStat.setTitle("用户性别占比");
        Set<Map.Entry<String, Long>> entries = genderAgg.entrySet();
        for (Map.Entry<String, Long> entry : entries) {
            String key = entry.getKey();
            Long value = entry.getValue();
            Option option = new Option();
            option.setName(key.equals("M")?"男":"女");
            option.setValue(value);
            genderStat.addOption(option);
        }
        result.addStat(genderStat);

        // 2.4 设置年龄饼图
        Map<String, Long> ageAgg = (HashMap<String, Long>) saleDetailAndAgg.get("ageAgg");
        Stat ageStat = new Stat();
        ageStat.setTitle("用户年龄占比");
        ageStat.addOption(new Option("20岁以下", 0L));
        ageStat.addOption(new Option("20岁到30岁", 0L));
        ageStat.addOption(new Option("30岁及以上", 0L));

        for (Map.Entry<String, Long> entry : ageAgg.entrySet()) {
            int age = Integer.parseInt(entry.getKey());
            Long value = entry.getValue();
            if(age < 20){
                Option opt = ageStat.getOptions().get(0);
                opt.setValue(value + opt.getValue());
            }else if(age < 30){
                Option opt = ageStat.getOptions().get(1);
                opt.setValue(value + opt.getValue());
            }else{
                Option opt = ageStat.getOptions().get(2);
                opt.setValue(value + opt.getValue());
            }
        }
        result.addStat(ageStat);
        // 3.返回最终结果
        return JSON.toJSONString(result);
    }
}
