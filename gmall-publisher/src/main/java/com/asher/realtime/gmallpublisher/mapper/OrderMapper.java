package com.asher.realtime.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * 订单
 * @Author Asher Wu
 * @Date 2020/8/19 20:52
 * @Version 1.0
 */
public interface OrderMapper {

    // 获取每日销售总额
    public Double getTotalAmount(String date);

    // 获取每日，每小时的销售总额
    List<Map<String, Object>> getHourAmount(String date);
}
