package com.asher.realtime.gmallpublisher.service;

import com.google.inject.internal.util.$Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @Author Asher Wu
 * @Date 2020/8/18 20:30
 * @Version 1.0
 */
public interface PublisherService {
    // 获取日活
    Long getDau(String date);

    // 获取每小时日活明细
    Map<String, Long> getHourDau(String date);

    // 获取每日累计订单总金额
    public Double getTotalAmount(String date);

    // 获取每日，每小时的订单总额
    Map<String,Double> getHourAmount(String date);

    // 从es（/gmall_sale_detail/_search）中获取销售详情和聚合结果
    Map<String,Object> getSaleDetailAndAgg(String date,String keyword,int startpage,int size) throws IOException;
}
