package com.asher.realtime.gmallpublisher.service;

import com.asher.realtime.gmallpublisher.mapper.DauMapper;
import com.asher.realtime.gmallpublisher.mapper.OrderMapper;
import com.asher.realtime.gmallpublisher.util.ESUtil;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.map.HashedMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author Asher Wu
 * @Date 2020/8/18 20:31
 * @Version 1.0
 */
@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;
    @Autowired
    OrderMapper orderMapper;

    /**
     * 获取日活
     * @param date
     * @return
     */
    @Override
    public Long getDau(String date) {
        return dauMapper.getDau(date);
    }

    /**
     * 获取每小时日活明细
     * @param date
     * @return
     */
    @Override
    public Map<String, Long> getHourDau(String date) {
        List<Map<String, Object>> list = dauMapper.getHourDau(date);

        Map<String, Long> result = new HashedMap<>();
        for (Map<String, Object> map : list) {
            String loghour = map.get("LOGHOUR").toString();
            Long count = (Long)map.get("COUNT");

            result.put(loghour,count);
        }
        return result;
    }

    /**
     * 获取每日累计订单金额
     * @param date
     * @return
     */
    @Override
    public Double getTotalAmount(String date) {
        return orderMapper.getTotalAmount(date) ==null?0:orderMapper.getTotalAmount(date);
    }

    /**
     * 每小时的销售额
     * @param date
     * @return
     */
    @Override
    public Map<String,Double> getHourAmount(String date) {
        List<Map<String, Object>> list = orderMapper.getHourAmount(date);

        Map<String, Double> result = new HashedMap<>();
        for (Map<String, Object> map : list) {
            String create_hour = map.get("CREATE_HOUR").toString();
            Double sum = ((BigDecimal) map.get("SUM")).doubleValue();

            result.put(create_hour, sum);
        }
        return result;
    }

    /**
     * 从es（/gmall_sale_detail/_search）中获取销售详情和聚合结果
     *
     *     总数:
     *     聚合结果:
     *         1. 年龄
     *         2. 性别
     *      详情:
     *         ...
     *      Map(String-> Object)
     *
     * @param date 日期
     * @param keyword  按 "手机小米" 过滤
     * @param startpage 查询的页号
     * @param size 每页的条数
     * @return
     */
    @Override
    public Map<String, Object> getSaleDetailAndAgg(String date, String keyword, int startpage, int size) throws IOException {
        // 1.先去查询es
        // 1.1 获取es客户端
        JestClient client = ESUtil.getClient();
        // 1.2 查询
        Search search = new Search.Builder(ESUtil.getDSL(date, keyword, startpage, size))
                .addIndex("gmall_sale_detail")
                .addType("_doc")
                .build();
        SearchResult searchResult = client.execute(search);

        // 2.从查询结果中，解析出需要的数据，将数据存放在map中
        // result,存放需要返回的数据
        Map<String, Object> result = new HashedMap<>();

        // 2.1放入总数
        Long total = searchResult.getTotal();
        result.put("total",total);
        // 2.2放入详情
        List<SearchResult.Hit<HashMap, Void>> hits = searchResult.getHits(HashMap.class);
        List<Map> details = new ArrayList<>();
        for (SearchResult.Hit<HashMap, Void> hit : hits) {
            HashMap source = hit.source;
            details.add(source);
        }
        result.put("details",details);
        // 2.3性别聚合
        Map<String, Long> genderAgg = new HashMap<>();
        MetricAggregation genderAggregations = searchResult.getAggregations();
        List<TermsAggregation.Entry> genderBuckets = genderAggregations.getTermsAggregation("group_by_user_gender").getBuckets();
        for (TermsAggregation.Entry bucket : genderBuckets) {
            String key = bucket.getKey();
            Long value = bucket.getCount();
            genderAgg.put(key,value);
        }
        result.put("genderAgg",genderAgg);

        // 2.4年龄聚合
        Map<String, Long> ageAgg = new HashMap<>();
        MetricAggregation ageAggregations = searchResult.getAggregations();
        List<TermsAggregation.Entry> ageBuckets = ageAggregations.getTermsAggregation("group_by_user_age").getBuckets();
        for (TermsAggregation.Entry bucket : ageBuckets) {
            String key = bucket.getKey();
            Long value = bucket.getCount();
            ageAgg.put(key,value);
        }
        result.put("ageAgg",ageAgg);

        // 3.返回封装的结果
        return result;
    }
}
