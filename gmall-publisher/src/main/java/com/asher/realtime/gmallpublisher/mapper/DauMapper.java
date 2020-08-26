package com.asher.realtime.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @Author Asher Wu
 * @Date 2020/8/18 20:19
 * @Version 1.0
 */
public interface DauMapper {
    /**
     * 获取日活
     * @return
     */
    Long getDau(String date);

    /**
     * 获取每小时日活明细
     * +----------+--------+
     * | LOGHOUR  | COUNT  |
     * +----------+--------+
     * | "14"       | 25     |
     * | "15"       | 53     |
     * +----------+--------+
     * @param date
     * @return
     */
    List<Map<String, Object>> getHourDau(String date);
}
