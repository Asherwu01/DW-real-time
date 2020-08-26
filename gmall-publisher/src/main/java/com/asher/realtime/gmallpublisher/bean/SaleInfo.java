package com.asher.realtime.gmallpublisher.bean;

import  java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SaleInfo {
    private Long total;
    private List<Stat> stat = new ArrayList<>();
    private List<Map> detail;

    public void addStat(Stat stat){
        this.stat.add(stat);
    }

    public Long getTotal() {
        return total;
    }

    public void setTotal(Long total) {
        this.total = total;
    }

    public List<Stat> getStat() {
        return stat;
    }

    public List<Map> getDetail() {
        return detail;
    }

    public void setDetail(List<Map> detail) {
        this.detail = detail;
    }
}
