package com.project.gmallpublisher.service;

import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Map;

/**
 * @author jw
 * @description
 * @date 2021/2/22 14:16
 **/

public interface PublisherService {
    //日活
    public Integer getDAUTotal(String date);

    //分时
    public Map<String,Long> getDAUTotalHourMap(String date);

    //GMV总数
    public Double getOrderAmount(String date);

    //GMV分时数据
    public Map getOrderAmountHour(String date);

    //灵活分析
    public String getSaleDetail(String date, int startpage, int size, String keyword) throws IOException;
}
