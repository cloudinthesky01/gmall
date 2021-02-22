package com.project.gmallpublisher.service;

import org.springframework.stereotype.Service;

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

    public Double getOrderAmount(String date);

    public Map getOrderAmountHour(String date);

}
