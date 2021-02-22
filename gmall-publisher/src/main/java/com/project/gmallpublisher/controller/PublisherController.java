package com.project.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.project.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author jw
 * @description
 * @date 2021/2/22 14:25
 **/
@Controller
@ResponseBody
public class PublisherController {
    @Autowired
    PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getDAUTotal(@RequestParam("date")String date){
        Integer dauTotal = publisherService.getDAUTotal(date);
        HashMap<String, Object> dauMap = new HashMap<>();
        HashMap<String, Object> devMap = new HashMap<String, Object>();
        ArrayList<Map<String, Object>> reuslt = new ArrayList<Map<String, Object>>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);
        devMap.put("id","new_mid");
        devMap.put("name","新增设备");
        devMap.put("value","233");


        reuslt.add(dauMap);
        reuslt.add(devMap);

        HashMap<String, Object> gmvMap = new HashMap<>();
        gmvMap.put("id", "order_amount");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", publisherService.getOrderAmount(date));
        reuslt.add(gmvMap);

        String jsonResult = JSON.toJSONString(reuslt);
        return jsonResult;
    }

    @RequestMapping("realtime-hours")
    public String getDAUTotalHourMap(@RequestParam("id")String id,@RequestParam("date")String date){
        HashMap<String, Map> result = new HashMap<>();
        Map todayMap = null;
        Map yesterdayMap = null;

        String yesterday = LocalDate.parse(date).plusDays(-1).toString();
        if (id.equals("dau")){

            todayMap = publisherService.getDAUTotalHourMap(date);

            yesterdayMap = publisherService.getDAUTotalHourMap(yesterday);
        }else if (id.equals("order_amount")){
            todayMap = publisherService.getOrderAmountHour(date);

            yesterdayMap = publisherService.getOrderAmountHour(yesterday);
        }

        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        return JSON.toJSONString(result);
    }
}
