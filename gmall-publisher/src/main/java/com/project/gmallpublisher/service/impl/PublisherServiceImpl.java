package com.project.gmallpublisher.service.impl;

import com.project.gmallpublisher.mapper.DAUMapper;
import com.project.gmallpublisher.mapper.OrderMapper;
import com.project.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jw
 * @description
 * @date 2021/2/22 14:18
 **/
@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    DAUMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Override
    public Integer getDAUTotal(String date) {
        return dauMapper.selectDAUTotal(date);
    }

    @Override
    public Map<String, Long> getDAUTotalHourMap(String date) {
        List<Map> maps = dauMapper.selectDAUTotalHourMap(date);
        HashMap<String, Long> result = new HashMap<>();
        for (Map map : maps) {
            result.put((String)map.get("LH"),(Long)map.get("CT"));
        }
        return result;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        List<Map> maps = orderMapper.selectOrderAmountHourMap(date);
        HashMap map = new HashMap<>();
        for (Map map1 : maps) {
            map.put(map1.get("CREATE_HOUR"), map1.get("SUM_AMOUNT"));
        }
        return map;
    }
}
