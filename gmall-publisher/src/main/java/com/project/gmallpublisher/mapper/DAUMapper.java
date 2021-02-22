package com.project.gmallpublisher.mapper;

import org.apache.catalina.mapper.Mapper;

import java.util.List;
import java.util.Map;

/**
 * @author jw
 * @description
 * @date 2021/2/22 14:10
 **/
public interface DAUMapper {
    //日活数据
    public Integer selectDAUTotal(String date);

    //分时数据
    public List<Map> selectDAUTotalHourMap(String date);
}
