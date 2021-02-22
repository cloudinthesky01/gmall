package com.project.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author jw
 * @description
 * @date 2021/2/22 21:25
 **/
public interface OrderMapper {

    //1 查询当日交易额总数
    public Double selectOrderAmountTotal(String date);

    //2 查询当日交易额分时明细
    public List<Map> selectOrderAmountHourMap(String date);

}
