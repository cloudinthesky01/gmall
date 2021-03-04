package com.project.gmallpublisher.service.impl;

import com.alibaba.fastjson.JSON;
import com.project.gmallpublisher.bean.Option;
import com.project.gmallpublisher.bean.Stat;
import com.project.gmallpublisher.mapper.DAUMapper;
import com.project.gmallpublisher.mapper.OrderMapper;
import com.project.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import lombok.experimental.var;
import lombok.val;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
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

    @Autowired
    JestClient client;

    @Override
    public Integer getDAUTotal(String date) {
        return dauMapper.selectDAUTotal(date);
    }

    @Override
    public Map<String, Long> getDAUTotalHourMap(String date) {
        List<Map> maps = dauMapper.selectDAUTotalHourMap(date);
        HashMap<String, Long> result = new HashMap<>();
        for (Map map : maps) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
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

    @Override
    public String getSaleDetail(String date, int startpage, int size, String keyword) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        TermQueryBuilder dt = new TermQueryBuilder("dt", date);
        boolQueryBuilder.filter(dt);
        MatchQueryBuilder sku_nameMatchQueryBuilder = new MatchQueryBuilder("sku_name", keyword)
                .operator(MatchQueryBuilder.Operator.AND);
        boolQueryBuilder.must(sku_nameMatchQueryBuilder);
        searchSourceBuilder.query(boolQueryBuilder);

        TermsBuilder groupByUserGenderAggs = AggregationBuilders.terms("groupby_user_gender").field("user_gender");
        TermsBuilder groupByUserAgeAggs = AggregationBuilders.terms("groupby_user_age").field("user_age");
        searchSourceBuilder.aggregation(groupByUserAgeAggs);
        searchSourceBuilder.aggregation(groupByUserGenderAggs);

        searchSourceBuilder.from((startpage - 1) * size);
        searchSourceBuilder.size(size);

        //最终要转换为json字符串返回的总的结果
        HashMap<String, Object> result = new HashMap<>();

        //执行es查询
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex("gmall2021_sale_detail-query")
                .addType("_doc").build();
        SearchResult searchResult = client.execute(search);

        //查到的总数
        Long total = searchResult.getTotal();

        result.put("total", total);

        //存放查到的结果的详细信息
        ArrayList<Map> details = new ArrayList<>();
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            details.add(hit.source);
        }
        result.put("detail", details);

        //求性别占比
        MetricAggregation aggregations = searchResult.getAggregations();
        TermsAggregation groupby_user_gender = aggregations.getTermsAggregation("groupby_user_gender");
        List<TermsAggregation.Entry> buckets = groupby_user_gender.getBuckets();
        Long maleCount = 0L;
        for (TermsAggregation.Entry bucket : buckets) {
            if (bucket.getKey().equals("M")) {
                maleCount += bucket.getCount();
            }
        }
        double maleRatio = Math.round((maleCount * 1000D / total)) / 10D;
        double femaleRatio = 100D - maleRatio;
        Option maleOpt = new Option("男", maleRatio);
        Option femaleOpt = new Option("女", femaleRatio);
        ArrayList<Option> genderOptions = new ArrayList<>();
        genderOptions.add(maleOpt);
        genderOptions.add(femaleOpt);
        Stat genderStat = new Stat(genderOptions, "性别分布");

        //求年龄占比
        TermsAggregation groupby_user_age = aggregations.getTermsAggregation("groupby_user_age");
        List<TermsAggregation.Entry> buckets1 = groupby_user_age.getBuckets();
        Long low20Count = 0L;
        Long up30Count = 0L;
        for (TermsAggregation.Entry ageBucket : buckets1) {
            if (Integer.parseInt(ageBucket.getKey()) < 20) {
                low20Count += ageBucket.getCount();
            } else if (Integer.parseInt(ageBucket.getKey()) >= 30) {
                up30Count += ageBucket.getCount();
            }
        }
        //获取小于20岁年龄占比
        double low20Ratio = Math.round(low20Count * 1000D / total) / 10D;
        //获取大于等于30岁年龄占比
        double up30Ratio = Math.round(up30Count * 1000D / total) / 10D;
        //20<=age<30
        double between20And30Ratio =100D - low20Ratio - up30Ratio;
        //创建年龄的Option对象
        Option low20Opt = new Option("20岁以下", low20Ratio);
        Option up20To30Opt = new Option("20岁到30岁", between20And30Ratio);
        Option up30Opt = new Option("30岁及30岁以上", up30Ratio);
        ArrayList<Option> ageOptions = new ArrayList<>();
        ageOptions.add(low20Opt);
        ageOptions.add(up20To30Opt);
        ageOptions.add(up30Opt);
        Stat ageStat = new Stat(ageOptions, "年龄分布");

        ArrayList<Stat> stats = new ArrayList<>();
        stats.add(genderStat);
        stats.add(ageStat);
        result.put("stat", stats);

        return JSON.toJSONString(result);
    }
}
