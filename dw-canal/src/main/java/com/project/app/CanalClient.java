package com.project.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.project.constants.GmallConstants;
import com.project.utils.MyKafkaSender;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author jw
 * @description
 * @date 2021/2/22 20:09
 **/
public class CanalClient {
    public static void main(String[] args) {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111),
                "example", "", "");
        while (true) {
            canalConnector.connect();
            canalConnector.subscribe("gmall0923.*");
            Message message = canalConnector.get(100);
            if (message.getEntries().size() <= 0) {
                System.out.println("没有数据，休息一下");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                for (CanalEntry.Entry entry : message.getEntries()) {
                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {
                        String tableName = entry.getHeader().getTableName();
                        ByteString storeValue = entry.getStoreValue();
                        CanalEntry.RowChange rowChange = null;
                        try {
                            rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        handler(tableName, rowDatasList, eventType);
                    }
                }
            }
        }
    }

    private static void handler(String tableName, List<CanalEntry.RowData> rowDatasList, CanalEntry.EventType eventType) {
        if (tableName.equals("order_info") && CanalEntry.EventType.INSERT.equals(eventType)) {
            sendToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER);
        }
        if (tableName.equals("order_detail") && CanalEntry.EventType.INSERT.equals(eventType)) {
            sendToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);
        }
        if (tableName.equals("user_info") && (CanalEntry.EventType.INSERT.equals(eventType)
                || CanalEntry.EventType.UPDATE.equals(eventType))) {
            sendToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_USER);
        }
    }

    private static void sendToKafka(List<CanalEntry.RowData> rowDatasList, String kafkaTopic) {
        for (CanalEntry.RowData rowData : rowDatasList) {
            JSONObject jsonObject = new JSONObject();
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            for (CanalEntry.Column column : afterColumnsList) {
                jsonObject.put(column.getName(), column.getValue());
            }
            System.out.println(jsonObject.toString());
            MyKafkaSender.send(kafkaTopic, jsonObject.toJSONString());
        }
    }


}
