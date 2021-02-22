package com.project.app

import com.alibaba.fastjson.JSON
import com.project.bean.OrderInfo
import com.project.constants.GmallConstants
import com.project.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object OrderApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("GMVApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants
      .KAFKA_TOPIC_ORDER, ssc)
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.mapPartitions(
      iter => {
        iter.map(record => {
          val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
          val create_time: String = orderInfo.create_time
          orderInfo.create_date = create_time.split(" ")(0)
          orderInfo.create_hour = create_time.split(" ")(1).split(":")(0)
          //手机号脱敏
          orderInfo.consignee_tel = orderInfo.consignee_tel.substring(0, 4) + "*********"
          orderInfo
        })
      }
    )
    import org.apache.phoenix.spark._
    orderInfoDStream.foreachRDD(
      rdd => {
        rdd.saveToPhoenix(
          "GMALL0923_ORDER_INFO",
          Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL",
            "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT",
            "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO",
            "PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
          HBaseConfiguration.create(),
          Some("hadoop102,hadoop103,hadoop104:2181"))
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
