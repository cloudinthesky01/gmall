package com.project.app

import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.project.bean.StartUpLog
import com.project.constants.GmallConstants
import com.project.handler.DAUHandler
import com.project.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object DAUApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(
      iter => {
        iter.map(consumerRecord => {
          val value: String = consumerRecord.value()
          val startUpLog: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])
          val ts: Long = startUpLog.ts
          val time: String = simpleDateFormat.format(ts)
          val strings: Array[String] = time.split(" ")
          startUpLog.logDate = strings(0)
          startUpLog.logHour = strings(1)
          startUpLog
        })
      }
    )
    val filterByRedisDStream: DStream[StartUpLog] = DAUHandler.filterByRedis(startUpLogDStream, ssc.sparkContext)
    startUpLogDStream.cache()
    filterByRedisDStream.cache()
        startUpLogDStream.count().print()
    filterByRedisDStream.count().print()
    DAUHandler.saveMidToRedis(filterByRedisDStream)
    //    startUpLogDStream.print()
    val filterByMidDStream: DStream[StartUpLog] = DAUHandler.filterByMid(filterByRedisDStream)
    filterByMidDStream.cache()
//    filterByMidDStream.count().print()
    //写入hbase
    filterByMidDStream.foreachRDD(
      rdd => {
        rdd.saveToPhoenix(
          "GMALL0923_DAU",
          Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
          HBaseConfiguration.create(),
          Some("hadoop102,hadoop103,hadoop104:2181")
        )
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }

}
