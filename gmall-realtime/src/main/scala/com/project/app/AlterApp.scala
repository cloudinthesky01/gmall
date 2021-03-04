package com.project.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.project.bean.{CouponAlertInfo, EventLog}
import com.project.constants.GmallConstants
import com.project.utils.{ESUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object AlterApp {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("Alter").setMaster("local[*]")
    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //3.消费kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)
    //4.将数据转化成样例类(EventLog文档中有)，补充时间字段，将数据转换为（k，v） k->?  v->?
    val format = new SimpleDateFormat("yyyy-MM-dd HH")
    val eventLogDStream: DStream[EventLog] = kafkaDStream.mapPartitions(
      itr => {
        itr.map(
          consumerRecord => {
            val str: String = consumerRecord.value()
            val eventLog: EventLog = JSON.parseObject(str, classOf[EventLog])
            val time: String = format.format(eventLog.ts)
            eventLog.logDate = time.split(" ")(0);
            eventLog.logHour = time.split(" ")(0)
            eventLog
          })
      }
    )
    //5.开窗5min
    val windowDStream: DStream[EventLog] = eventLogDStream.window(Seconds(300), Seconds(5))
    //6.按照mid分组聚合，即对mid去重
    val midToEventLogItrDStream: DStream[(String, Iterable[EventLog])] = windowDStream.mapPartitions(
      _.map(
        eventLog => (eventLog.mid, eventLog)
      )
    ).groupByKey()
    //7.筛选数据，首先用户得领优惠券，并且用户没有浏览商品行为（将符合这些行为的uid保存下来至set集合）
    val booleanToCouponAlertInfoDStream: DStream[(Boolean, CouponAlertInfo)] = midToEventLogItrDStream.map {
      case (mid, eventLogItr) => {
        val uids = new java.util.HashSet[String]()
        val itemIds = new java.util.HashSet[String]()
        val events = new java.util.ArrayList[String]()
        var notClick = true;
        import scala.util.control.Breaks._
        breakable {
          for (eventLog <- eventLogItr) {
            events.add(eventLog.evid)
            if ("coupon".equals(eventLog.evid)) {
              uids.add(eventLog.uid)
              itemIds.add(eventLog.itemid)
            } else if ("clickItem".equals(eventLog.evid)) {
              notClick = false
              break()
            }
          }
        }
        //8.生成预警日志(将数据保存至CouponAlertInfo样例类中，文档中有)，条件：符合第七步要求，并且uid个数>=3（主要为“过滤”出这些数据），实质：补全CouponAlertInfo样例类
        (uids.size >= 3 && notClick, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
      }
    }
    val idToCouponAlterInfoDStream: DStream[(String, CouponAlertInfo)] = booleanToCouponAlertInfoDStream.filter(_._1).map {
      case (flag, alertInfo) => {
        val minutePeriod: Long = alertInfo.ts / 1000L / 60L
        val id: String = alertInfo.mid + "_" + minutePeriod
        (id, alertInfo)
      }
    }
    val currentDayFormat = new SimpleDateFormat("yyyy-MM-dd")
    idToCouponAlterInfoDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          itr => {
            val currentDay: String = currentDayFormat.format(new Date)
            ESUtil.insertBulk(GmallConstants.ES_INDEX_ALERT + "_" + currentDay,itr.toList)
          }
        )
      }
    )

    ssc.start()
    ssc.awaitTermination()









    //9.将预警数据写入ES

    //10.开启


  }

}
