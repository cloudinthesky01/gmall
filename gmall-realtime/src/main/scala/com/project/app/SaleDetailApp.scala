package com.project.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.project.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.project.constants.GmallConstants
import com.project.utils.{ESUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

import collection.JavaConverters._


object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val orderDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil
      .getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)
    val orderDetailDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil
      .getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)
    val orderIdToOrderDetailDStream: DStream[(String, OrderDetail)] = orderDetailDStream.mapPartitions(
      iter => {
        iter.map(
          record => {
            val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
            (orderDetail.order_id, orderDetail)
          }
        )
      }
    )
    val orderIdToOrderInfoDStream: DStream[(String, OrderInfo)] = orderDStream.mapPartitions(
      iter => {
        iter.map(
          record => {
            val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
            orderInfo.create_date = orderInfo.create_time.split(" ")(0)
            orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
            (orderInfo.id, orderInfo)
          }
        )
      }
    )
    // 双流full join
    val fullJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] =
      orderIdToOrderInfoDStream.fullOuterJoin(orderIdToOrderDetailDStream)

    val noUserDStream: DStream[SaleDetail] = fullJoinDStream.mapPartitions(
      iter => {
        implicit val formats = org.json4s.DefaultFormats
        val details = new util.ArrayList[SaleDetail]
        //创建redis链接
        val client: Jedis = RedisUtil.getJedisClient
        iter.foreach {
          case (orderId, (orderInfoOpt, orderDetailOpt)) => {
            //redis中的key
            val orderInfoKey = s"orderInfo:${orderId}"
            val orderDetailKey = s"orderDetail:${orderId}"
            //fulljoin中orderInfo不为空
            if (orderInfoOpt.isDefined) {
              val orderInfo: OrderInfo = orderInfoOpt.get
              //a. fulljoin中orderDetail不为空
              if (orderDetailOpt.isDefined) {
                details.add(new SaleDetail(orderInfo, orderDetailOpt.get))
              }
              //b. 将orderInfo写入reids，做备份给orderDetail来join
              val orderInfoJSONString: String = Serialization.write(orderInfo)
              client.set(orderInfoKey, orderInfoJSONString)
              //设置过期时间
              client.expire(orderInfoKey, 100)

              //c. 去redis查询orderDetail缓存中是否有orderId相同的数据
              if (client.exists(orderDetailKey)) {
                val orderDetailSet: util.Set[String] = client.smembers(orderDetailKey)
                orderDetailSet.asScala.foreach(
                  string => {
                    details.add(new SaleDetail(orderInfo, JSON.parseObject(string, classOf[OrderDetail])))
                  }
                )
              }
            } else { //fulljoin中orderInfo为空
              // fulljoin中orderDetail不为空，去redis查orderInfo的缓存
              if (orderDetailOpt.isDefined) {
                if (client.exists(orderInfoKey)) {
                  val str: String = client.get(orderInfoKey)
                  details.add(new SaleDetail(JSON.parseObject(str, classOf[OrderInfo]), orderDetailOpt.get))
                }
              } else { //fulljoin中orderDetail为空,将orderDetail存入redis
                val orderDetailJson: String = Serialization.write(orderDetailOpt.get)
                client.sadd(orderDetailKey, orderDetailJson)
                client.expire(orderDetailKey, 100)
              }
            }
          }
        }
        client.close()
        //        details.iterator()返回的是java的Iterator，不行，必须返回scala的Iterator对象
        details.asScala.toIterator
      }
    )
    //补全用户信息
    val saleDetailDStream: DStream[SaleDetail] = noUserDStream.mapPartitions(
      iter => {
        val client: Jedis = RedisUtil.getJedisClient
        val details: Iterator[SaleDetail] = iter.map(
          saleDetail => {
            val userInfoKey = "userInfo:" + saleDetail.user_id
            val userInfoJson: String = client.get(userInfoKey)
            println(Thread.currentThread().getName + userInfoJson)
            println("=================")
            saleDetail.mergeUserInfo(JSON.parseObject(userInfoJson, classOf[UserInfo]))
            println(saleDetail)
            saleDetail
          }
        )
        client.close()
        details
      }
    )
    saleDetailDStream.print(100)
    val format = new SimpleDateFormat("yyyy-MM-dd")
    saleDetailDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter => {
            val indexName: String = GmallConstants.ES_DETAIL_INDEX + "-" + format.format(new Date())
            val tuples: Iterator[(String, SaleDetail)] = iter.map(
              saleDetail => (saleDetail.order_detail_id, saleDetail)
            )
            ESUtil.insertBulk(indexName, tuples.toList)
          }
        )
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
