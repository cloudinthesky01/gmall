package com.project.app

import com.alibaba.fastjson.JSON
import com.project.bean.UserInfo
import com.project.constants.GmallConstants
import com.project.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * 从kafka消费UserInfo，存入redis，这个功能单独拿出来，是orderInfo和orderDetail在双流join处理完成后，
 * 需要和userInfo做关联，此时，所有的userInfo数据都应该已经存入了redis，避免noUserDStream在redis中查不到userInfo数据
 */
object UserInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val userDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER, ssc)
    val infoJsonDStream: DStream[String] = userDStream.mapPartitions(
      iter => {
        iter.map(_.value()
        )
      }
    )
    infoJsonDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter => {
            val client: Jedis = RedisUtil.getJedisClient
            iter.foreach(
              //写入redis
              string => {
                val userInfo: UserInfo = JSON.parseObject(string,classOf[UserInfo])
                val userInfoKey = s"userInfo:${userInfo.id}"
                client.set(userInfoKey,string)
              }
            )
            client.close()
          }
        )
      }
    )
    infoJsonDStream.print(100)
    ssc.start()
    ssc.awaitTermination()
  }
}
