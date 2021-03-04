package com.project.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.project.bean.StartUpLog
import com.project.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DAUHandler {
  /**
   * 批次内去重
   * @param fileterByRedisDStream
   * @return
   */
  def filterByMid(fileterByRedisDStream: DStream[StartUpLog]) = {
    val midAndLogDateToLog: DStream[((String, String), StartUpLog)] = fileterByRedisDStream.map(
      log => {
        ((log.mid, log.logDate), log)
      }
    )
    val midAndLogDateToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = midAndLogDateToLog.groupByKey()
    val midAndLogDateToLogList: DStream[((String, String), List[StartUpLog])] = midAndLogDateToLogIterDStream.mapValues(
      iter => {
        iter.toList.sortWith(_.ts < _.ts).take(1)
      }
    )
    val result: DStream[StartUpLog] = midAndLogDateToLogList.flatMap(_._2)
    result
  }

  /**
   * 跨批次去重
   *
   * @param startUpLogDStream
   * @param sc
   * @return
   */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog], sc: SparkContext): DStream[StartUpLog] = {
    //方案1
/*    startUpLogDStream.filter(
      log => {
        val client: Jedis = RedisUtil.getJedisClient
        val key: String = "DAU" + log.logDate
        val boolean: Boolean = client.sismember(key, log.mid)
        client.close()
        !boolean
      }
    )*/
    //方案2 分区下获取连接，减少连接个数
/*    startUpLogDStream.mapPartitions(
      itr => {
        val client: Jedis = RedisUtil.getJedisClient
        val logs: Iterator[StartUpLog] = itr.filter(
          log => {
            val key: String = "DAU" + log.logDate
            val boolean: Boolean = client.sismember(key, log.mid)
            !boolean
          }
        )
        client.close()
        logs
      }
    )*/
    //方案3 每个批次获取一次连接
val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    startUpLogDStream.transform(rdd=>{
      //1.获取连接
      val jedisClient: Jedis = RedisUtil.getJedisClient

      //2.获取数据
      val redisKey="DAU:" + sdf.format(new Date(System.currentTimeMillis()))
      val midSet: util.Set[String] = jedisClient.smembers(redisKey)
      println(Thread.currentThread().getName+"----"+midSet)
      //3.归还连接
      jedisClient.close()

      //4.广播数据
      val midBC: Broadcast[util.Set[String]] = sc.broadcast(midSet)

      //5.过滤数据
      val result: RDD[StartUpLog] = rdd.filter(log => {
        !midBC.value.contains(log.mid)

//        !midSet.contains(log.mid)
      })
      result
    })

  }

  /**
   * 将DStream中的StartUpLog的mid存入redis
   * @param startUpLogDStream
   */
  def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          itr => {
            val client: Jedis = RedisUtil.getJedisClient
            itr.foreach(
              log => {
                val key: String = "DAU:"+log.logDate
                client.sadd(key,log.mid)
              }
            )
            client.close()
          }
        )
      }
    )
  }
}
