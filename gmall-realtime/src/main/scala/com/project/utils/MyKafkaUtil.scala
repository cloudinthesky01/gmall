package com.project.utils

import java.util.Properties

import org.apache.kafka.common.serialization.StringDeserializer

object MyKafkaUtil {
  private val properties: Properties = PropertiesUtil.load("config.properties")

  val brokerList: String = properties.getProperty("kafka.broker.list")

  val kafkaParam = Map(
    "bootstrap.servers" -> brokerList,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "bigData",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

}
