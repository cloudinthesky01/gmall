package com.project.utils

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

object ESUtil {
  var factory: JestClientFactory = null;
  val HOST: String = "http://hadoop102"
  val HTTP_PORT: String = "9200"

  def getClient: JestClient = {
    if (factory == null) {
      build()
    }
    factory.getObject
  }

  def close(jestClient: JestClient): Unit = {
    if (jestClient == null) {
      jestClient.close()
    }
  }

  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(HOST + ":" + HTTP_PORT)
      .multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build)
  }

  def insertBulk(indexName: String, doList: List[(String, AnyRef)]): Unit = {
    if (doList.size>0) {
      val client: JestClient = getClient
      val bulkBuilder = new Bulk.Builder
      bulkBuilder.defaultIndex(indexName).defaultType("_doc")
      for ((id, obj) <- doList) {
        val indexBuilder = new Index.Builder(obj)
        if (id != null) {
          indexBuilder.id(id)
        }
        val index: Index = indexBuilder.build()
        bulkBuilder.addAction(index)
      }
      val bulk: Bulk = bulkBuilder.build()
      var items: util.List[BulkResult#BulkResultItem] = null
      try {
        items = client.execute(bulk).getItems
      } catch {
        case e: Exception => println(e.toString)
      } finally {
        close(client)
      }
      if (items.size() > 0) {
        println("保存了" + items.size() + "条数据")
      }
    }
  }

}
