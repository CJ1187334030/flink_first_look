package com.atguigu.flink_first_look.utils

import java.util

import com.atguigu.flink_first_look.bean.{StartUpLog}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.json4s.DefaultFormats

object MyESUtil {


  private val hosts: util.ArrayList[HttpHost] = new util.ArrayList[HttpHost]

  hosts.add(new HttpHost("192.168.30.141",9200,"http"))


  def MyEsSink(indexName:String) ={

    val sinkbulid = new ElasticsearchSink.Builder[StartUpLog](hosts,new MyElasticsearchSinkFunction(indexName))

    //刷新前缓冲的最大动作量
    sinkbulid.setBulkFlushMaxActions(10)

    val esSink: ElasticsearchSink[StartUpLog] = sinkbulid.build()

    esSink


  }


  class MyElasticsearchSinkFunction(indexName:String) extends ElasticsearchSinkFunction[StartUpLog] {

    override def process(t: StartUpLog, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {

      implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

      val str: String = org.json4s.native.Serialization.write(t)

      val indexRequest: IndexRequest = Requests.indexRequest.index(indexName).`type`("_doc").source(str)

      requestIndexer.add(indexRequest)

    }

  }


}
