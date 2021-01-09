package com.atguigu.flink_first_look.utils

import java.util

import com.atguigu.flink_first_look.bean.{Person}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object MyESUtil {


  private val hosts: util.ArrayList[HttpHost] = new util.ArrayList[HttpHost]

  hosts.add(new HttpHost("192.168.30.141",9200))


  def MyEsSink() ={

    val sinkbulid = new ElasticsearchSink.Builder[Person](hosts,new MyElasticsearchSinkFunction)

    //刷新前缓冲的最大动作量  条
    sinkbulid.setBulkFlushMaxActions(10)

    val esSink: ElasticsearchSink[Person] = sinkbulid.build()

    esSink

  }


  class MyElasticsearchSinkFunction() extends ElasticsearchSinkFunction[Person] {

    override def process(t: Person, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {

      println("saving data: " + t)

      val json = new util.HashMap[String, String]()
      json.put("name", t.name)
      json.put("age", t.age.toString)
      json.put("sex", t.sex)

      println(json.toString)

      val indexRequest = Requests.indexRequest().index("person").`type`("_doc").source(json)

      //发送
      requestIndexer.add(indexRequest)

      println("saved successfully")

    }
  }


}
