package com.atguigu.flink_first_look

import com.alibaba.fastjson.JSON
import com.atguigu.flink_first_look.bean.{StartUpLog}
import com.atguigu.flink_first_look.utils.{MyESUtil, MyKafkaUtil}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink

object DateStreamAPIEs {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaSouce: FlinkKafkaConsumer011[String] = MyKafkaUtil.getConsumer("GMALL_STARTUP")

    val DsStartLog: DataStream[StartUpLog] = env.addSource(kafkaSouce).map{ str => JSON.parseObject(str,classOf[StartUpLog])}

    DsStartLog.print()

    val esSink: ElasticsearchSink[StartUpLog] = MyESUtil.MyEsSink("gmall1205_dau")

    DsStartLog.addSink(esSink)

    env.execute()

  }

}
