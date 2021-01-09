package com.atguigu.flink_first_look


import com.alibaba.fastjson.JSON
import com.atguigu.flink_first_look.bean.Person
import com.atguigu.flink_first_look.utils.{MyESUtil, MyJdbcSink, MyKafkaUtil}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink


object DateStreamAPIEs {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaSouce: FlinkKafkaConsumer011[String] = MyKafkaUtil.getConsumer("GMALL_STARTUP")

    val DsStartLog: DataStream[Person] = env.addSource(kafkaSouce).filter(_.nonEmpty).filter(_.startsWith("{")).filter(_.endsWith("}"))
      .map { str => JSON.parseObject(str, classOf[Person]) }

    val ds1: DataStream[String] = env.addSource(kafkaSouce)

    DsStartLog.print()

    //ES测试  程序异常终止  才插入
//    val esSink: ElasticsearchSink[Person] = MyESUtil.MyEsSink()

    //jdbc mysql 测试


    DsStartLog.addSink(new MyJdbcSink())

    env.execute()

  }

}
