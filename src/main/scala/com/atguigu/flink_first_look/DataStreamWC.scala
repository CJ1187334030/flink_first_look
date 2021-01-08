package com.atguigu.flink_first_look

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import org.apache.flink.api.scala._

object DataStreamWC {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    val dstream: DataStream[String] = env.socketTextStream("192.168.30.131",7777)

    val value: DataStream[(String, Int)] = dstream.flatMap(_.split(" ")).filter(_.nonEmpty).map((_,1)).keyBy(0).sum(1)

    value.print()

    env.execute()

  }

}
