package com.atguigu.flink_first_look.utils

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object MyKafkaUtil {

  private val pro = new Properties()

  pro.setProperty("bootstrap.servers","192.168.30.131:9092")
  pro.setProperty("group.id","test111")

  def getConsumer(topic:String) ={

    val kafkaSource = new FlinkKafkaConsumer011[String](topic,new SimpleStringSchema(),pro)

    kafkaSource


  }

}