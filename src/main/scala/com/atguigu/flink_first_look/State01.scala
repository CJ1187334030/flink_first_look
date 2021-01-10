package com.atguigu.flink_first_look

import java.lang

import com.atguigu.flink_first_look.bean.SensorRead
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state._
import org.apache.flink.configuration.Configuration


object State01 {


  def main(args: Array[String]): Unit = {


  }


  class MyRichMapFunction extends RichMapFunction[SensorRead,String] {

    //生命周期中开始
    private var valueState:ValueState[String] = _

    //外部定义 调用开始
    lazy private val listState: ListState[String] = getRuntimeContext.getListState[String](new ListStateDescriptor[String]("listState",classOf[String]))

    private var mapState:MapState[String,String] = _


    override def open(parameters: Configuration): Unit = {
      super.open(parameters)

      getRuntimeContext.getState[String](new ValueStateDescriptor[String]("valueState",classOf[String]))

    }


    override def map(in: SensorRead) = {

      val str: String = valueState.value()

      val strings: lang.Iterable[String] = listState.get()

      str

    }


  }

}


