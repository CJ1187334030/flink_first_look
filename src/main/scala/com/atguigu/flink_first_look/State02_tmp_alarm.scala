package com.atguigu.flink_first_look

import com.atguigu.flink_first_look.bean.SensorRead
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object State02_tmp_alarm {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val ds: DataStream[SensorRead] = env.socketTextStream("192.168.30.131",7777)
      .filter(_.length>20)
      .map {
        log =>
          val strings: Array[String] = log.split(",")
          SensorRead(strings(0), strings(1).toLong, strings(2).toDouble)
      }


    ds.keyBy(_.num)
      .flatMap(new MyRichFlatMapFunction(10))
      .print()

    env.execute()


  }


  class MyRichFlatMapFunction(diff:Double) extends RichFlatMapFunction[SensorRead,(String,Double,Double)] {

    lazy private val valueState:ValueState[Double] = getRuntimeContext.getState[Double](new ValueStateDescriptor[Double]("valueState",classOf[Double]))

    override def flatMap(in: SensorRead, collector: Collector[(String, Double, Double)]) = {

      var lastTmp: Double = valueState.value()

      val nowTmp: Double = in.tmp

      if ((lastTmp - nowTmp).abs > diff)
        collector.collect(in.num,lastTmp,in.tmp)

      //lastTmp = in.tmp 错误
      valueState.update(nowTmp)

    }

  }

}

