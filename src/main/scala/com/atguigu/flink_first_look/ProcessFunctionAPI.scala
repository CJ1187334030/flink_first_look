package com.atguigu.flink_first_look

import com.atguigu.flink_first_look.bean.SensorRead
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessFunctionAPI {

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
      .process(new MyKeyedProcessFunction(10000L))
      .print()

    env.execute()

  }


  class MyKeyedProcessFunction(time:Long) extends KeyedProcessFunction[String,SensorRead,String] {

    lazy val lastTempState:ValueState[Double] = getRuntimeContext.getState[Double](new ValueStateDescriptor[Double]("lastTemp",classOf[Double]))
    lazy val timeTsState:ValueState[Long] = getRuntimeContext.getState[Long](new ValueStateDescriptor[Long]("timeTs",classOf[Long]))


    override def processElement(i: SensorRead, context: KeyedProcessFunction[String, SensorRead, String]#Context, collector: Collector[String]): Unit = {

      val lastTemp: Double = lastTempState.value()
      val timeTS: Long = timeTsState.value()

      val curTemp: Double = i.tmp

      lastTempState.update(curTemp)

      if(curTemp>lastTemp && timeTS == 0){
        val ts: Long = context.timerService().currentProcessingTime() + time
        context.timerService().registerProcessingTimeTimer(ts)
        timeTsState.update(ts)
      }
      else if(curTemp < lastTemp){
        context.timerService().deleteProcessingTimeTimer(timeTS)
        timeTsState.clear()
      }

    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorRead, String]#OnTimerContext, out: Collector[String]): Unit = {

      out.collect("定时器："+ctx.getCurrentKey+"的温度连续上升"+time/1000+"s")
      timeTsState.clear()

    }
  }

}
