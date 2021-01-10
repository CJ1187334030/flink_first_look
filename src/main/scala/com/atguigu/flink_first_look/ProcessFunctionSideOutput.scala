package com.atguigu.flink_first_look

import com.atguigu.flink_first_look.bean.SensorRead
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessFunctionSideOutput {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    //new MemoryStateBackend()   new FsStateBackend()   RocksDBStateBackend  引包
    env.setStateBackend(new FsStateBackend("hgfh"))

    val ds: DataStream[SensorRead] = env.socketTextStream("192.168.30.131",7777)
      .filter(_.length>20)
      .map {
        log =>
          val strings: Array[String] = log.split(",")
          SensorRead(strings(0), strings(1).toLong, strings(2).toDouble)
      }

    val dspress: DataStream[SensorRead] = ds.process(new MyProcessFunction(30))

    dspress.print("main")
    dspress.getSideOutput(new OutputTag[(String,Long,Double)]("side")).print("side")

    env.execute()

  }

  class MyProcessFunction(threshold:Double) extends ProcessFunction[SensorRead,SensorRead] {

    override def processElement(i: SensorRead, context: ProcessFunction[SensorRead, SensorRead]#Context, collector: Collector[SensorRead]): Unit = {

      if (i.tmp>30)
        collector.collect(i)
      else
        context.output(new OutputTag[(String,Long,Double)]("side"),(i.num,i.timestamp,i.tmp))

    }
  }

}
