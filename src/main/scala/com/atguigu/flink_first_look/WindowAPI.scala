package com.atguigu.flink_first_look

import com.atguigu.flink_first_look.bean.SensorRead
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object WindowAPI {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val ds: DataStream[SensorRead] = env.readTextFile("D:\\study\\IDEA\\flink_first_look\\src\\main\\resources\\sensor.txt")
      .map {
        log =>
          val strings: Array[String] = log.split(",")
          SensorRead(strings(0), strings(1).toLong, strings(2).toDouble)
      }
//      .assignAscendingTimestamps(_.timestamp * 1000)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorRead](Time.seconds(3)) {
      override def extractTimestamp(t: SensorRead): Long = t.timestamp * 1000
    })


    //每15s统计各个sensor的最低温度，以及最新时间戳
    val dsRes: DataStream[(String, Double, Long)] = ds.map(log => (log.num, log.tmp, log.timestamp))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15))
      //curRes, newDate  =》  当前结果 ，新数据
      .reduce((curRes, newDate) => (curRes._1, curRes._2.min(newDate._2), newDate._3))


    //方法二：
    val dsRes1: DataStream[SensorRead] = ds.keyBy(_.num)
      .timeWindow(Time.seconds(15))
      .reduce(new MyReduceFunction)

    val lateTag = new OutputTag[(String, Double, Long)]("late")
    val dsRes2: DataStream[(String, Double, Long)] = ds.map(log => (log.num, log.tmp, log.timestamp))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(lateTag)
      //curRes, newDate  =》  当前结果 ，新数据
      .reduce((curRes, newDate) => (curRes._1, curRes._2.min(newDate._2), newDate._3))

    dsRes.print()

    //打印延时输出流
    dsRes2.getSideOutput(lateTag)


    env.execute()

  }



  class MyReduceFunction extends ReduceFunction[SensorRead]{
    override def reduce(t: SensorRead, t1: SensorRead): SensorRead = {

      SensorRead(t.num,t1.timestamp,t.tmp.min(t1.tmp))
    }
  }



}
