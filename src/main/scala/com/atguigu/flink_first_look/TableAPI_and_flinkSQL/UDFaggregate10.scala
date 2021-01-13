package com.atguigu.flink_first_look.TableAPI_and_flinkSQL

import com.atguigu.flink_first_look.bean.SensorRead
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions._
import org.apache.flink.types.Row

object UDFaggregate10 {


  def main(args: Array[String]): Unit = {


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置时间语义为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val filePath = "D:\\study\\IDEA\\flink_first_look\\src\\main\\resources\\sensor.txt"

    val ds: DataStream[SensorRead] = env.readTextFile(filePath)
      .map {
        log =>
          val strings: Array[String] = log.split(",")
          SensorRead(strings(0), strings(1).toLong, strings(2).toDouble)
      }
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorRead](Time.seconds(1)) {
        override def extractTimestamp(t: SensorRead): Long = t.timestamp * 1000L
      })

    val table: Table = tableEnv.fromDataStream(ds,'num,'timestamp.rowtime as 'ts ,'tmp)

    val aggregateFunction = new MyAggregateFunction

    //table Api
    val table01: Table = table.groupBy('num)
      .aggregate(aggregateFunction('tmp) as 'avgTmp)
      .select('num, 'avgTmp)


    //flink sql
    tableEnv.createTemporaryView("sensor",table)
    tableEnv.registerFunction("aggregateFunction",aggregateFunction)
    val table02 = tableEnv.sqlQuery(
      """
        |select num,aggregateFunction(tmp)
        |from sensor
        |group by num
      """.stripMargin)


    table02.toRetractStream[Row].print()


    env.execute()

  }

}




class TempaggACC{
  var sum:Double = _
  var count:Int = _
}


class MyAggregateFunction extends AggregateFunction[Double,TempaggACC] {

  override def getValue(acc: TempaggACC): Double = acc.sum / acc.count

  override def createAccumulator(): TempaggACC = new TempaggACC

  //定死函数名
  def accumulate(tempaggACC: TempaggACC,temp:Double): Unit ={
    tempaggACC.sum += temp
    tempaggACC.count += 1
  }
}