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
import org.apache.flink.util.Collector

object TableAggregateFunction11 {

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


    val myTableAggregateFunction = new MyTableAggregateFunction

    // Table Api  无flink sql
    val table01: Table = table.groupBy('num)
      .flatAggregate(myTableAggregateFunction('tmp) as('tmp, 'rank))
      .select('num, 'tmp, 'rank)


    table01.toRetractStream[Row].print()


    env.execute()

  }

}


class Top2TmpAcc{
  var highestTmp:Double = Double.MinValue
  var secondHighestTmp = Double.MinValue
}

//输出（tmp，rank）
class MyTableAggregateFunction extends TableAggregateFunction[(Double,Int),Top2TmpAcc] {

  override def createAccumulator(): Top2TmpAcc = new Top2TmpAcc

  //实现accumulate 处理方法
  def accumulate(top2TmpAcc: Top2TmpAcc,tmp:Double): Unit ={

    if (tmp > top2TmpAcc.highestTmp) {
      top2TmpAcc.secondHighestTmp = top2TmpAcc.highestTmp
      top2TmpAcc.highestTmp = tmp
    }
    else if (tmp > top2TmpAcc.secondHighestTmp)
      top2TmpAcc.secondHighestTmp = tmp
  }

  //定义输出函数
  def emitValue(acc: Top2TmpAcc,out:Collector[(Double,Int)]): Unit ={

    out.collect(acc.highestTmp,1)
    out.collect(acc.secondHighestTmp,2)

  }

}


