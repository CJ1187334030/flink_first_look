package com.atguigu.flink_first_look.TableAPI_and_flinkSQL

import com.atguigu.flink_first_look.bean.SensorRead
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object Window08 {

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

    val dstable: Table = tableEnv.fromDataStream(ds,'num,'timestamp.rowtime as 'ts,'tmp)

    //group window
    //每10S统计一次，滚动窗口  Table API实现
    val table01: Table = dstable
      .window(Tumble over 10.seconds on 'ts as 'w)
      .groupBy('w, 'num)
      .select('num, 'num.count,'w.end)

    //flink sql实现
    tableEnv.createTemporaryView("sensor",dstable)

    val table02:Table = tableEnv.sqlQuery(
      """
        |select num,
        |count(*),
        |avg(tmp),
        |tumble_end(ts,interval '10' second)
        |from sensor
        |group by num,
        |tumble(ts, interval '10' second)
      """.stripMargin)


    //over window  每个传感器当前温度与前面两次温度的xxx
    //table API
    val table03: Table = dstable
      .window(Over partitionBy 'num orderBy 'ts preceding 2.rows as 'ov)
      .select('num, 'ts, 'num.count over 'ov, 'tmp.avg over 'ov)

    //flink sql
    val table04 = tableEnv.sqlQuery(
      """
        |select num,ts,
        |count(num) over ov,
        |avg(tmp) over ov
        |from sensor
        |window ov as(
        |partition by num
        |order by ts
        |rows between 2 preceding and current row
        |)
      """.stripMargin
    )


    //10s输出结果append 和 retract一样
    table04.toAppendStream[Row].print()


    env.execute()

  }

}
