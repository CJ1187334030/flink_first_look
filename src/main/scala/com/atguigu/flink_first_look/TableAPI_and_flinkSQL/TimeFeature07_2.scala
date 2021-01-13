package com.atguigu.flink_first_look.TableAPI_and_flinkSQL


import com.atguigu.flink_first_look.bean.SensorRead
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._
import org.apache.flink.types.Row

object TimeFeature07_2 {


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


    //指定或直接追加
    val tableTime: Table = tableEnv.fromDataStream(ds,'num,'timestamp.rowtime,'tmp)

    val tableTime2: Table = tableEnv.fromDataStream(ds,'num,'timestamp,'tmp,'rt.rowtime)


    //table schema时指定
    tableEnv.connect( new FileSystem().path(filePath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("num", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .rowtime( new Rowtime()
          .timestampsFromField("timestamp") // 从字段中提取时间戳
           .watermarksPeriodicBounded(1000) // watermark 延迟 1 秒
           )
        .field("tmp", DataTypes.DOUBLE())
      ) // 定义表结构
    .createTemporaryTable("inputTable") // 创建临时表


//    tableTime.toAppendStream[Row].print()
//    tableTime2.toAppendStream[Row].print()

    val tableTime3: Table = tableEnv.sqlQuery("select * from inputTable")

    tableTime3.toAppendStream[Row].print()

    env.execute()
  }


}
