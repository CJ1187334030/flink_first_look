package com.atguigu.flink_first_look.TableAPI_and_flinkSQL

import com.atguigu.flink_first_look.bean.SensorRead
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.{ScalarFunction, TableFunction}
import org.apache.flink.types.Row

object FunctionAndUDF09 {

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

    //ScalarFunction
    //table API调用
    val myScalarFunction = new MyScalarFunction(100)
    val table01: Table = table.select('num,'tmp,myScalarFunction('num))


    //flink sql  不要使用function关键字
    tableEnv.createTemporaryView("sensor",table)
    tableEnv.registerFunction("myScalarFunction",myScalarFunction)
    val table02: Table = tableEnv.sqlQuery("select num,myScalarFunction(num) from sensor")


    //TableFunction
    //Table API
    val tableFunction = new MyTableFunction("_")
    val table03: Table = table.joinLateral(tableFunction('num) as('word, 'length))
      .select('num, 'word, 'length)

    //flink sql
    tableEnv.registerFunction("split",tableFunction)
    val table04 = tableEnv.sqlQuery(
      """
        |select num,word,length
        |from sensor,
        |lateral table(split(num)) as newsensor(word,length)
      """.stripMargin)



    table04.toAppendStream[Row].print()



    env.execute()

  }

}



//一对多TableFunction flatmap  Unit必须
class MyTableFunction(splitor:String) extends TableFunction[(String,Int)]{
  def eval(s:String): Unit ={
    s.split(splitor).foreach(
      word => collect((word,word.length))
    )
  }
}

//一对一ScalarFunction  map
class MyScalarFunction(factor:Int) extends ScalarFunction{

  def eval(s:String) ={
    s.hashCode * factor -10000
  }

}
