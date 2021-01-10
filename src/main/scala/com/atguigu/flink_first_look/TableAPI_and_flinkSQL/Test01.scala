package com.atguigu.flink_first_look.TableAPI_and_flinkSQL

import com.atguigu.flink_first_look.bean.SensorRead
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

object Test01 {

  def main(args: Array[String]): Unit = {


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val ds: DataStream[SensorRead] = env.readTextFile("D:\\study\\IDEA\\flink_first_look\\src\\main\\resources\\sensor.txt")
      .filter(_.length > 20)
      .map {
        log =>
          val strings: Array[String] = log.split(",")
          SensorRead(strings(0), strings(1).toLong, strings(2).toDouble)
      }



    //创建环境
    val tableEnv = StreamTableEnvironment.create(env)

    //加载数据
    val dateTable: Table = tableEnv.fromDataStream(ds)

   //table Api
    val resTable: Table = dateTable.select("num,tmp")
      .filter("num == 'sensor_1'")

    resTable.toAppendStream[(String,Double)].print("result")


    //flink sql
    tableEnv.createTemporaryView("temp",ds)
    val resTable2:Table = tableEnv.sqlQuery("select * from temp where num = 'sensor_1'")

    resTable2.toAppendStream[(String,Long,Double)].print("result2")




    env.execute("hfghgf")


  }

}
