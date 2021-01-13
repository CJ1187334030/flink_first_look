package com.atguigu.flink_first_look.TableAPI_and_flinkSQL

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object TableSink03 {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)


    val filePath = "F:\\study\\IDEA\\flink_first_look\\src\\main\\resources\\sensor.txt"

    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("num",DataTypes.STRING())
        .field("timestamp",DataTypes.BIGINT())
        .field("tmp",DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")

    val allTable: Table = tableEnv.sqlQuery("select * from inputTable")

    val aggrTable: Table = tableEnv.sqlQuery("select num,count(*) from inputTable group by num")

    allTable.toAppendStream[(String,Long,Double)].print()

    aggrTable.toRetractStream[(String,Long)].print()


    //文件输出
    val outputFilePath = "F:\\study\\IDEA\\flink_first_look\\src\\main\\resources\\sensor_output.txt"

    tableEnv.connect(new FileSystem().path(outputFilePath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("num",DataTypes.STRING())
        .field("timestamp",DataTypes.BIGINT())
        .field("tmp",DataTypes.DOUBLE())
      )
      .createTemporaryTable("outputTable")

    //追加流可以输出，aggr流无法输出（变化流）
    allTable.insertInto("outputTable")




    env.execute()

  }

}
