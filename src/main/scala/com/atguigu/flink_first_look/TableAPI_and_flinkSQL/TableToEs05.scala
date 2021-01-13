package com.atguigu.flink_first_look.TableAPI_and_flinkSQL

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._

object TableToEs05 {

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


    //输出到Es  支持group by
    tableEnv.connect( new Elasticsearch()
      .version("6")
      .host("localhost", 9200, "http")
      .index("sensor")
      .documentType("temp") )
      .inUpsertMode()
      .withFormat(new Json())
      .withSchema( new Schema()
        .field("id", DataTypes.STRING())
        .field("count", DataTypes.BIGINT())
      )
      .createTemporaryTable("esOutputTable")


    allTable.insertInto("esOutputTable")


    env.execute()

  }

}
