package com.atguigu.flink_first_look.TableAPI_and_flinkSQL

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._

object BatchStreamOldApi02 {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)


    /*
    //老版本流式处理
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()

    val oldStreamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env,settings)


    //老版本批处理
    val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val batchOldEnv: BatchTableEnvironment = BatchTableEnvironment.create(batchEnv)


    //blink 流
    val blinkSetting: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val blinkTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env,blinkSetting)


    //blink 批
    val blinkBatchSetting: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()

    val blinkBatchTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env,blinkBatchSetting)
    */




    //文件读取数据
    val filePath = "D:\\study\\IDEA\\flink_first_look\\src\\main\\resources\\sensor.txt"

    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv())
      .withSchema(new Schema()
          .field("num",DataTypes.STRING())
          .field("timestamp",DataTypes.BIGINT())
          .field("tmp",DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")



    /*
    //kafka读取数据据
    tableEnv.connect(new Kafka()
        .version("0.11")
        .topic("test")
        .property("zookkeeper.connect","192.168.30.131:2181")
        .property("bootstrap.servers","192.168.30.131:9092")
    ).withFormat(new Csv())
      .withSchema(new Schema()
          .field("num",DataTypes.STRING())
          .field("timestamp",DataTypes.BIGINT())
          .field("tmp",DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaTable")
      */


    //flink sql  直接使用临时表
    val tableQuery: Table = tableEnv.sqlQuery("select * from inputTable where tmp <= 30")

    //table API  拿到Table再使用，包括后面打印
    val inputTable: Table = tableEnv.from("inputTable")
    val tableSql: Table = inputTable.select("num,tmp")
      .filter("tmp <= 30")

    //表的打印
    tableQuery.toAppendStream[(String,Long,Double)].print("tableSql")

    env.execute()

  }

}
