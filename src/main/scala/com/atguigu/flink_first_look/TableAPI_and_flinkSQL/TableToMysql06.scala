package com.atguigu.flink_first_look.TableAPI_and_flinkSQL

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._

object TableToMysql06 {


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



    val sinkDDL: String =
      """
        |create table jdbcOutputTable (
        | id varchar(20) not null,
        | cnt bigint not null
        |) with (
        | 'connector.type' = 'jdbc',
        | 'connector.url' = 'jdbc:mysql://localhost:3306/test',
        | 'connector.table' = 'sensor_count',
        | 'connector.driver' = 'com.mysql.jdbc.Driver',
        | 'connector.username' = 'root',
        | 'connector.password' = '123456'
        |)
      """.stripMargin

    //执行ddl创建表
    tableEnv.sqlUpdate(sinkDDL)
    aggrTable.insertInto("jdbcOutputTable")



    env.execute()


  }


}
