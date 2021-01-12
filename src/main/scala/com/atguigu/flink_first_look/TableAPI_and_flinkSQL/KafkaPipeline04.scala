package com.atguigu.flink_first_look.TableAPI_and_flinkSQL

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._

object KafkaPipeline04 {


  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)


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
      .createTemporaryTable("inputKafka")


    val kafkaTable: Table = tableEnv.sqlQuery("select num,tmp from inputKafka")


    //输出  不支持 给aggr
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("test")
      .property("zookkeeper.connect","192.168.30.131:2181")
      .property("bootstrap.servers","192.168.30.131:9092")
    ).withFormat(new Csv())
      .withSchema(new Schema()
        .field("num",DataTypes.STRING())
        .field("tmp",DataTypes.DOUBLE())
      )
      .createTemporaryTable("outputKafka")


    kafkaTable.insertInto("outputKafka")


    env.execute()


  }

}
