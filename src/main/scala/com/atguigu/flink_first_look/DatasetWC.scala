package com.atguigu.flink_first_look

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

object DatasetWC {

  def main(args: Array[String]): Unit = {

    val tool: ParameterTool = ParameterTool.fromArgs(args)

    val input: String = tool.get("input")
    val output: String = tool.get("output")

    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //"D:\\data\\allfiles.txt")
    val ds: DataSet[String] = environment.readTextFile(input)

    val unit: AggregateDataSet[(String, Int)] = ds.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)

    unit.writeAsCsv(output).setParallelism(1)

    environment.execute()

  }
}
