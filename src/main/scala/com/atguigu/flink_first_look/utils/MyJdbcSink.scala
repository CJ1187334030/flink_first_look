package com.atguigu.flink_first_look.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.atguigu.flink_first_look.bean.Person
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}


class MyJdbcSink extends RichSinkFunction[Person] {

  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  //创建连接
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    conn = DriverManager.getConnection("jdbc:mysql://192.168.30.131:3306/test", "root", "111111")
    insertStmt = conn.prepareStatement("INSERT INTO flink_person (name,age,sex) VALUES (?,?,?)")
    updateStmt = conn.prepareStatement("UPDATE flink_person SET age = ? WHERE name = ?")

  }

  override def invoke(value: Person, context: SinkFunction.Context[_]): Unit = {

    updateStmt.setLong(1, value.age)
    updateStmt.setString(2, value.name)
    updateStmt.execute()


    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.name)
      insertStmt.setLong(2, value.age)
      insertStmt.setString(3, value.sex)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }

}
