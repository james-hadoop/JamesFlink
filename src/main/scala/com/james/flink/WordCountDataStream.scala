package com.james.flink

import org.apache.flink.streaming.api.scala._

object WordCountDataStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream = env.socketTextStream("localhost", 7777)

    val wordCountDataStream = dataStream.flatMap(_.split(" ")).filter(_.nonEmpty).map((_, 1)).keyBy(0).sum(1)

    wordCountDataStream.print()

    env.execute("WordCountDataStream")
  }
}
