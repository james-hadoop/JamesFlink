package com.james.flink.app.wc

import org.apache.flink.streaming.api.scala._
import org.slf4j.LoggerFactory

object WordCountDataStream {
  val LOG = LoggerFactory.getLogger(WordCountDataSet.getClass)


  def main(args: Array[String]): Unit = {
    LOG.info(WordCountDataStream.getClass.getName + " start...")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream = env.socketTextStream("localhost", 7777)

    val wordCountDataStream = dataStream.flatMap(_.split(" ")).filter(_.nonEmpty).map((_, 1)).keyBy(0).sum(1)

    wordCountDataStream.print()

    env.execute("WordCountDataStream")
  }
}
