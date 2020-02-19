package com.james.flink.wc

import org.apache.flink.api.scala._
import org.slf4j.LoggerFactory

object WordCountDataSet {
  val LOG = LoggerFactory.getLogger(WordCountDataSet.getClass)

  def main(args: Array[String]): Unit = {
    LOG.info(WordCountDataSet.getClass.getName + " start...")

    val resource = getClass.getResource("/data/hello.txt")

    val env = ExecutionEnvironment.getExecutionEnvironment

    // 从文件中取数据
    val inputDataSet = env.readTextFile(resource.getPath)

    // word count处理
    val wordCountDataSet = inputDataSet.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)
    wordCountDataSet.print()
  }
}
