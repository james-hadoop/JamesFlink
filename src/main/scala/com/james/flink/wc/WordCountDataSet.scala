package com.james.flink.wc

import org.apache.flink.api.scala._

object WordCountDataSet {
  def main(args: Array[String]): Unit = {
    println("Hello!")

    val env = ExecutionEnvironment.getExecutionEnvironment

    // 从文件中取数据
    val inputPath = "/home/james/workspace/JamesFlink/src/main/resources/hello.txt"
    val inputDataSet = env.readTextFile(inputPath)

    // word count处理
    val wordCountDataSet = inputDataSet.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)
    wordCountDataSet.print()
  }
}
