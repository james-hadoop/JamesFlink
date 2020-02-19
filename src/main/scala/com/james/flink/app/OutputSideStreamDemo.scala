package com.james.flink.app

import com.james.flink.conf.SrcQiyongExCntThreshold
import com.james.flink.flink_source.MysqlThresholdSource
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.mutable

object OutputSideStreamDemo {
  private val LOG = LoggerFactory.getLogger(OutputSideStreamDemo.getClass)

  def main(args: Array[String]): Unit = {
    LOG.info("OutputSideStreamDemo start...")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val mainStream = env.addSource(new MysqlThresholdSource())
    val outsideStream = mainStream.process(new ExDetectFunction())
    outsideStream.getSideOutput(new OutputTag[String]("outputSideStreamTagFromOutputSideStreamDemo")).print(">>> ex data")

    mainStream.print()

    env.execute()
  }
}

class ExDetectFunction() extends ProcessFunction[mutable.HashMap[Long, SrcQiyongExCntThreshold], mutable.HashMap[Long, SrcQiyongExCntThreshold]] {
  lazy val exDetectOutput: OutputTag[String] = new OutputTag[String]("outputSideStreamTagFromOutputSideStreamDemo")

  override def processElement(value: mutable.HashMap[Long, SrcQiyongExCntThreshold], ctx: ProcessFunction[mutable.HashMap[Long, SrcQiyongExCntThreshold], mutable.HashMap[Long, SrcQiyongExCntThreshold]]#Context, out: Collector[mutable.HashMap[Long, SrcQiyongExCntThreshold]]): Unit = {
    val iter = value.iterator
    while (iter.hasNext) {
      val entry = iter.next()
      if (entry._1 == 49) {
        ctx.output(exDetectOutput, entry._2.src + "-" + entry._2.lowCnt + "-" + entry._2.highCnt + "-" + entry._2.updateTs)
      } else {
        out.collect(value)
      }
    }
  }
}
