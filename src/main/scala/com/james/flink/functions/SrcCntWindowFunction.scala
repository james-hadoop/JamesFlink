package com.james.flink.functions

import java.text.SimpleDateFormat

import com.james.flink.conf.SrcCntResult
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
  * 来源 -- 内容量窗口处理函数
  * 用于计算某个时间窗口内所有来源的(入库量, 机审量, 人审量, 启用量)
  *
  * 输入：(Long, Long, Long, Long)
  * 输出：SrcCntResult
  */
class SrcCntWindowFunction() extends WindowFunction[(Long, Long, Long, Long), SrcCntResult, Long, TimeWindow] {
  private val LOG = LoggerFactory.getLogger(classOf[SrcCntWindowFunction])

  override def apply(key: Long, window: TimeWindow, input: Iterable[(Long, Long, Long, Long)], out: Collector[SrcCntResult]): Unit = {
    val tuple = input.iterator.next()

//    LOG.warn("apply() >>>" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(window.getEnd) + " call SrcCntWindowFunction()")

    out.collect(SrcCntResult(key.toLong, tuple._1, tuple._2, tuple._3, tuple._4, window.getEnd))
  }
}