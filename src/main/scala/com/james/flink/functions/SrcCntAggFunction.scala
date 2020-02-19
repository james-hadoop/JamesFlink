package com.james.flink.functions

import com.james.flink.conf.SrcCntResult
import org.apache.flink.api.common.functions.AggregateFunction
import org.slf4j.LoggerFactory

/**
  * 来源 -- 内容量聚合处理函数
  * 用于聚合某个时间窗口内所有来源的(入库量, 机审量, 人审量, 启用量)
  * 把流水数据转化成时间窗口内的批量数据，减少数据量
  *
  * 输入：SrcCntResult
  * 输出：(Long, Long, Long, Long)
  */
class SrcCntAggFunction() extends AggregateFunction[SrcCntResult, (Long, Long, Long, Long), (Long, Long, Long, Long)] {
  private val LOG = LoggerFactory.getLogger(classOf[SrcCntAggFunction])

  override def createAccumulator(): (Long, Long, Long, Long) = (0, 0, 0, 0)

  // 使用累加器计数
  override def add(value: SrcCntResult, accumulator: (Long, Long, Long, Long)): (Long, Long, Long, Long) = {
    LOG.debug("add() >>> " + value.src + ": " + value.qiyongCnt)

    (accumulator._1 + value.rukuCnt, accumulator._2 + value.jishenCnt, accumulator._3 + value.renshenCnt, accumulator._4 + value.qiyongCnt)
  }

  override def getResult(accumulator: (Long, Long, Long, Long)): (Long, Long, Long, Long) = {
    LOG.debug("getResult() >>> accumulator=" + accumulator)

    accumulator
  }

  override def merge(a: (Long, Long, Long, Long), b: (Long, Long, Long, Long)): (Long, Long, Long, Long) = {
    (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4)
  }
}