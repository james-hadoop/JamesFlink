package com.james.flink.functions

import com.james.flink.conf.{RowkeyInfo, SrcCntResult}
import org.apache.flink.api.common.functions.RichMapFunction
import org.slf4j.LoggerFactory

/**
  * 转换处理函数
  *
  * 将RowkeyInfo数据经过json解析，转换成SrcCntResult
  */
class ParseSrcAuditCntMapFunction extends RichMapFunction[RowkeyInfo, SrcCntResult] {
  private val LOG = LoggerFactory.getLogger(classOf[ParseSrcAuditCntMapFunction])

  override def map(rowkeyInfo: RowkeyInfo): SrcCntResult = {
    LOG.debug("\tmap() >>> " + rowkeyInfo.rowkey + "->" + rowkeyInfo.src + "->" + rowkeyInfo.qiyongTs)

    var rukuCnt = 0
    var jishenCnt = 0
    var renshenCnt = 0
    var qiyongCnt = 0

    // 内容的来源为空
    if (-1 == rowkeyInfo.src) {
      SrcCntResult(rowkeyInfo.src, 0, 0, 0, 0, -1)
    } else {
      // 单条内容入库
      if (rowkeyInfo.rukuTs > 0) {
        rukuCnt = 1
      }

      // 单条内容机审
      if (rowkeyInfo.jishenTs > 0) {
        jishenCnt = 1
      }

      // 单条内容人审
      if (rowkeyInfo.renshenTs > 0) {
        renshenCnt = 1
      }

      // 单条内容启用
      if (rowkeyInfo.qiyongTs > 0) {
        qiyongCnt = 1
      }

      LOG.debug(rowkeyInfo.src.toString + rukuCnt.toString + jishenCnt.toString + renshenCnt.toString + qiyongCnt.toString)
      SrcCntResult(rowkeyInfo.src, rukuCnt, jishenCnt, renshenCnt, qiyongCnt, -1)
    }
  }
}