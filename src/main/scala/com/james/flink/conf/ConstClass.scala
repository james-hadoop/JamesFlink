package com.james.flink.conf

import java.util.Date

/**
  * 原始日志样例类
  *
  * @param dbank_imp_date
  * @param reporttime
  * @param clientip
  * @param ts
  * @param op_id
  * @param ip
  * @param appid
  * @param db
  * @param table
  * @param extra_info
  * @param rowkey
  */
case class Log04620(dbank_imp_date: String, reporttime: String, clientip: String, ts: Long, op_id: String, ip: String, appid: String, db: String, table: String, extra_info: String, rowkey: String)

/**
  * 内容审核时间样例类
  *
  * @param rowkey    内容ID
  * @param src       来源ID
  * @param rukuTs    入库时间戳
  * @param jishenTs  机审时间戳
  * @param renshenTs 人审时间戳
  * @param qiyongTs  启用时间戳
  * @param timestamp 日志上报时间戳
  */
case class RowkeyInfo(rowkey: String, src: Long, rukuTs: Long, jishenTs: Long, renshenTs: Long, qiyongTs: Long, timestamp: Long)

/**
  * 来源审核量样例类
  *
  * @param src        来源ID
  * @param rukuCnt    入库量
  * @param jishenCnt  机审量
  * @param renshenCnt 人审量
  * @param qiyongCnt  启用量
  * @param windowEnd  时间窗口时间戳，用于窗口聚合
  */
case class SrcCntResult(src: Long, rukuCnt: Long, jishenCnt: Long, renshenCnt: Long, qiyongCnt: Long, windowEnd: Long)

/**
  * 来源启用量异常阈值样例类
  *
  * @param src      来源ID
  * @param lowCnt   异常低位阈值，低于该值位异常
  * @param highCnt  异常高位阈值，高于该值位异常
  * @param updateTs 更新时间
  */
case class SrcQiyongExCntThreshold(src: Long, lowCnt: Long, highCnt: Long, updateTs: Date)

/**
  * 来源启用量异常阈值样例类
  *
  * @param src        来源ID
  * @param currentCnt 当前来源启用量
  * @param lowCnt     异常低位阈值，低于该值位异常
  * @param highCnt    异常高位阈值，高于该值位异常
  * @param updateTs   更新时间
  */
case class SrcQiyongExCntOutput(src: Long, currentCnt: Long, lowCnt: Long, highCnt: Long, updateTs: Date)
