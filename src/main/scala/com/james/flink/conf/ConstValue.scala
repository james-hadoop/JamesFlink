package com.james.flink.conf

/**
  * 字段常量
  */
object ConstValue {
  /**
    * redis key，src与内容数的映射关系
    */
  val SRC_CNT_KEY = "src-cnt"

  /**
    * redis key，src与内容数的映射关系
    */
  val DEBUG_SRC_CNT_KEY = "debug_src-cnt"

  /**
    * redis key，rowkey与src的映射关系
    */
  val ROWKEY_SRC_KEY = "rowkey-src"

  /**
    * redis key，src与内容数的映射关系
    */
  val SRC_CNT_KEY_LOCAL = "src-cnt-local"

  /**
    * 需要解析的上报json字段名称
    */
  val FIELD_VALUE_FIELD = "field_value"

  /**
    * 一级来源ID
    */
  val SRC = "src"

  /**
    * 日志原始入库时间戳字段
    */
  val ORIGIN_RUKU_FIELD = "input_ts"

  /**
    * 日志原始机审时间戳字段
    */
  val ORIGIN_JISHEN_FIELD = "machine_process_end_time"

  /**
    * 日志原始人审时间戳字段
    */
  val ORIGIN_RENSHEN_FIELD = "st_evaluate_time"

  /**
    * 日志原始启用时间戳字段
    */
  val ORIGIN_QIYONG_FIELD = "st_kd"

  /**
    * 入库字段
    */
  val RUKU_FIELD = "ruku"

  /**
    * 机审字段
    */
  val JISHEN_FIELD = "jishen"

  /**
    * 人审字段
    */
  val RENSHEN_FIELD = "renshen"

  /**
    * 启用字段
    */
  val QIYONG_FIELD = "qiyong"

  /**
    * 来源启用量广播状态描述符
    */
  val SRC_QIYONG_CNT_THRESHOLD_CONFIG = "srcQiyongCntThresholdConfig"

  /**
    * 异常阈值来源ID
    */
  val THRESHOLD_SRC_FIELD = "src"

  /**
    * 异常阈值启用量阈值低位
    */
  val THRESHOLD_LOW_CNT_FIELD = "low_cnt"

  /**
    * 异常启用量阈值高位
    */
  val THRESHOLD_HIGH_CNT_FIELD = "high_cnt"

  /**
    * 异常启用量阈值更新时间
    */
  val THRESHOLD_UPDATE_TIME_FIELD = "update_time"

  /**
    * 异常启用量阈值更新周期：毫秒
    */
  val THRESHOLD_UPDATE_INTERVAL = 10000000

  /**
    * 当前启用量
    */
  val THRESHOLD_CURRENT_CNT_FIELD = "current_cnt"

}
