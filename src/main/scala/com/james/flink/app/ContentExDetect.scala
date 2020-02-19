package com.james.flink.app

import com.james.flink.conf._
import com.james.flink.flink_sink.MysqlExOutputSink
import com.james.flink.flink_source.MysqlThresholdSource
import com.james.flink.functions.{ParseSrcAuditCntMapFunction, SrcCntAggFunction, SrcCntKeyedProcessFunction, SrcCntWindowFunction, SrcThresholdBroadcastProcessFunction}
import com.james.flink.utils.{JedisPoolUtil, JsonUtil}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * 内容启用量异常检测主类
  */
object ContentExDetect {
  private val LOG = LoggerFactory.getLogger(ContentExDetect.getClass)

  val jedisPool = JedisPoolUtil.makePool(ConstConfig.REDIS_HOST, ConstConfig.REDIS_PORT)

  def main(args: Array[String]): Unit = {
    lazy val jedis = JedisPoolUtil.getJedisPool.getResource

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/01.txt")
    val dataStream = env.readTextFile(resource.getPath).filter(_ != null).filter(_.length > 1)
      .map(data => {
        val dataArray = data.split("\\t")
        LOG.debug("Log04620>>> " + (dataArray(0).trim, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong, dataArray(4).trim, dataArray(5).trim, dataArray(6).trim, dataArray(7), dataArray(8).trim, dataArray(9).trim, dataArray(10).trim))
        Log04620(dataArray(0).trim, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong, dataArray(4).trim, dataArray(5).trim, dataArray(6).trim, dataArray(7), dataArray(8).trim, dataArray(9).trim, dataArray(10).trim)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Log04620](Time.seconds(1)) {
        override def extractTimestamp(element: Log04620): Long = {
          element.ts
        }
      })
      .filter(data => data.extra_info.contains(ConstValue.FIELD_VALUE_FIELD) && null != data.rowkey)
      .filter(data => data.extra_info.contains("\"" + ConstValue.ORIGIN_RUKU_FIELD + "\"") || data.extra_info.contains("\"" + ConstValue.ORIGIN_JISHEN_FIELD + "\"") || data.extra_info.contains("\"" + ConstValue.ORIGIN_RENSHEN_FIELD + "\"") || data.extra_info.contains("\"" + ConstValue.ORIGIN_QIYONG_FIELD + "\""))
      .map(data => {
        val rowkey: String = data.rowkey
        var src: Long = -1

        val jsonMap = JsonUtil.jsonString2Map(data.extra_info)
        LOG.debug("jsonMap>>> " + jsonMap)

        if (null != jsonMap) {
          src = jsonMap.get(ConstValue.SRC).get

          if (null != src && -1 != src) {
            jedis.hset(ConstValue.ROWKEY_SRC_KEY, rowkey, src.toString)
          } else {
            val srcString = jedis.hget(ConstValue.ROWKEY_SRC_KEY, rowkey)
            if (null != srcString) {
              src = srcString.toLong
              jedis.hset(ConstValue.ROWKEY_SRC_KEY, rowkey, src.toString)
            }
          }

          RowkeyInfo(rowkey, src, jsonMap.get(ConstValue.RUKU_FIELD).get, jsonMap.get(ConstValue.JISHEN_FIELD).get, jsonMap.get(ConstValue.RENSHEN_FIELD).get, jsonMap.get(ConstValue.QIYONG_FIELD).get, data.ts)
        } else {
          RowkeyInfo(rowkey, src, -1, -1, -1, -1, -1)
        }
      })
      .map(new ParseSrcAuditCntMapFunction())
      .keyBy(_.src)
      .timeWindow(Time.minutes(10), Time.minutes(10))
      .allowedLateness(Time.minutes(60))
      .aggregate(new SrcCntAggFunction(), new SrcCntWindowFunction())
      .keyBy(_.windowEnd)
      .process(new SrcCntKeyedProcessFunction())

    val thresholdStateDesc = new MapStateDescriptor(ConstValue.SRC_QIYONG_CNT_THRESHOLD_CONFIG, TypeInformation.of(classOf[Long]), TypeInformation.of(classOf[mutable.HashMap[Long, SrcQiyongExCntThreshold]]))
    val thresholdStream = env.addSource(new MysqlThresholdSource()).broadcast(thresholdStateDesc)

    val connectedStream = dataStream.connect(thresholdStream).process(new SrcThresholdBroadcastProcessFunction())
    connectedStream.addSink(new MysqlExOutputSink()).uid("ContentQiyongExceptonDetection Job")

    env.execute("ContentQiyongExceptonDetection Job")
  }
}