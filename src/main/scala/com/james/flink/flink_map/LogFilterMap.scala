package com.james.flink.flink_map

import com.james.flink.conf.Log04620
import org.apache.flink.api.common.functions.RichMapFunction
import org.slf4j.LoggerFactory

class LogFilterMap extends RichMapFunction[String, Log04620] {
  private val LOG = LoggerFactory.getLogger(classOf[LogFilterMap])

  override def map(value: String): Log04620 = {
    LOG.warn(value)

    if (null == value || 1 > value.length) {
      return null
    }

    try {
      val dataArray = value.split("\\t")
      if (null == dataArray || dataArray.size != 11) {
        return null
      }

      LOG.warn("Log04620>>> " + (dataArray(0).trim, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong, dataArray(4).trim, dataArray(5).trim, dataArray(6).trim, dataArray(7), dataArray(8).trim, dataArray(9).trim, dataArray(10).trim))


      val ret = Log04620(dataArray(0).trim, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong, dataArray(4).trim, dataArray(5).trim, dataArray(6).trim, dataArray(7), dataArray(8).trim, dataArray(9).trim, dataArray(10).trim)
      return ret
    } catch {
      case ex: Exception =>
        LOG.error(ex.getMessage)
        LOG.error(value)
        val ret = Log04620("", "", "", -1, "", "", "", "", "", "", "")
        return ret
    }
  }
}
