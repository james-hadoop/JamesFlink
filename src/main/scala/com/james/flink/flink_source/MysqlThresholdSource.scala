package com.james.flink.flink_source

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.james.flink.conf.{ConstConfig, ConstValue, SrcQiyongExCntThreshold}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * 来源内容量异常阈值Source
  *
  * 用于配置每个来源的异常阈值
  *
  * 每隔特定时间更新一次，更新好的异常阈值数据广播到数据处理流
  */
class MysqlThresholdSource extends RichSourceFunction[mutable.HashMap[Long, SrcQiyongExCntThreshold]] {
  private val LOG = LoggerFactory.getLogger(classOf[MysqlThresholdSource])

  private var conn: Connection = null
  private var ps: PreparedStatement = null

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    // 初始化Mysql连接
    Class.forName(ConstConfig.MYSQL_DRIVER)
    try {
      conn = DriverManager.getConnection(ConstConfig.MYSQL_URL, ConstConfig.MYSQL_USERNAME, ConstConfig.MYSQL_PASSWORD)

      val sql = "select src, low_cnt, high_cnt, update_time from " + ConstConfig.MYSQL_TABLE_THRESHOLD
      ps = conn.prepareStatement(sql)
    } catch {
      case ex: Exception => {
        LOG.error("failed to connect to mysql:url={}", ConstConfig.MYSQL_URL)
        LOG.error(ex.getMessage)
        throw new Exception(ex)
      }
    }
  }

  override def close(): Unit = {
    super.close()

    try {
      if (null != ps) {
        ps.close()
      }

      if (null != conn) {
        conn.close()
      }
    } catch {
      case ex: Exception => {
        LOG.error("failed to disconnect to mysql.")
        LOG.error(ex.getMessage)
        throw new Exception(ex)
      }
    } finally {
      if (null != ps) {
        ps.close()
      }

      if (null != conn) {
        conn.close()
      }
    }
  }

  override def run(ctx: SourceFunction.SourceContext[mutable.HashMap[Long, SrcQiyongExCntThreshold]]): Unit = {
    try {
      while (true) {
        val output = new mutable.HashMap[Long, SrcQiyongExCntThreshold]();
        val rs = ps.executeQuery();
        while (rs.next()) {
          val src = rs.getInt(ConstValue.THRESHOLD_SRC_FIELD)
          val lowCnt = rs.getLong(ConstValue.THRESHOLD_LOW_CNT_FIELD)
          val highCnt = rs.getLong(ConstValue.THRESHOLD_HIGH_CNT_FIELD)
          val updateTs = rs.getDate(ConstValue.THRESHOLD_UPDATE_TIME_FIELD)

          LOG.info("update threshold config:" + (src, lowCnt, highCnt, updateTs))

          output.put(src, SrcQiyongExCntThreshold(src, lowCnt, highCnt, updateTs));
        }
        ctx.collect(output);

        //每隔特定的时间更新一次阈值，当前设置是3600000毫秒，即1小时
        Thread.sleep(ConstValue.THRESHOLD_UPDATE_INTERVAL);
      }
    } catch {
      case ex: Exception => {
        LOG.error("failed to update threshold config")
        LOG.error(ex.getMessage)
        throw new Exception(ex)
      }
    }
  }

  override def cancel(): Unit = {
    super.close()

    if (null != conn) {
      conn.close()
    }

    if (null != ps) {
      ps.close()
    }
  }
}