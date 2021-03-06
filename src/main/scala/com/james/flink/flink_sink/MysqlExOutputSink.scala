package com.james.flink.flink_sink

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}

import com.james.flink.conf.{ConstConfig, SrcQiyongExCntOutput}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.slf4j.LoggerFactory

/**
  * 来源启用量异常阈值检测输出Sink
  * d
  * 用于配置每个来源的异常阈值
  *
  * 每天更新一次，把产生的来源启用量异常输出到JDBC Sink中
  */
class MysqlExOutputSink extends RichSinkFunction[SrcQiyongExCntOutput] {
  private val LOG = LoggerFactory.getLogger(classOf[MysqlExOutputSink])

  private var conn: Connection = _
  private var insertStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    LOG.error("called open()")
    super.open(parameters)

    // 初始化Mysql连接
    Class.forName(ConstConfig.MYSQL_DRIVER)
    try {
      conn = DriverManager.getConnection(ConstConfig.MYSQL_URL, ConstConfig.MYSQL_USERNAME, ConstConfig.MYSQL_PASSWORD)

      insertStmt = conn.prepareStatement("insert into " + ConstConfig.MYSQL_TABLE_OUTPUT + " (src, current_cnt, low_cnt, high_cnt, event_time, update_time) VALUES(?,?,?,?,?,?)")
    } catch {
      case ex: Exception => {
        LOG.error("failed to connect to mysql:url={}", ConstConfig.MYSQL_URL)
        LOG.error(ex.getMessage)
        throw new Exception(ex)
      }
    }
  }

  override def close(): Unit = {
    LOG.error("called close()")
    super.close()

    try {
      if (null != insertStmt) {
        insertStmt.close()
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
      if (null != insertStmt) {
        insertStmt.close()
      }

      if (null != conn) {
        conn.close()
      }
    }
  }

  /**
    * 将异常结果插入到数据表中
    *
    * @param value
    * @param ctx
    */
  override def invoke(value: SrcQiyongExCntOutput, ctx: SinkFunction.Context[_]): Unit = {
    LOG.error("called invoke()")
    LOG.error("invoke()>>> " + value.src + " -> " + value.currentCnt + " -> " + value.updateTs)

    try {
      insertStmt.setInt(1, value.src.toInt)
      insertStmt.setLong(2, value.currentCnt)
      insertStmt.setLong(3, value.lowCnt)
      insertStmt.setLong(4, value.highCnt)
      insertStmt.setTimestamp(5, new Timestamp((value.eventTs)))
      insertStmt.setTimestamp(6, new Timestamp((value.updateTs)))

      insertStmt.execute()
    } catch {
      case ex: Exception => {
        LOG.error("failed to disconnect to mysql.")
        LOG.error(ex.getMessage)
        throw new Exception(ex)
      }
    }
  }
}
