package com.james.flink.conf

import org.apache.flink.api.java.utils.ParameterTool
import org.slf4j.LoggerFactory

/**
  * 解析配置文件，生成配置信息类
  */
object ConstConfig {
  val LOG = LoggerFactory.getLogger(ConstConfig.getClass)

  val configPropertiesResource = getClass.getResource("/config.properties")
  val parameters = ParameterTool.fromPropertiesFile(configPropertiesResource.getPath)

  /*
   * FLINK
   */
  LOG.info("FLINK CONFIG as below: ----------------")

  /*
   SOURCE FILE LOG
   */
  val SRC_FILE_NAME = parameters.get("src.file.name")

  LOG.info("SOURCE FILE LOG CONFIG as below: ----------------")
  LOG.info("SRC_FILE_NAME={}", SRC_FILE_NAME)

  /*
   * REDIS
   */
  val REDIS_HOST = parameters.get("redis.host", "127.0.0.1")
  val REDIS_PORT = parameters.get("redis.port", "6379").toInt
  val REDIS_MAX_CONN_SIZE = parameters.get("redis.max.conn.size", "100").toInt
  val REDIS_MAX_IDLE_CONN_SIZE = parameters.get("redis.max.idle.conn.size", "10").toInt
  val REDIS_MIN_IDLE_CONN_SIZE = parameters.get("redis.min.wait.ms", "10").toInt
  val REDIS_MAX_WAIT_MS = parameters.get("redis.max.wait.ms", "5000").toInt
  val REDIS_CONN_TIMEOUT_MS = parameters.get("redis.conn.timeout.ms", "5000").toInt
  val REDIS_TEST_ON_BORROW = parameters.get("redis.test.on.borrow", "true").toBoolean
  val REDIS_TEST_ON_RETURN = parameters.get("redis.test.on.return", "true").toBoolean
  val REDIS_EXPIRE_HOUR = parameters.get("redis.expire.hour", "24").toInt

  LOG.info("REDIS CONFIG CONFIG as below: ----------------")
  LOG.info("REDIS_HOST={}", REDIS_HOST)
  LOG.info("REDIS_PORT={}", REDIS_PORT)
  LOG.info("REDIS_MAX_CONN_SIZE={}", REDIS_MAX_CONN_SIZE)
  LOG.info("REDIS_MAX_IDLE_CONN_SIZE={}", REDIS_MAX_IDLE_CONN_SIZE)
  LOG.info("REDIS_MAX_WAIT_MS={}", REDIS_MAX_WAIT_MS)
  LOG.info("REDIS_CONN_TIMEOUT_MS={}", REDIS_CONN_TIMEOUT_MS)
  LOG.info("REDIS_TEST_ON_BORROW={}", REDIS_TEST_ON_BORROW)
  LOG.info("REDIS_EXPIRE_HOUR={}", REDIS_EXPIRE_HOUR)

  /*
   * MYSQL
   */
  val MYSQL_DRIVER = parameters.get("mysql.driver")
  val MYSQL_URL = parameters.get("mysql.url")
  val MYSQL_USERNAME = parameters.get("mysql.username")
  val MYSQL_PASSWORD = parameters.get("mysql.password")
  val MYSQL_TABLE_THRESHOLD = parameters.get("mysql.table.threshold")
  val MYSQL_TABLE_OUTPUT = parameters.get("mysql.table.output")

  LOG.info("MYSQL CONFIG CONFIG as below: ----------------")
  LOG.info("MYSQL_DRIVER={}", MYSQL_DRIVER)
  LOG.info("MYSQL_URL={}", MYSQL_URL)
  LOG.info("MYSQL_TABLE_THRESHOLD={}", MYSQL_TABLE_THRESHOLD)
  LOG.info("MYSQL_TABLE_OUTPUT={}", MYSQL_TABLE_OUTPUT)

  def main(args: Array[String]): Unit = {
    println("Redis config:" + REDIS_HOST)
  }
}
