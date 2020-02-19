package com.james.flink.utils

import com.james.flink.conf.ConstConfig
import org.slf4j.LoggerFactory
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object JedisPoolUtil extends Serializable {
  val LOG = LoggerFactory.getLogger(JedisPoolUtil.getClass)

  @transient private var pool: JedisPool = null

  def makePool(host: String, port: Int): Unit = {
    makePool(host, port, ConstConfig.REDIS_CONN_TIMEOUT_MS, ConstConfig.REDIS_MAX_CONN_SIZE, ConstConfig.REDIS_MAX_IDLE_CONN_SIZE, ConstConfig.REDIS_MIN_IDLE_CONN_SIZE, ConstConfig.REDIS_TEST_ON_BORROW, ConstConfig.REDIS_TEST_ON_RETURN, ConstConfig.REDIS_MAX_WAIT_MS)
  }

  def makePool(host: String, port: Int, redisTimeout: Int, maxTotal: Int, maxIdle: Int, minIdle: Int): Unit = {
    makePool(host, port, redisTimeout, maxTotal, maxIdle, minIdle, ConstConfig.REDIS_TEST_ON_BORROW, ConstConfig.REDIS_TEST_ON_RETURN, ConstConfig.REDIS_MAX_WAIT_MS)
  }

  def makePool(host: String, port: Int, redisTimeout: Int, maxTotal: Int, maxIdle: Int, minIdle: Int, testOnBorrow: Boolean, testOnReturn: Boolean, maxWaitMillis: Long): Unit = {
    if (pool == null) {
      val poolConfig = new JedisPoolConfig()
      poolConfig.setMaxTotal(maxTotal)
      poolConfig.setMaxIdle(maxIdle)
      poolConfig.setMinIdle(minIdle)
      poolConfig.setTestOnBorrow(testOnBorrow)
      poolConfig.setTestOnReturn(testOnReturn)
      poolConfig.setMaxWaitMillis(maxWaitMillis)
      pool = new JedisPool(poolConfig, host, port, redisTimeout)
      LOG.info("JedisPool inited as host={}, port={}, redisTimeout={}, maxTotal={}, maxIdle={}, maxWaitMillis={}", host, port.toString, redisTimeout.toString, maxTotal.toString, maxIdle.toString, maxWaitMillis.toString)

      val hook = new Thread {
        override def run = pool.destroy()
      }
      sys.addShutdownHook(hook.run)
    }
  }

  def getJedisPool: JedisPool = {
    assert(pool != null)
    pool
  }
}
