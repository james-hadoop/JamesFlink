package com.james.flink.functions

import com.james.flink.conf._
import com.james.flink.utils.JedisPoolUtil
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  *
  */
class SrcCntKeyedProcessFunction() extends KeyedProcessFunction[Long, SrcCntResult, mutable.HashMap[Long, Long]] {
//  private val LOG = LoggerFactory.getLogger("SrcCntKeyedProcessFunction")

//    lazy val jedis = new Jedis("localhost", 6379)
  lazy val jedis = JedisPoolUtil.getJedisPool.getResource

  lazy val srcCntMap: MapState[Long, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[Long, Long]("src-count", TypeInformation.of(classOf[Long]), TypeInformation.of(classOf[Long])))

  override def processElement(value: SrcCntResult, ctx: KeyedProcessFunction[Long, SrcCntResult, mutable.HashMap[Long, Long]]#Context, out: Collector[mutable.HashMap[Long, Long]]): Unit = {
    var cnt = value.qiyongCnt

    if (srcCntMap.contains(value.src)) {
      cnt += srcCntMap.get(value.src)
    }

    srcCntMap.put(value.src, cnt)

    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)

    val sb: StringBuilder = new StringBuilder()
    val iter = srcCntMap.entries().iterator()
    while (iter.hasNext()) {
      val entry = iter.next()

      val src = entry.getKey
      val cnt = entry.getValue

      sb.append("src=" + src + " -> cnt=" + cnt + "\n")
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, SrcCntResult, mutable.HashMap[Long, Long]]#OnTimerContext, out: Collector[mutable.HashMap[Long, Long]]): Unit = {
    //    println("debug>>> onTimer() >>>" + ctx.getCurrentKey)

    var sb: StringBuilder = new StringBuilder()
    var map = new mutable.HashMap[Long, Long]()
    val iter = srcCntMap.entries().iterator()
    while (iter.hasNext()) {
      val entry = iter.next()

      val src = entry.getKey
      var cnt = entry.getValue

      if (null != jedis.hget(ConstValue.DEBUG_SRC_CNT_KEY, src.toString)) {
        cnt += jedis.hget(ConstValue.DEBUG_SRC_CNT_KEY, src.toString).toLong
      }

      jedis.hset(ConstValue.DEBUG_SRC_CNT_KEY, src.toString, cnt.toString)

      //sb.append("timestamp=" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ctx.getCurrentKey) + " src=" + entry.getKey + " count=" + entry.getValue).append("\n")
      map.put(entry.getKey, entry.getValue)
    }
    //    println("-" * 120)
    //    println(sb.toString)
    //    LOGGER.error(sb.toString())
    //    println(jedis.hgetAll(ConstConf.DEBUG_SRC_CNT_KEY))

    // 清空状态
    srcCntMap.clear()
    // jedis.expire(ConstConf.DEBUG_SRC_CNT_KEY, 10)

    out.collect(map)
  }
}
