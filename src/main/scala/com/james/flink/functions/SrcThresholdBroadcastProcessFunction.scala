package com.james.flink.functions

import java.util.Date
import java.util.concurrent.ConcurrentHashMap

import com.james.flink.conf.{ConstValue, SrcCntResult, SrcQiyongExCntOutput, SrcQiyongExCntThreshold}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable

class SrcThresholdBroadcastProcessFunction extends BroadcastProcessFunction[mutable.HashMap[Long, Long], mutable.HashMap[Long, SrcQiyongExCntThreshold], SrcQiyongExCntOutput] {
  private val items = new MapStateDescriptor("items", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(classOf[SrcCntResult]))

  private val thresholdStateDesc = new MapStateDescriptor(ConstValue.SRC_QIYONG_CNT_THRESHOLD_CONFIG, TypeInformation.of(classOf[Long]), TypeInformation.of(classOf[SrcQiyongExCntThreshold]))

  private val cntThresholdMap = new ConcurrentHashMap[Long, SrcQiyongExCntThreshold]()

  override def processElement(value: mutable.HashMap[Long, Long], ctx: BroadcastProcessFunction[mutable.HashMap[Long, Long], mutable.HashMap[Long, SrcQiyongExCntThreshold], SrcQiyongExCntOutput]#ReadOnlyContext, out: Collector[SrcQiyongExCntOutput]): Unit = {
    val stateMap = ctx.getBroadcastState(thresholdStateDesc)
    val stateMapIter = stateMap.immutableEntries().iterator()


    while (stateMapIter.hasNext) {
      val entry = stateMapIter.next()
      println("\t >>>: " + entry.getKey + " -> " + entry.getValue.src + "@" + entry.getValue.lowCnt + "-" + entry.getValue.highCnt)
      cntThresholdMap.put(entry.getKey, entry.getValue)
    }

    val iter = value.iterator
    while (iter.hasNext) {
      val entry = iter.next()
      println("\t processElement(): " + entry._1 + " -> " + entry._2)

      if (null != cntThresholdMap.get(entry._1) && cntThresholdMap.get(entry._1).highCnt > entry._1) {
        val srcQiyongEx = new SrcQiyongExCntOutput(entry._1, entry._2, cntThresholdMap.get(entry._1).lowCnt, cntThresholdMap.get(entry._1).highCnt, new Date())
        println(">>> srcQiyongExCntThreshold: " + srcQiyongEx)
        out.collect(srcQiyongEx)
      }
    }
  }

  override def processBroadcastElement(value: mutable.HashMap[Long, SrcQiyongExCntThreshold], ctx: BroadcastProcessFunction[mutable.HashMap[Long, Long], mutable.HashMap[Long, SrcQiyongExCntThreshold], SrcQiyongExCntOutput]#Context, out: Collector[SrcQiyongExCntOutput]): Unit = {
    val bcStateMap = ctx.getBroadcastState(thresholdStateDesc)

    val iter = value.iterator
    while (iter.hasNext) {
      val entry = iter.next()
      //          println("\t processBroadcastElement(): " + entry._1 + " -> " + entry._2)
      bcStateMap.put(entry._1, entry._2)
    }
  }
}
