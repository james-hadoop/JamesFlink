package com.james.flink.flink_source

import com.james.flink.conf.RowkeyInfo
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

class RowkeyInfoSource() extends RichSourceFunction[RowkeyInfo]{
  override def run(ctx: SourceFunction.SourceContext[RowkeyInfo]): Unit = {}

  override def cancel(): Unit = {}
}
