package com.james.flink.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1. 从集合中读取数据
    val stream1 = env.fromCollection(List(SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.4029843984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.10106760489344)
    ))
    // stream1.print("stream1").setParallelism(1)

    // 2. 从文件中读取数据
    val stream2 = env.readTextFile("/home/james/workspace/JamesFlink/src/main/resources/sensor.txt")
    //    stream2.print("stream2").setParallelism(1)

    // 3. 从Kafka中读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")


    val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
    stream3.print("stream3").setParallelism(1)

    // 4. 自定义Source

    // 启动执行环境
    //    env.execute("SourceTest")
  }
}


