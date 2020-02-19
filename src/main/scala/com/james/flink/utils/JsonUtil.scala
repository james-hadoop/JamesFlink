package com.james.flink.utils

import com.alibaba.fastjson.{JSON, JSONObject}
import com.james.flink.conf.ConstValue
import org.slf4j.LoggerFactory

import scala.collection.mutable

object JsonUtil {
  val LOG = LoggerFactory.getLogger(JsonUtil.getClass)

  def jsonString2Map(jsonString: String): mutable.Map[String, Long] = {
    if (null == jsonString || 0 == jsonString.length) return null

    val map = new mutable.HashMap[String, Long]

    var fieldValueMap: JSONObject = null

    try {
      fieldValueMap = JSON.parseObject(jsonString).get(ConstValue.FIELD_VALUE_FIELD).asInstanceOf[JSONObject]
      if (null == fieldValueMap) return null
    } catch {
      case e: Exception =>
        LOG.error(e.getMessage)
    }

    if (null == fieldValueMap) {
      return null
    }

    val rukuTsStr = fieldValueMap.get(ConstValue.ORIGIN_RUKU_FIELD).asInstanceOf[String]
    if (null == rukuTsStr) {
      map.put(ConstValue.RUKU_FIELD, -1L)
    }
    else {
      map.put(ConstValue.RUKU_FIELD, rukuTsStr.toLong)
    }

    val jishenTsStr = fieldValueMap.get(ConstValue.ORIGIN_JISHEN_FIELD).asInstanceOf[String]
    if (null == jishenTsStr) {
      map.put(ConstValue.JISHEN_FIELD, -1L)
    }
    else {
      map.put(ConstValue.JISHEN_FIELD, jishenTsStr.toLong)
    }

    val renshenTsStr = fieldValueMap.get(ConstValue.ORIGIN_RENSHEN_FIELD).asInstanceOf[String]
    if (null == renshenTsStr) {
      map.put(ConstValue.RENSHEN_FIELD, -1L)
    }
    else {
      map.put(ConstValue.RENSHEN_FIELD, renshenTsStr.toLong)
    }

    val qiyongTsStr = fieldValueMap.get(ConstValue.ORIGIN_QIYONG_FIELD).asInstanceOf[String]
    if (null == qiyongTsStr) {
      map.put(ConstValue.QIYONG_FIELD, -1L)
    }
    else {
      map.put(ConstValue.QIYONG_FIELD, qiyongTsStr.toLong)
    }

    val src = fieldValueMap.get(ConstValue.SRC).asInstanceOf[String]
    if (null == src) {
      map.put(ConstValue.SRC, -1L)
    }
    else {
      map.put(ConstValue.SRC, src.toLong)
    }

    map
  }

  def main(args: Array[String]): Unit = {
    System.out.println("JsonUtil start...")
    //String originJsonString = "{\"field_value\":{\"premium_level\":\"[{\\\"version\\\":\\\"2.0.0\\\",\\\"premium_level\\\":0},{\\\"version\\\":\\\"3.1.0\\\",\\\"premium_level\\\":0}]\",\"st_union_chann_tag\":\"1\",\"union_chann\":\"1273__len__\",\"union_compose_tag\":\"[{\\\"version\\\":\\\"2.0.0\\\",\\\"index_attr\\\":null,\\\"doc_attr\\\":null,\\\"title_attr\\\":null},{\\\"version\\\":\\\"3.1.0\\\",\\\"index_attr\\\":null,\\\"doc_attr\\\":null,\\\"title_attr\\\":null}]\",\"union_concept\":\"[{\\\"version\\\":\\\"2.0.0\\\",\\\"index_attr\\\":null,\\\"doc_attr\\\":null,\\\"title_attr\\\":null},{\\\"version\\\":\\\"3.1.0\\\",\\\"index_attr\\\":null,\\\"doc_attr\\\":null,\\\"title_attr\\\":null}]\",\"union_entity\":\"[{\\\"version\\\":\\\"2.0.0\\\",\\\"index_attr\\\":null,\\\"doc_attr\\\":null,\\\"title_attr\\\":null},{\\\"version\\\":\\\"3.1.0\\\",\\\"index_attr\\\":null,\\\"doc_attr\\\":null,\\\"title_attr\\\":null}]\",\"union_event\":\"[{\\\"version\\\":\\\"2.0.0\\\",\\\"index_attr\\\":null,\\\"doc_attr\\\":null,\\\"title_attr\\\":null},{\\\"version\\\":\\\"3.1.0\\\",\\\"index_attr\\\":null,\\\"doc_attr\\\":null,\\\"title_attr\\\":null}]\",\"union_ori_source\":\"5042__len__\",\"union_relations\":\"[{\\\"version\\\":\\\"2.0.0\\\",\\\"doc_relations\\\":[]},{\\\"version\\\":\\\"3.1.0\\\",\\\"doc_relations\\\":[]}]\",\"union_sentiment_entity\":\"[{\\\"version\\\":\\\"2.0.0\\\",\\\"category_id\\\":12801,\\\"doc_entities\\\":null,\\\"title_entities\\\":null,\\\"doc_sentiment\\\":2}]\",\"union_sentiment_entity_relation\":\"[{\\\"version\\\":\\\"2.0.0\\\",\\\"doc_relations\\\":null}]\",\"union_tag\":\"5026__len__\",\"union_title_tag\":\"[{\\\"version\\\":\\\"2.0.0\\\",\\\"value\\\":[{\\\"id\\\":\\\"1002586\\\",\\\"name\\\":\\\"人才\\\",\\\"category\\\":815},{\\\"id\\\":\\\"108787495\\\",\\\"name\\\":\\\"人渣\\\",\\\"category\\\":0}]},{\\\"version\\\":\\\"3.1.0\\\",\\\"value\\\":[{\\\"id\\\":\\\"1002586\\\",\\\"name\\\":\\\"人才\\\",\\\"category\\\":815},{\\\"id\\\":\\\"108787495\\\",\\\"name\\\":\\\"人渣\\\",\\\"category\\\":0}]}]\",\"union_topic\":\"[{\\\"version\\\":\\\"2.0.0\\\",\\\"index_attr\\\":null,\\\"doc_attr\\\":null,\\\"title_attr\\\":null},{\\\"version\\\":\\\"3.1.0\\\",\\\"index_attr\\\":null,\\\"doc_attr\\\":null,\\\"title_attr\\\":null}]\"},\"v4\":0}";
    val originJsonString = "{\"field_value\":{\"been_valid\":\"1\",\"been_valid_first_time\":\"1578446639\",\"enable_platform_experiment\":\"1\",\"filter_reason\":\"0\",\"kd_filter_flag\":\"0\",\"output_ts\":\"1578446639\",\"src_name\":\"微信爬取\",\"st_kd\":\"1\",\"version\":\"2\"},\"v4\":0}"
    val map = jsonString2Map(originJsonString)
    import scala.collection.JavaConversions._
    for (entry <- map.entrySet) {
      System.out.println(entry.getKey + " -> " + entry.getValue)
    }
  }
}
