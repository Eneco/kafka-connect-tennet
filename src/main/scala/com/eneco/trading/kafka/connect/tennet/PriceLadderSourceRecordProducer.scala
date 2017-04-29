package com.eneco.trading.kafka.connect.tennet

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.Struct

import scala.xml.Node

case class PriceLadderSourceRecordProducer(readers: ServiceProvider, sourceType: SourceType)
  extends SourceRecordProducer(readers, sourceType) with StrictLogging {

  override def schema = TennetSourceConfig.SCHEMA_PRICELADDER

  override def mapRecord(record: Node, generatedAt: Long): Struct = {
    new Struct(schema)
      .put("date", (record \ "DATE").text.toString)
      .put("ptu", (record \ "PTU").text.toLong)
      .put("period_from", (record \ "PERIOD_FROM").text.toString)
      .put("period_until", (record \ "PERIOD_UNTIL").text.toString)
      .put("neg_total", TennetHelper.NodeSeqToDouble(record \ "NEG_TOTAL"))
      .put("neg_max", TennetHelper.NodeSeqToDouble(record \ "NEG_MAX"))
      .put("neg_600", TennetHelper.NodeSeqToDouble(record \ "NEG_600"))
      .put("neg_300", TennetHelper.NodeSeqToDouble(record \ "NEG_300"))
      .put("neg_100", TennetHelper.NodeSeqToDouble(record \ "NEG_100"))
      .put("neg_min", TennetHelper.NodeSeqToDouble(record \ "NEG_MIN"))
      .put("pos_min", TennetHelper.NodeSeqToDouble(record \ "POS_MIN"))
      .put("pos_100", TennetHelper.NodeSeqToDouble(record \ "POS_100"))
      .put("pos_300", TennetHelper.NodeSeqToDouble(record \ "POS_300"))
      .put("pos_600", TennetHelper.NodeSeqToDouble(record \ "POS_600"))
      .put("pos_max", TennetHelper.NodeSeqToDouble(record \ "POS_MAX"))
      .put("pos_total", TennetHelper.NodeSeqToDouble(record \ "POS_TOTAL"))
      .put("generated_at", generatedAt)
      .put("ptu_start", epochMillis.fromPTU((record \ "DATE").text.toString, (record \ "PTU").text.toInt))
  }
}