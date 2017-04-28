package com.eneco.trading.kafka.connect.tennet

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.Struct

import scala.xml.Node

case class BidLadderTotalSourceRecordProducer(readers: ServiceProvider, sourceType: SourceType)
  extends SourceRecordProducer(readers, sourceType) with StrictLogging {

  override def schema = TennetSourceConfig.SCHEMA_BIDLADDERTOTAL

  override def mapRecord(record: Node, generatedAt: Long): Struct  = {
    new Struct(schema)
      .put("date", (record \ "DATE").text.toString)
      .put("ptu", (record \ "PTU").text.toLong)
      .put("period_from", (record \ "PERIOD_FROM").text.toString)
      .put("period_until", (record \ "PERIOD_UNTIL").text.toString)
      .put("rampdown_60", TennetHelper.NodeSeqToDouble(record \ "RAMPDOWN_60_"))
      .put("rampdown_15_60", TennetHelper.NodeSeqToDouble(record \ "RAMPDOWN_15_60"))
      .put("rampdown_0_15", TennetHelper.NodeSeqToDouble(record \ "RAMPDOWN_0_15"))
      .put("rampup_0_15", TennetHelper.NodeSeqToDouble(record \ "RAMPUP_0_15"))
      .put("rampup_15_60", TennetHelper.NodeSeqToDouble(record \ "RAMPUP_15_60"))
      .put("rampup_60_240", TennetHelper.NodeSeqToDouble(record \ "RAMPUP_60_240"))
      .put("rampup_240_480", TennetHelper.NodeSeqToDouble(record \ "RAMPUP_240_480"))
      .put("rampup_480", TennetHelper.NodeSeqToDouble(record \ "RAMPUP_480_"))
      .put("generated_at", generatedAt)
      .put("ptu_start", epochMillis.fromPTU((record \ "DATE").text.toString, (record \ "PTU").text.toInt))
  }
}