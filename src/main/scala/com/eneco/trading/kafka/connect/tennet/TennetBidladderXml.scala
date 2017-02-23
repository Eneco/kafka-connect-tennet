package com.eneco.trading.kafka.connect.tennet

import com.typesafe.scalalogging.slf4j.StrictLogging

import scala.xml.Node

case class TennetBidladderXml(readers: ServiceProvider, sourceType: SourceType)
  extends SourceRecordProducer(readers, sourceType) with StrictLogging {

  override def schema = BidLadderSourceRecord.schema

  def mapRecord(record: Node, generatedAt: Long): Object = {
    BidLadderSourceRecord.struct(
      BidLadderTennetRecord(
        (record \ "DATE").text.toString,
        (record \ "PTU").text.toInt,
        (record \ "PERIOD_FROM").text.toString,
        (record \ "PERIOD_UNTIL").text.toString,
        TennetHelper.NodeSeqToDouble(record \ "TOTAL_RAMPDOWN_REQUIRED").getOrElse(0),
        TennetHelper.NodeSeqToDouble(record \ "RAMPDOWN_REQUIRED").getOrElse(0),
        TennetHelper.NodeSeqToDouble(record \ "RAMPDOWN_RESERVE").getOrElse(0),
        TennetHelper.NodeSeqToDouble(record \ "RAMPDOWN_POWER").getOrElse(0),
        TennetHelper.NodeSeqToDouble(record \ "RAMPUP_POWER").getOrElse(0),
        TennetHelper.NodeSeqToDouble(record \ "RAMPUP_RESERVE").getOrElse(0),
        TennetHelper.NodeSeqToDouble(record \ "RAMPUP_REQUIRED").getOrElse(0),
        TennetHelper.NodeSeqToDouble(record \ "TOTAL_RAMPUP_REQUIRED").getOrElse(0),
        generatedAt,
        epochMillis.fromPTU((record \ "DATE").text.toString, (record \ "PTU").text.toInt)
      )
    )
  }
}